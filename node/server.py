import sys, os
import threading, time, grpc, argparse

from node.peer import Peer
from concurrent import futures
from node.state import NodeState
from node.config import get_peers, NODES

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "clusters")))

import cluster_pb2
import cluster_pb2_grpc

HEARTBEAT_TIMEOUT = 4  
FETCH_INTERVAL = 1   
STATUS_INTERVAL = 1   

class ClusterServer(cluster_pb2_grpc.ClusterServiceServicer):
    def __init__(self, node_id, fetch_interval=FETCH_INTERVAL):
        self.state = NodeState(no_id=node_id) 
        self.lock = self.state.lock

        self.fetch_interval = fetch_interval

        self.quorum = len(NODES) // 2 + 1
        self.peer_ids = list(get_peers(node_id).keys())
        self.match_index = { peer_id: 0 for peer_id in self.peer_ids }
        self.last_fetch_received = {}


    def step_down(self, new_term):
        self.state.termo_atual = new_term
        self.state.papel = "follower"
        self.state.voto_alvo = None
        self.state.votos_recebidos.clear()
        self.state.lider_id = None
       
        self.state.persist_meta()

    def become_leader(self):
        self.state.papel = "leader"
        self.state.lider_id = self.state.no_id
       
        self.match_index = {pid: 0 for pid in self.peer_ids}
        self.last_fetch_received = {}

        self.state.adiciona_entradas([{
            "index": len(self.state.log) + 1,
            "term": self.state.termo_atual,
            "operation": "NOOP",
            "key": "",
            "value": ""
        }])

        print(f"[Nó {self.state.no_id}] FOI ELEITO LÍDER no termo "
              f"{self.state.termo_atual}!")

    def advance_commit(self):
        log = self.state.log
        for index in range(self.state.commit_index + 1, len(log) + 1):
            if log[index - 1]["term"] != self.state.termo_atual:
                continue
            confirmations = 1 + sum(
                1 for m in self.match_index.values() if m >= index
            )
            if confirmations >= self.quorum:
                self.state.commit_index = index
            else:
                break

        self.state.apply_committed()

    def divergence_point(self, index, term):
        log = self.state.log
        limit = min(index - 1, len(log))

        for i in range(limit, 0, -1):
            if log[i - 1]["term"] <= term:
                return i
        return 0

    def Heartbeat(self, request, context):

        print(
            f"[Nó {self.state.no_id}] "
            f"Heartbeat recebido do nó {request.node_id}"
        )

        return cluster_pb2.Response(
            success=True,
            message="Servidor funcionando!"
        )

    def RequestVote(self, request, context):

        term, granted = self.state.grant_vote(
            candidato_id=request.candidate_id,
            candidate_term=request.term,
            candidate_last_log_index=request.last_log_index,
            candidate_last_log_term=request.last_log_term
        )

        print(
            f"[Nó {self.state.no_id}] "
            f"Solicitação de voto do nó {request.candidate_id}"
        )

        print(f"Voto concedido: {granted}")

        return cluster_pb2.VoteResponse(
            term=term,
            vote_granted=granted
            )

    def _ask_vote(self, peer_id, host, port, request):
        peer = Peer(peer_id, host, port)
        try:
            return peer_id, peer.request_vote(request)
        except Exception as error:
            print(f"[Nó {self.state.no_id}] Falha ao contatar nó {peer_id}: "
                f"{error.code() if hasattr(error, 'code') else error}")
            return peer_id, None
        finally:
            peer.close()

    def run_election(self):
        self.state.start_election()
        with self.lock:
            election_term = self.state.termo_atual
            last_log_index, last_log_term = self.state.last_log_info()

        print(f"[Nó {self.state.no_id}] Enviando solicitações de voto...")

        request = cluster_pb2.VoteRequest(
            candidate_id=self.state.no_id,
            term=election_term,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )

        peers = get_peers(self.state.no_id)
        executor = futures.ThreadPoolExecutor(max_workers=len(peers))

        try:
            pending = [
                executor.submit(self._ask_vote, pid, host, port, request)
                for pid, (host, port) in peers.items()
            ]

            for fut in futures.as_completed(pending):
                peer_id, response = fut.result()

                if response is None:      
                    continue

                with self.lock:
                    if (self.state.papel != "candidate"
                            or self.state.termo_atual != election_term):
                        return

                    if response.term > self.state.termo_atual:
                        self.step_down(response.term)
                        self.state.reset_eleic_timeout()
                        print(f"[Nó {self.state.no_id}] "
                              f"Termo maior recebido: {response.term}")
                        return

                    print(f"[Nó {self.state.no_id}] "
                          f"Resposta do nó {peer_id}: {response.vote_granted}")

                    if response.vote_granted:
                        self.state.votos_recebidos.add(peer_id)

                    # atingiu o quorum
                    if len(self.state.votos_recebidos) >= self.quorum:
                        self.become_leader()
                        print(f"[Nó {self.state.no_id}] "
                              f"Votos: {self.state.votos_recebidos}")
                        return

            with self.lock:
                print(f"[Nó {self.state.no_id}] Não conseguiu votos suficientes. "
                      f"Votos: {self.state.votos_recebidos}")

        finally:
            executor.shutdown(wait=False)

    def election_loop(self):
        while True:
            with self.lock:
                pode_eleger = (
                    self.state.papel in ("follower", "candidate")
                    and self.state.eleic_timeout_expired()
                )

            if pode_eleger:
                print(f"[Nó {self.state.no_id}] Timeout expirado! Iniciando eleicao...")
                self.run_election()
            time.sleep(0.1)


    def Fetch(self, request, context):
        with self.lock:
            if request.term > self.state.termo_atual:
                self.step_down(request.term)
                return cluster_pb2.FetchResponse(
                    term=self.state.termo_atual,
                    leader_id=0,
                    success=False,
                    leader_commit=self.state.commit_index
                )

            if self.state.papel != "leader":
                return cluster_pb2.FetchResponse(
                    term=self.state.termo_atual,
                    leader_id=self.state.lider_id or 0,
                    success=False,
                    leader_commit=self.state.commit_index
                )

            self.last_fetch_received[request.node_id] = time.monotonic()
            log = self.state.log
            index = request.last_log_index

            matches = (index == 0 or (index <= len(log) and log[index - 1]["term"] == request.last_log_term))

            if matches:
                self.match_index[request.node_id] = index
            else:
                self.match_index[request.node_id] = 0

            self.advance_commit()

            if not matches:
                return cluster_pb2.FetchResponse(
                    term=self.state.termo_atual,
                    leader_id=self.state.no_id,
                    success=True,
                    diverged=True,
                    truncate_to=self.divergence_point(index, request.last_log_term),
                    leader_commit=self.state.commit_index
                )
            
            entries = [
                cluster_pb2.LogEntry(
                    index=entry["index"],
                    term=entry["term"],
                    operation=entry["operation"],
                    key=entry["key"],
                    value=entry["value"]
                )
                for entry in log[index:]
            ]

            return cluster_pb2.FetchResponse(
                term=self.state.termo_atual,
                leader_id=self.state.no_id,
                success=True,
                entries=entries,
                leader_commit=self.state.commit_index
            )

    def fetch_loop(self):
        while True:
            with self.lock:
                active = self.state.papel != "leader"
                peers = get_peers(self.state.no_id)
                leader_id = self.state.lider_id

            if active:
                if leader_id in peers:
                    target_ids = [leader_id] + [p for p in peers if p != leader_id]
                else:
                    target_ids = list(peers.keys())

                for peer_id in target_ids:
                    host, port = peers[peer_id]
                    if self._fetch_from(peer_id, host, port):
                        break

            time.sleep(self.fetch_interval)     

    def _fetch_from(self, peer_id, host, port):
        with self.lock:
            last_index, last_term = self.state.last_log_info()
            request = cluster_pb2.FetchRequest(
                node_id=self.state.no_id,
                term=self.state.termo_atual,
                last_log_index=last_index,
                last_log_term=last_term,
                commit_index=self.state.commit_index
            )

        peer = Peer(peer_id, host, port)
        try:
            response = peer.fetch(request)
        except grpc.RpcError:
            return False
        finally:
            peer.close()

        with self.lock:
            if self.state.papel == "leader":     # virou líder durante a chamada
                return False

            if response.term > self.state.termo_atual:
                self.step_down(response.term)

            if response.term < self.state.termo_atual:
                print(f"[Nó {self.state.no_id}] Ignorando Fetch de líder obsoleto")
                return False

            if response.leader_id != 0:
                self.state.lider_id = response.leader_id

            if not response.success:
                return False

            self.state.papel = "follower"       
            self.state.lider_id = response.leader_id

            if response.diverged:
                self.state.trunca_log(response.truncate_to)
            else:
                new_entries = []
                next_index = len(self.state.log) + 1

                for e in response.entries:
                    if e.index == next_index:
                        new_entries.append({
                            "index": e.index, "term": e.term,
                            "operation": e.operation,
                            "key": e.key, "value": e.value
                        })
                        next_index += 1

                if new_entries:
                    self.state.adiciona_entradas(new_entries)

                self.state.commit_index = max(
                    self.state.commit_index,
                    min(response.leader_commit, len(self.state.log))
                )
                self.state.apply_committed()

            self.state.reset_eleic_timeout()
            return True

    def Write(self, request, context):
        with self.lock:
            if self.state.papel != "leader":
                return cluster_pb2.WriteResponse(
                    success=False, message="Este nó não é o líder.",
                    term=self.state.termo_atual,
                    leader_id=self.state.lider_id or 0
                )

            now = time.monotonic()
            active_nodes = 1 + sum(
                1 for seen in self.last_fetch_received.values()
                if now - seen <= HEARTBEAT_TIMEOUT
            )
            if active_nodes < self.quorum:
                return cluster_pb2.WriteResponse(
                    success=False, message="Sem quórum. Escrita rejeitada.",
                    term=self.state.termo_atual, leader_id=self.state.no_id
                )

            new_index = len(self.state.log) + 1   
            write_term = self.state.termo_atual
            self.state.adiciona_entradas([{
                "index": new_index, "term": write_term, "operation": "PUT",
                "key": request.key, "value": request.value
            }])

            if request.crash_after_append:
                print(f"[Nó {self.state.no_id}] *** CRASH SIMULADO após gravar "
                      f"{request.key}={request.value} no log local (índice {new_index}) ***",
                      flush=True)
                os._exit(1)

        print(f"[Nó {self.state.no_id}] Nova escrita: {request.key}={request.value}")

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with self.lock:
                lost_leadership = (
                    self.state.papel != "leader"
                    or self.state.termo_atual != write_term
                )
                if lost_leadership:
                    break
                if self.state.commit_index >= new_index:
                    return cluster_pb2.WriteResponse(
                        success=True, message="Registro confirmado pelo quórum.",
                        term=write_term, leader_id=self.state.no_id,
                        index=new_index
                    )
            time.sleep(0.05)

        with self.lock:
            return cluster_pb2.WriteResponse(
                success=False,
                message="Não foi possível confirmar a escrita pelo quórum.",
                term=self.state.termo_atual,
                leader_id=self.state.lider_id or 0,
                index=new_index
            )

    def Read(self, request, context):
        with self.lock:
            found = request.key in self.state.db
            return cluster_pb2.ReadResponse(
                found=found,
                value=self.state.db.get(request.key, ""),
                node_id=self.state.no_id,
                role=self.state.papel,
                term=self.state.termo_atual,
                commit_index=self.state.commit_index
            )

    def Status(self, request, context):
        with self.lock:
            return cluster_pb2.StatusResponse(
                node_id=self.state.no_id,
                role=self.state.papel,
                term=self.state.termo_atual,
                commit_index=self.state.commit_index,
                last_applied=self.state.ultimo_indice,
                leader_id=self.state.lider_id or 0,
                voted_for=self.state.voto_alvo or 0,
                log=[
                    cluster_pb2.LogEntry(
                        index=e["index"], term=e["term"],
                        operation=e["operation"],
                        key=e["key"], value=e["value"]
                    )
                    for e in self.state.log
                ],
                data=self.state.db
            )

    def status_loop(self):
        ultimo = None
        ultimo_print = 0

        while True:
            with self.lock:
                atual = (
                    self.state.papel,
                    self.state.termo_atual,
                    self.state.commit_index,
                    len(self.state.log),
                    self.state.lider_id
                )

            agora = time.monotonic()

            if atual != ultimo or agora - ultimo_print >= 30:
                print(
                    f"[STATUS Nó {self.state.no_id}] "
                    f"papel={atual[0]} termo={atual[1]} "
                    f"commit={atual[2]} log={atual[3]} líder={atual[4]}",
                    flush=True
                )
                ultimo = atual
                ultimo_print = agora

            time.sleep(2)


def start_server(node_id, port, fetch_interval=FETCH_INTERVAL):

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    service = ClusterServer(node_id, fetch_interval)

    cluster_pb2_grpc.add_ClusterServiceServicer_to_server(
        service,
        server
    )

    server.add_insecure_port(f"[::]:{port}")

    server.start()
    print(f"Nó {node_id} iniciado na porta {port}")

    election_thread = threading.Thread(
        target=service.election_loop,
        daemon=True
    )

    fetch_thread = threading.Thread(
        target=service.fetch_loop,
        daemon=True
    )

    status_thread = threading.Thread(
        target=service.status_loop,
        daemon=True
    )

    fetch_thread.start()
    election_thread.start()
    status_thread.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\nNó {node_id} encerrado.")
        server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--id",
        type=int,
        required=True,
        help="Identificação do servidor"
    )

    args = parser.parse_args()

    start_server(args.id, NODES[args.id][1])