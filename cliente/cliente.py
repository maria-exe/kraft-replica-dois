import sys, os
import threading, time

import grpc

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "clusters")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import cluster_pb2
import cluster_pb2_grpc

from node.config import NODES

def get_stub(no_id): # cliente usa para chamar funcoes do server
    host, port = NODES[no_id]
    channel = grpc.insecure_channel(f"{host}:{port}")
    return channel, cluster_pb2_grpc.ClusterServiceStub(channel)

# gravacao com redirecionamento 
def write_data(key, value, crash_after_append=False, first_node=None):
    # se enviou escrita a um no que nao e lider redireciona para o real (reenvia)
    no_ids = list(NODES.keys())
    no_alvo = first_node if first_node else no_ids[0]

    for tentativa in range(12):
        channel, stub = get_stub(no_alvo)
        try:
            request = cluster_pb2.WriteRequest (
                key=key, value=value, crash_after_append=crash_after_append )

            response = stub.Write(request, timeout=8)

        except grpc.RpcError as error:
            print(f"Nó {no_alvo} indisponivel: {error.code()}")

            if crash_after_append:
                print("(esperado: o lider caiu logo apos gravar no log local)")
                return False

            # tenta o proximo no
            no_alvo = no_ids[(no_ids.index(no_alvo) + 1) % len(no_ids)]
            time.sleep(0.3)
            continue

        finally:
            channel.close()

        if response.success:
            print(f"ESCRITA CONFIRMADA pelo lider (nó {response.leader_id})")
            print(f"  indice={response.index} termo={response.term}")
            return True

        # nao e lider: indica quem e
        if response.leader_id != 0 and response.leader_id != no_alvo:
            print(f"Redirecionando para o no: {response.leader_id}")
            no_alvo = response.leader_id
            continue

        # o proprio lider recusou (sem quórum / não confirmou)
        if response.leader_id == no_alvo:
            print(f"Escrita nao feita pelo líder (no {no_alvo}): {response.message}")
            return False

        # se lider_id == 0 tenta outro no
        print(f"Eleicao em andamento: reencaminhando...")
        no_alvo = no_ids[(no_ids.index(no_alvo) + 1) % len(no_ids)]
        time.sleep(1)

    print("\nErro ao fazer gravacao")
    return False


# leitura de dados nos nos
def read_from(no_id, key):
    channel, stub = get_stub(no_id)

    try:
        response = stub.Read( cluster_pb2.ReadRequest(key=key), timeout=3 )
        valor = response.value if response.found else "(nao encontrado)"

        print(
            f"Nó {response.node_id} [{response.role}, termo {response.term}, "
            f"commit {response.commit_index}]: {key} = {valor}"
        )

    except grpc.RpcError as error:
        print(f"Nó {no_id} indisponível: {error.code()}")

    finally:
        channel.close()

# le de todos os nos
def read_all(key):
    for no_id in NODES:
        read_from(no_id, key)

# status de cada no
def status_of(no_id):
    channel, stub = get_stub(no_id)

    try:
        status = stub.Status(cluster_pb2.StatusRequest(), timeout=3)

        print(
            f"\nNo {status.node_id}: papel={status.role} termo={status.term} "
            f"commit={status.commit_index} aplicado={status.last_applied} "
            f"lider={status.leader_id} voto={status.voted_for}"
        )

        if status.log:
            print("LOG:")
            for entrada in status.log:
                marca = "comitada" if entrada.index <= status.commit_index else "NÃO comitada"
                print(f"    [{entrada.index}] termo={entrada.term} {entrada.operation} "
                      f"{entrada.key}={entrada.value}  ({marca})")
        else:
            print("  LOG: (vazio)")

        print(f"  BANCO (comitados): {dict(status.data)}")

    except grpc.RpcError as error:
        print(f"\nNó {no_id} indisponível: {error.code()}")

    finally:
        channel.close()


def status_all():
    for no_id in NODES:
        status_of(no_id)


## funcao de teste
def demo_isolamento(key, value):
    resultado = {}

    def escrever():
        resultado["ok"] = write_data(key, value)

    t0 = time.monotonic()
    writer = threading.Thread(target=escrever)
    writer.start()

    ultimo = {}

    while writer.is_alive() or time.monotonic() - t0 < 6:
        for no_id in NODES:
            channel, stub = get_stub(no_id)

            try:
                st = stub.Status(cluster_pb2.StatusRequest(), timeout=1)
                no_log = any(e.key == key and e.value == value for e in st.log)
                no_banco = st.data.get(key)

                snapshot = (len(st.log), st.commit_index, no_log, no_banco)

                if ultimo.get(no_id) != snapshot:
                    ultimo[no_id] = snapshot
                    print(
                        f"t={time.monotonic() - t0:5.2f}s  Nó {no_id} "
                        f"[{st.role:9}] log={len(st.log)} "
                        f"commit={st.commit_index} "
                        f"'{key}' no LOG={no_log} "
                        f"| LEITURA devolve={no_banco!r}"
                    )

            except grpc.RpcError:
                pass

            finally:
                channel.close()

        time.sleep(0.15)
    writer.join()

# menu
MENU = """
==================== CLIENTE ====================
 1) Gravar (com redirecionamento ao líder)
 2) Ler de UM nó
 3) Ler de TODOS os nós
 4) Status de todos os nós (papel, termo, commit, log, banco)
 5) Status de um nó
 6) Isolamento de dados nao comitados
 7) Cenario: lider grava e cai antes de replicar
 0) Sair
=================================================
"""

def main():
    while True:
        print(MENU)
        escolha = input("Opção: ").strip()

        if escolha == "1":
            key = input("Chave: ").strip()
            value = input("Valor: ").strip()
            write_data(key, value)

        elif escolha == "2":
            no_id = int(input("Nó (1-4): "))
            key = input("Chave: ").strip()
            read_from(no_id, key)

        elif escolha == "3":
            key = input("Chave: ").strip()
            read_all(key)

        elif escolha == "4":
            status_all()

        elif escolha == "5":
            status_of(int(input("Nó (1-4): ")))

        elif escolha == "6":
            key = input("Chave: ").strip()
            value = input("Valor: ").strip()
            demo_isolamento(key, value)

        elif escolha == "7":
            key = input("Chave: ").strip()
            value = input("Valor: ").strip()
            print("ATENÇÃO: o líder vai ENCERRAR o processo logo após gravar.")
            write_data(key, value, crash_after_append=True)

        elif escolha == "0":
            break

if __name__ == "__main__":
    main()