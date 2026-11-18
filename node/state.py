# info dos estados de cada no

import os, random, threading, time
from node.bd import Storage

class NodeState:
    def __init__(self, no_id, data_dir=None):
        self.lock = threading.RLock()

        self.votos_recebidos = set() 
        self.eleic_timeout = None # tempo de eleicao

        self.no_id = no_id
        self.papel = "follower"
        self.termo_atual = 0

        self.voto_alvo = None
        self.log = []

        self.commit_index = 0
        self.ultimo_indice = 0

        self.db = {}
        self.lider_id = None
        self.election_deadline = None

    
        if data_dir is None:
            data_dir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data")
            )

        self.bd = Storage(
            os.path.join(data_dir, f"node{no_id}.db")
        )
        termo, voto_alvo, ultimo_indice, log, db = self.bd.carrega()

        self.termo_atual = termo
        self.voto_alvo = voto_alvo
        self.log = log
        self.db = db
        self.ultimo_indice = ultimo_indice

        self.commit_index = ultimo_indice

        self._saved_meta = (self.termo_atual, self.voto_alvo)

        if termo or log:
            print(
                f"[Nó {self.no_id}] Estado recuperado do disco: "
                f"termo={self.termo_atual} voto={self.voto_alvo} "
                f"log={len(self.log)} commit={self.commit_index} "
                f"dados={self.db}"
            )

        self.reset_eleic_timeout(verbose=True)

    def last_log_info(self): # retorna indice e termo da ultima entrada 
        with self.lock:
            if self.log:
                return len(self.log), self.log[-1]["term"]
            return 0, 0

    def persist_meta(self):
        with self.lock:
            snapshot = (self.termo_atual, self.voto_alvo)

            if snapshot != self._saved_meta:
                self.bd.save_meta(*snapshot)
                self._saved_meta = snapshot

    def adiciona_entradas(self, entries): # adiciona no log
        with self.lock:
            self.log.extend(entries)
            self.bd.adiciona_log(entries)

    def trunca_log(self, to_index):
    
        with self.lock:
            to_index = max(to_index, self.commit_index)

            if to_index >= len(self.log):
                return

            removed = self.log[to_index:]
            del self.log[to_index:]
            self.bd.trunca_log(to_index)

            print(
                f"[Nó {self.no_id}] TRUNCAMENTO DE LOG: removendo "
                f"{len(removed)} entradas nao comitadas a partir do indice {to_index + 1}: "
                f"{[(e['index'], e['term'], e['key'], e['value']) for e in removed]}"
            )

    def apply_committed(self):
        with self.lock:
            if self.commit_index <= self.ultimo_indice:
                return

            pendente = self.log[self.ultimo_indice:self.commit_index]
            puts = []

            for entradas in pendente:
                if entradas["operation"] == "PUT":
                    self.db[entradas["key"]] = entradas["value"]
                    puts.append((entradas["key"], entradas["value"]))

            self.ultimo_indice = self.commit_index
            self.bd.apply(puts, self.ultimo_indice)

            for key, value in puts:
                print(
                    f"[Nó {self.no_id}] COMMIT aplicado no banco: "
                    f"{key}={value} (commit_index={self.commit_index})"
                )

    def grant_vote(self, candidato_id, candidate_term,
                   candidate_last_log_index, candidate_last_log_term):

        with self.lock:
            # candidato com termo antigo nao recebe voto
            if candidate_term < self.termo_atual:
                return self.termo_atual, False

            # se recebeu um termo maior, atualiza seu estado
            if candidate_term > self.termo_atual:
                self.termo_atual = candidate_term
                self.voto_alvo = None
                self.papel = "follower"
                self.lider_id = None

            # descobre o ultimo registro do proprio servidor
            last_log_index, last_log_term = self.last_log_info()

            # verifica se o log do candidato está atualizado
            candidate_log_is_updated = (
                candidate_last_log_term > last_log_term
                or (
                    candidate_last_log_term == last_log_term
                    and candidate_last_log_index >= last_log_index
                )
            )

            # so vota uma vez por temo
            can_vote = (
                self.voto_alvo is None
                or self.voto_alvo == candidato_id
            )

            # concede o voto
            if can_vote and candidate_log_is_updated:

                self.voto_alvo = candidato_id
                self.persist_meta()
                self.reset_eleic_timeout()

                return self.termo_atual, True

            self.persist_meta()
            return self.termo_atual, False

    def reset_eleic_timeout(self, verbose=False):
        with self.lock:
            self.eleic_timeout = random.uniform(3, 5)

            self.election_deadline = (
                time.monotonic() + self.eleic_timeout
            )

            if verbose:
                print(
                    f"[Nó {self.no_id}] "
                    f"Timeout definido: {self.eleic_timeout:.2f} segundos"
                )

    def eleic_timeout_expired(self):
        with self.lock:
            return time.monotonic() >= self.election_deadline


    def start_election(self):
        with self.lock:
            self.papel = "candidate" # muda o nome
            self.termo_atual += 1 # incrementa tempo de eleicao
            self.voto_alvo = self.no_id # vota em si

            self.persist_meta()

            self.votos_recebidos = {self.no_id} # registra

            self.lider_id = None
            self.reset_eleic_timeout(verbose=True)

            print("=" * 40)
            print(f"[Nó {self.no_id}] INICIOU UMA ELEIÇÃO!")
            print(f"Papel: {self.papel}")
            print(f"Termo: {self.termo_atual}")
            print(f"Votos recebidos: {self.votos_recebidos}")
            print("=" * 40)