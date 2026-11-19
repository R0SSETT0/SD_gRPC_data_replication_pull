#!/usr/bin/env python3

import argparse
import json
import os
import random
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

N_NODES = 4
QUORUM = N_NODES // 2 + 1          # quórum fixo: 3 de 4
SERVICE = "raft.Node"
FOLLOWER, CANDIDATE, LEADER = "SEGUIDOR", "CANDIDATO", "LIDER"
WRITE_TIMEOUT = 5.0                # tempo máx. que o líder espera o commit de uma escrita
MAX_BATCH = 50                     # máx. de entradas por FetchResponse
CHANNEL_OPTS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
]


def enc(obj):
    return json.dumps(obj).encode("utf-8")


def dec(data):
    return json.loads(data.decode("utf-8"))


class Node:
    def __init__(self, node_id, election_timeout, fetch_interval, heartbeat_timeout,
                 host, base_port, reset):
        self.id = node_id
        self.base_timeout = election_timeout
        self.fetch_interval = fetch_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.peers = {i: f"{host}:{base_port + i}" for i in range(1, N_NODES + 1) if i != node_id}

        self.lock = threading.RLock()
        self.commit_cond = threading.Condition(self.lock)

        # ---------- estado persistente ----------
        self.dir = f"dados_no{node_id}"
        if reset and os.path.isdir(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(self.dir, exist_ok=True)
        self.current_term = 0
        self.voted_for = None
        self.log = []          # entradas: {index, term, key, value[, noop]}; índice i está em log[i-1]
        self.commit_index = 0
        self.db = {}           # banco final: somente dados comitados
        self.load()

        # ---------- estado volátil ----------
        self.role = FOLLOWER
        self.leader_id = None
        self.election_deadline = 0
        self.reset_election_timer()
        self.last_fetch = {}       # (líder) id da réplica -> instante do último FetchRequest
        self.replica_interval = {} # (líder) id da réplica -> intervalo de fetch informado
        self.match_index = {}      # (líder) id da réplica -> último índice replicado
        self.frozen = False
        self.rr = 0

        self.rpc = {}
        for pid, addr in self.peers.items():
            ch = grpc.insecure_channel(addr, options=CHANNEL_OPTS)
            self.rpc[pid] = {
                m: ch.unary_unary(f"/{SERVICE}/{m}", request_serializer=enc, response_deserializer=dec)
                for m in ("RequestVote", "Fetch")
            }

    # ------------------------------------------------------------------ util
    def say(self, msg):
        print(f"[{time.strftime('%H:%M:%S')}] [NÓ {self.id}] {msg}", flush=True)

    def status_line(self):
        extra = ""
        if self.role == LEADER:
            extra = f" ativos={self.active_count()}/{N_NODES}"
        if self.frozen:
            extra += " (CONGELADO)"
        return (f"papel={self.role} termo={self.current_term} commit={self.commit_index} "
                f"ult_indice={self.last_index()} lider={self.leader_id}{extra}")

    # ----------------------------------------------------------- persistência
    def _write(self, name, obj):
        path = os.path.join(self.dir, name)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def persist_state(self):
        self._write("estado.json", {"current_term": self.current_term, "voted_for": self.voted_for})

    def persist_log(self):
        self._write("log.json", self.log)

    def persist_db(self):
        self._write("banco.json", {"commit_index": self.commit_index, "dados": self.db})

    def load(self):
        def read(name):
            p = os.path.join(self.dir, name)
            if os.path.exists(p):
                with open(p, encoding="utf-8") as f:
                    return json.load(f)
            return None
        st = read("estado.json")
        if st:
            self.current_term = st["current_term"]
            self.voted_for = st["voted_for"]
        lg = read("log.json")
        if lg:
            self.log = lg
        bd = read("banco.json")
        if bd:
            self.commit_index = bd["commit_index"]
            self.db = bd["dados"]

    # --------------------------------------------------------------- log
    def last_index(self):
        return len(self.log)

    def last_term(self):
        return self.log[-1]["term"] if self.log else 0

    def term_at(self, idx):
        if idx == 0:
            return 0
        if idx <= len(self.log):
            return self.log[idx - 1]["term"]
        return None

    def apply_commit(self, new_commit):
        """Efetiva no banco final as entradas até new_commit (só dados comitados ficam visíveis)."""
        for i in range(self.commit_index + 1, new_commit + 1):
            e = self.log[i - 1]
            if not e.get("noop"):
                self.db[e["key"]] = e["value"]
        self.commit_index = new_commit
        self.persist_db()
        self.say(f"índice de commit avançou -> {self.commit_index}")
        self.commit_cond.notify_all()

    # --------------------------------------------------------- termo / papéis
    def reset_election_timer(self):
        self.election_deadline = time.time() + random.uniform(self.base_timeout, self.base_timeout * 1.5)

    def set_term(self, term):
        """Atualização obrigatória de termo: termo maior => vira Seguidor."""
        self.current_term = term
        self.voted_for = None
        self.persist_state()
        self.leader_id = None
        if self.role != FOLLOWER:
            self.say(f"viu termo {term} maior -> vira SEGUIDOR")
            self.role = FOLLOWER
            self.reset_election_timer()
            self.commit_cond.notify_all()

    def become_leader(self, voters):
        self.role = LEADER
        self.leader_id = self.id
        self.match_index = {}
        self.last_fetch = {p: time.time() for p in voters}
        # Como no KRaft: o novo líder registra uma entrada de controle no seu termo.
        # Isso permite comitar (indiretamente) entradas de termos anteriores.
        self.log.append({"index": self.last_index() + 1, "term": self.current_term,
                         "key": None, "value": None, "noop": True})
        self.persist_log()
        self.say(f"*** ELEITO LÍDER no termo {self.current_term} ***")

    def active_count(self):
        now = time.time()
        n = 1  # o próprio líder
        for p in self.peers:
            lim = max(self.heartbeat_timeout, 3 * self.replica_interval.get(p, 0))
            if now - self.last_fetch.get(p, 0) <= lim:
                n += 1
        return n

    # ------------------------------------------------------------- eleição
    def ticker(self):
        while True:
            time.sleep(0.02)
            with self.lock:
                if self.frozen or self.role == LEADER or time.time() < self.election_deadline:
                    continue
            try:
                self.start_election()
            except Exception as e:  # noqa
                self.say(f"erro na eleição: {e}")

    def start_election(self):
        with self.lock:
            self.role = CANDIDATE
            self.current_term += 1
            self.voted_for = self.id
            self.leader_id = None
            self.persist_state()
            self.reset_election_timer()
            term = self.current_term
            req = {"term": term, "candidate_id": self.id,
                   "last_log_index": self.last_index(), "last_log_term": self.last_term()}
            self.say(f"timeout do líder -> vira CANDIDATO, termo {term}; pedindo votos")

        def ask(pid):
            try:
                return pid, self.rpc[pid]["RequestVote"](req, timeout=0.5)
            except grpc.RpcError:
                return pid, None

        with ThreadPoolExecutor(len(self.peers)) as ex:
            results = list(ex.map(ask, list(self.peers)))

        with self.lock:
            if self.role != CANDIDATE or self.current_term != term:
                return
            votes, voters = 1, []
            for pid, r in results:
                if r is None:
                    continue
                if r["term"] > self.current_term:
                    self.set_term(r["term"])
                    return
                if r["vote_granted"]:
                    votes += 1
                    voters.append(pid)
            self.say(f"votos recebidos: {votes}/{N_NODES} (quórum {QUORUM})")
            if votes >= QUORUM:
                self.become_leader(voters)

    def RequestVote(self, req, ctx):
        with self.lock:
            t = req["term"]
            if t > self.current_term:
                self.set_term(t)
            granted = False
            if t == self.current_term:
                cand_ok = (req["last_log_term"], req["last_log_index"]) >= (self.last_term(), self.last_index())
                if self.voted_for in (None, req["candidate_id"]) and cand_ok:   # voto único por termo
                    self.voted_for = req["candidate_id"]
                    self.persist_state()
                    granted = True
                    self.reset_election_timer()
            self.say(f"RequestVote do nó {req['candidate_id']} (termo {t}): "
                     f"{'VOTO CONCEDIDO' if granted else 'voto negado'}")
            return {"term": self.current_term, "vote_granted": granted}

    # ------------------------------------------------- réplica: loop de fetch
    def fetch_loop(self):
        while True:
            time.sleep(self.fetch_interval)
            if self.frozen:
                continue
            try:
                self.do_fetch()
            except Exception as e:  # noqa
                self.say(f"erro no fetch: {e}")

    def pick_target(self):
        if self.leader_id is not None and self.leader_id != self.id:
            return self.leader_id
        ids = sorted(self.peers)
        self.rr = (self.rr + 1) % len(ids)
        return ids[self.rr]

    def do_fetch(self):
        with self.lock:
            if self.role not in (FOLLOWER, CANDIDATE):
                return
            target = self.pick_target()
            sent_term = self.current_term
            req = {"term": self.current_term, "replica_id": self.id,
                   "last_log_index": self.last_index(), "last_log_term": self.last_term(),
                   "fetch_interval": self.fetch_interval}
        try:
            resp = self.rpc[target]["Fetch"](req, timeout=0.5)
        except grpc.RpcError:
            with self.lock:
                if self.leader_id == target:
                    self.leader_id = None
            return

        with self.lock:
            if self.role not in (FOLLOWER, CANDIDATE) or self.current_term != sent_term:
                return
            if resp["term"] > self.current_term:
                self.set_term(resp["term"])
                if resp.get("is_leader"):
                    self.leader_id = resp["leader_id"]
                    self.reset_election_timer()
                elif resp.get("leader_id") not in (None, self.id):
                    self.leader_id = resp["leader_id"]
                return
            if resp["term"] < self.current_term:
                return
            if not resp["is_leader"]:
                hint = resp.get("leader_id")
                self.leader_id = hint if hint not in (None, self.id, target) else None
                return

            # Resposta do líder legítimo do meu termo
            if self.role == CANDIDATE:
                self.say(f"líder {target} do termo {self.current_term} encontrado -> vira SEGUIDOR")
                self.role = FOLLOWER
            if self.leader_id != target:
                self.say(f"líder atual: nó {target} (termo {self.current_term})")
            self.leader_id = target
            self.reset_election_timer()

            if not resp["success"]:
                # Divergência de log: trunca a partir do ponto conflitante
                tt = resp["truncate_to"]
                if tt < self.last_index() and tt >= self.commit_index:
                    self.say(f"DIVERGÊNCIA DE LOG: truncando log do índice {tt + 1} em diante "
                             f"(removendo {self.last_index() - tt} entrada(s) não comitada(s))")
                    self.log = self.log[:tt]
                    self.persist_log()
                return

            entries = resp["entries"]
            if entries and entries[0]["index"] == self.last_index() + 1:
                self.log.extend(entries)
                self.persist_log()
                self.say(f"recebeu {len(entries)} entrada(s) do líder via Fetch "
                         f"(índices {entries[0]['index']}..{entries[-1]['index']}) - ainda NÃO comitadas")
            new_commit = min(resp["commit_index"], self.last_index())
            if new_commit > self.commit_index:
                self.apply_commit(new_commit)

    # ------------------------------------------------------ líder: Fetch
    def Fetch(self, req, ctx):
        if self.frozen:
            ctx.abort(grpc.StatusCode.UNAVAILABLE, "nó congelado")
        with self.lock:
            t = req["term"]
            if t > self.current_term:
                self.set_term(t)
            base = {"term": self.current_term, "leader_id": self.leader_id,
                    "is_leader": self.role == LEADER, "success": False}
            if self.role != LEADER or t < self.current_term:
                return base

            rid, li, lt = req["replica_id"], req["last_log_index"], req["last_log_term"]
            self.last_fetch[rid] = time.time()                     # heartbeat
            self.replica_interval[rid] = req.get("fetch_interval", 0)

            if li > self.last_index():                             # réplica com log mais longo
                return {**base, "truncate_to": self.last_index()}
            if self.term_at(li) != lt:                             # termo conflitante
                return {**base, "truncate_to": li - 1}

            self.match_index[rid] = li                             # réplica confirma ter o log até li
            self.advance_commit()
            return {**base, "success": True, "entries": self.log[li:li + MAX_BATCH],
                    "commit_index": self.commit_index}

    def advance_commit(self):
        """commit_index = maior índice presente em >= QUORUM nós (e do termo atual)."""
        if self.role != LEADER:
            return
        idxs = sorted([self.last_index()] + [self.match_index.get(p, 0) for p in self.peers], reverse=True)
        n = idxs[QUORUM - 1]
        if n > self.commit_index and self.term_at(n) == self.current_term:
            self.apply_commit(n)

    # ------------------------------------------------- cliente: Write / Read
    def Write(self, req, ctx):
        with self.lock:
            if self.role != LEADER:
                return {"success": False, "is_leader": False, "leader_id": self.leader_id,
                        "message": f"nó {self.id} é {self.role}, não é o líder"}
            active = self.active_count()
            # (no modo freeze o quórum é ignorado, para simular o crash antes da replicação)
            if active < QUORUM and not self.frozen:
                self.say(f"ESCRITA REJEITADA: apenas {active} nós ativos (mínimo {QUORUM})")
                return {"success": False, "is_leader": True, "leader_id": self.id,
                        "message": f"sem quórum: apenas {active} de {N_NODES} nós ativos (mínimo {QUORUM})"}
            idx = self.last_index() + 1
            term = self.current_term
            self.log.append({"index": idx, "term": term, "key": req["key"], "value": req["value"]})
            self.persist_log()
            self.say(f"escrita {req['key']}={req['value']} gravada no log local (índice {idx}, termo {term})")

            deadline = time.time() + WRITE_TIMEOUT
            while self.commit_index < idx:
                if self.role != LEADER or self.current_term != term:
                    return {"success": False, "is_leader": False, "leader_id": self.leader_id,
                            "message": "liderança perdida antes do commit"}
                remaining = deadline - time.time()
                if remaining <= 0:
                    return {"success": False, "is_leader": True, "leader_id": self.id,
                            "message": "timeout: escrita não alcançou o quórum (não comitada)"}
                self.commit_cond.wait(remaining)
            if self.term_at(idx) != term:
                return {"success": False, "is_leader": False, "leader_id": self.leader_id,
                        "message": "entrada descartada após troca de líder"}
            return {"success": True, "is_leader": True, "leader_id": self.id, "index": idx,
                    "term": term, "commit_index": self.commit_index,
                    "message": "escrita comitada"}

    def Read(self, req, ctx):
        with self.lock:   # só dados comitados (banco final)
            base = {"node_id": self.id, "role": self.role, "term": self.current_term,
                    "commit_index": self.commit_index}
            key = req.get("key")
            if key is None:
                return {**base, "found": True, "dados": dict(self.db)}
            if key in self.db:
                return {**base, "found": True, "value": self.db[key]}
            return {**base, "found": False}

    def Status(self, req, ctx):
        with self.lock:
            return {"node_id": self.id, "role": self.role, "term": self.current_term,
                    "voted_for": self.voted_for, "leader_id": self.leader_id,
                    "commit_index": self.commit_index, "last_log_index": self.last_index(),
                    "frozen": self.frozen, "dados": dict(self.db),
                    "log": [{**e, "committed": e["index"] <= self.commit_index} for e in self.log]}

    # ------------------------------------------------- console e exibição
    def status_loop(self):
        while True:
            time.sleep(2)
            with self.lock:
                self.say(self.status_line())

    def console(self):
        for line in sys.stdin:
            cmd = line.strip().lower()
            with self.lock:
                if cmd == "status":
                    self.say(self.status_line())
                elif cmd == "log":
                    for e in self.log:
                        mark = "COMITADO" if e["index"] <= self.commit_index else "não comitado"
                        what = "(controle: novo líder)" if e.get("noop") else f"{e['key']}={e['value']}"
                        print(f"   #{e['index']} termo={e['term']} {what} [{mark}]", flush=True)
                elif cmd == "dados":
                    print(f"   banco final (comitado): {self.db}", flush=True)
                elif cmd == "freeze":
                    self.frozen = True
                    self.say("CONGELADO (não responde Fetch, não busca, não elege)")
                elif cmd == "unfreeze":
                    self.frozen = False
                    self.reset_election_timer()
                    self.say("DESCONGELADO")
                elif cmd:
                    print("   comandos: status | log | dados | freeze | unfreeze", flush=True)

    def serve(self, port):
        handlers = {
            m: grpc.unary_unary_rpc_method_handler(getattr(self, m), request_deserializer=dec,
                                                   response_serializer=enc)
            for m in ("RequestVote", "Fetch", "Write", "Read", "Status")
        }
        server = grpc.server(ThreadPoolExecutor(max_workers=32))
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(SERVICE, handlers),))
        server.add_insecure_port(f"0.0.0.0:{port}")
        server.start()
        self.say(f"escutando na porta {port} | timeout_base={self.base_timeout}s "
                 f"fetch={self.fetch_interval}s | {self.status_line()}")
        for fn in (self.ticker, self.fetch_loop, self.status_loop, self.console):
            threading.Thread(target=fn, daemon=True).start()
        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            self.say("encerrando (simulação de queda)")
            server.stop(0)
            os._exit(0)


def main():
    ap = argparse.ArgumentParser(description="Nó do cluster (replicação Pull + eleição estilo Raft)")
    ap.add_argument("--id", type=int, required=True, choices=range(1, N_NODES + 1), metavar="ID",
                    help="id do nó (1 a 4)")
    ap.add_argument("--timeout", type=float, default=3.0,
                    help="election timeout base em segundos (real: aleatório em [t, 1.5t])")
    ap.add_argument("--fetch-interval", type=float, default=0.1, help="intervalo de FetchRequest (s)")
    ap.add_argument("--heartbeat-timeout", type=float, default=1.0,
                    help="tempo sem fetch para o líder considerar a réplica inativa (s)")
    ap.add_argument("--host", default="localhost", help="host dos demais nós")
    ap.add_argument("--base-port", type=int, default=50050, help="porta do nó = base-port + id")
    ap.add_argument("--reset", action="store_true", help="apaga dados persistidos antes de iniciar")
    a = ap.parse_args()

    node = Node(a.id, a.timeout, a.fetch_interval, a.heartbeat_timeout, a.host, a.base_port, a.reset)
    node.serve(a.base_port + a.id)


if __name__ == "__main__":
    main()
