#!/usr/bin/env python3

import argparse
import json
import time

import grpc

N_NODES = 4
SERVICE = "raft.Node"
OPTS = [("grpc.initial_reconnect_backoff_ms", 100),
        ("grpc.min_reconnect_backoff_ms", 100),
        ("grpc.max_reconnect_backoff_ms", 500)]


def enc(obj):
    return json.dumps(obj).encode("utf-8")


def dec(data):
    return json.loads(data.decode("utf-8"))


class Cliente:
    def __init__(self, host, base_port):
        self.stubs = {}
        for i in range(1, N_NODES + 1):
            ch = grpc.insecure_channel(f"{host}:{base_port + i}", options=OPTS)
            self.stubs[i] = {m: ch.unary_unary(f"/{SERVICE}/{m}", request_serializer=enc,
                                               response_deserializer=dec)
                             for m in ("Write", "Read", "Status")}
        self.leader_hint = None

    def call(self, nid, method, req, timeout=2.0):
        return self.stubs[nid][method](req, timeout=timeout)

    # ---------------------------------------------------------- escrita
    def escrever(self, key, value):
        target = self.leader_hint or 1
        tried = set()
        for _ in range(8):
            tried.add(target)
            print(f"  -> enviando escrita ao nó {target}...")
            try:
                r = self.call(target, "Write", {"key": key, "value": value}, timeout=10)
            except grpc.RpcError:
                print(f"  nó {target} indisponível")
                self.leader_hint = None
                rest = [n for n in range(1, N_NODES + 1) if n not in tried]
                if not rest:
                    print("  FALHA: nenhum nó respondeu.")
                    return
                target = rest[0]
                continue

            if r["success"]:
                self.leader_hint = target
                print(f"  OK: '{key}={value}' COMITADO pelo líder (nó {target}) "
                      f"índice={r['index']} termo={r['term']} commit={r['commit_index']}")
                return
            if r["is_leader"]:                      # é o líder, mas rejeitou
                self.leader_hint = target
                print(f"  ESCRITA REJEITADA pelo líder (nó {target}): {r['message']}")
                return
            lid = r.get("leader_id")                # não é líder: redirecionamento
            if lid and lid != target:
                print(f"  nó {target} não é líder. Redirecionando para o líder atual: nó {lid}")
                target = lid
                continue
            rest = [n for n in range(1, N_NODES + 1) if n not in tried]
            if not rest:
                print("  FALHA: líder desconhecido (eleição em andamento?). Tente novamente.")
                self.leader_hint = None
                return
            print(f"  nó {target} não conhece o líder; tentando outro nó...")
            target = rest[0]
        print("  FALHA: muitas tentativas sem sucesso.")

    # ----------------------------------------------------------- leitura
    def _alvos(self, arg):
        if arg is None or arg == "todos":
            return list(range(1, N_NODES + 1))
        return [int(arg)]

    def ler(self, key, arg=None):
        for n in self._alvos(arg):
            try:
                r = self.call(n, "Read", {"key": key})
            except grpc.RpcError:
                print(f"  nó {n}: INDISPONÍVEL")
                continue
            head = f"  nó {n} [{r['role']:<9} termo={r['term']} commit={r['commit_index']}]"
            if r["found"]:
                print(f"{head} {key} = {r['value']}")
            else:
                print(f"{head} '{key}' não encontrada (nenhum dado comitado)")

    def dados(self, arg=None):
        for n in self._alvos(arg):
            try:
                r = self.call(n, "Read", {})
            except grpc.RpcError:
                print(f"  nó {n}: INDISPONÍVEL")
                continue
            print(f"  nó {n} [{r['role']} termo={r['term']} commit={r['commit_index']}] {r['dados']}")

    def status(self):
        for n in range(1, N_NODES + 1):
            try:
                r = self.call(n, "Status", {})
            except grpc.RpcError:
                print(f"  nó {n}: INDISPONÍVEL")
                continue
            fz = " (CONGELADO)" if r["frozen"] else ""
            print(f"  nó {n}: {r['role']:<9} termo={r['term']} commit={r['commit_index']} "
                  f"ult_indice={r['last_log_index']} lider={r['leader_id']} voto={r['voted_for']}{fz}")

    def log(self, n):
        try:
            r = self.call(int(n), "Status", {})
        except grpc.RpcError:
            print(f"  nó {n}: INDISPONÍVEL")
            return
        print(f"  log do nó {n} ({r['role']}, termo {r['term']}, commit {r['commit_index']}):")
        if not r["log"]:
            print("    (vazio)")
        for e in r["log"]:
            what = "(controle: novo líder)" if e.get("noop") else f"{e['key']}={e['value']}"
            mark = "COMITADO" if e["committed"] else "NÃO COMITADO (invisível para leitura)"
            print(f"    #{e['index']} termo={e['term']} {what}  [{mark}]")


AJUDA = __doc__.split("Comandos:")[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--base-port", type=int, default=50050, help="porta do nó = base-port + id")
    a = ap.parse_args()
    c = Cliente(a.host, a.base_port)
    print("Cliente do cluster. Comandos:" + AJUDA)

    while True:
        try:
            line = input("cliente> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        p = line.split(maxsplit=2)
        cmd = p[0].lower()
        try:
            if cmd in ("sair", "exit", "quit"):
                break
            elif cmd in ("escrever", "write", "w") and len(p) == 3:
                c.escrever(p[1], p[2])
            elif cmd in ("ler", "read", "r") and len(p) >= 2:
                c.ler(p[1], p[2] if len(p) > 2 else None)
            elif cmd == "dados":
                c.dados(p[1] if len(p) > 1 else None)
            elif cmd == "status":
                c.status()
            elif cmd == "log" and len(p) >= 2:
                c.log(p[1])
            else:
                print("  comando inválido.\n" + AJUDA)
        except ValueError:
            print("  id de nó inválido (use 1 a 4 ou 'todos').")


if __name__ == "__main__":
    main()
