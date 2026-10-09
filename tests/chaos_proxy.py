"""Tiny toxiproxy-style TCP proxy for failure injection (no external tools needed).

    proxy = ChaosProxy(listen_port=5701, upstream=("127.0.0.1", 5601)); proxy.start()
    proxy.down()            # drop all connections / refuse traffic  (simulates outage)
    proxy.up()
    proxy.set_latency(0.3)  # add 300 ms to each forwarded chunk
    proxy.reset_after(n)    # kill connection after n bytes (simulates connection reset mid-upload)
"""
import socket
import threading
import time


class ChaosProxy:
    def __init__(self, listen_port: int, upstream: tuple[str, int]):
        self.port, self.upstream = listen_port, upstream
        self._down = False
        self._latency = 0.0
        self._reset_after: int | None = None
        self._srv: socket.socket | None = None
        self._conns: list[socket.socket] = []
        self._lock = threading.Lock()
        self._stop = False

    # ---- controls ----
    def down(self):
        self._down = True
        with self._lock:
            for c in self._conns:
                try: c.close()
                except OSError: pass
            self._conns.clear()

    def up(self): self._down = False
    def set_latency(self, s: float): self._latency = s
    def reset_after(self, n: int | None): self._reset_after = n

    # ---- plumbing ----
    def start(self):
        self._srv = socket.socket()
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", self.port)); self._srv.listen(128)
        threading.Thread(target=self._accept, daemon=True).start()
        return self

    def stop(self):
        self._stop = True
        try: self._srv.close()
        except OSError: pass
        self.down()

    def _accept(self):
        while not self._stop:
            try:
                c, _ = self._srv.accept()
            except OSError:
                return
            if self._down:
                c.close(); continue
            threading.Thread(target=self._handle, args=(c,), daemon=True).start()

    def _handle(self, c: socket.socket):
        try:
            u = socket.create_connection(self.upstream, timeout=5)
        except OSError:
            c.close(); return
        with self._lock:
            self._conns += [c, u]
        sent = [0]

        def pump(a, b, count):
            try:
                while not self._down:
                    d = a.recv(65536)
                    if not d: break
                    if self._latency: time.sleep(self._latency)
                    if count and self._reset_after is not None:
                        sent[0] += len(d)
                        if sent[0] > self._reset_after:
                            self._reset_after = None   # one-shot
                            break
                    b.sendall(d)
            except OSError:
                pass
            finally:
                for s in (a, b):
                    try: s.close()
                    except OSError: pass

        threading.Thread(target=pump, args=(c, u, True), daemon=True).start()
        threading.Thread(target=pump, args=(u, c, False), daemon=True).start()
