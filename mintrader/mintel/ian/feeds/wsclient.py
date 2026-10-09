"""Reading a public WebSocket market-data stream, with or without a package.

``open_websocket(url)`` gives a connection with ``recv() -> str`` and
``close()``:

* the ``websocket-client`` package when it is installed (it answers the
  server's pings by itself), else
* :class:`SimpleWebSocket`, a small RFC 6455 client on the standard library:
  the HTTP upgrade (with the Sec-WebSocket-Accept check), text and binary
  frames (fragmented or not), and the control frames - a ping is answered
  with a pong carrying the same payload at once, a close is answered and
  ends the connection.

Only what a market-data READER needs: it sends nothing but pongs and the
closing handshake. TLS uses the system's certificates, plus certifi's when
that package is present (a python.org Python on a Mac has none of its own).
Any failure - a refused connection, a timeout, a close - is an exception the
caller turns into a reconnect.
"""
from __future__ import annotations

import base64
import hashlib
import os
import socket
import ssl
import struct
import threading
import urllib.parse
from typing import Optional

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
OP_CONT, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA
USER_AGENT = "mintel-financial-ian/1.0 (public market data reader)"


class WsClosed(ConnectionError):
    """The server closed the connection (or it dropped)."""


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    try:
        import certifi                                          # optional: a python.org Python on a Mac needs it
        ctx.load_verify_locations(certifi.where())
    except Exception:
        pass
    return ctx


def library_available() -> bool:
    try:
        import websocket  # noqa: F401
        return hasattr(websocket, "create_connection")
    except Exception:
        return False


def accept_key(key: str) -> str:
    return base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()


def encode_frame(opcode: int, payload: bytes, mask: bool = True, fin: bool = True,
                 mask_key: Optional[bytes] = None) -> bytes:
    """One frame. A client's frames are always masked (RFC 6455 5.3)."""
    b0 = (0x80 if fin else 0) | (opcode & 0x0F)
    n = len(payload)
    m = 0x80 if mask else 0
    if n < 126:
        head = struct.pack("!BB", b0, m | n)
    elif n < 65536:
        head = struct.pack("!BBH", b0, m | 126, n)
    else:
        head = struct.pack("!BBQ", b0, m | 127, n)
    if not mask:
        return head + payload
    key = mask_key if mask_key is not None else os.urandom(4)
    return head + key + bytes(b ^ key[i % 4] for i, b in enumerate(payload))


class SimpleWebSocket:
    impl = "built-in"

    def __init__(self, url: str, timeout: float = 30.0, ctx: Optional[ssl.SSLContext] = None,
                 max_message: int = 16 << 20):
        self.url = url
        self.timeout = float(timeout)
        self.ctx = ctx
        self.max_message = int(max_message)
        self.sock = None
        self._buf = b""
        self._send_lock = threading.Lock()
        self.close_code: Optional[int] = None

    # ---------------------------------------------------------- connect --
    def connect(self) -> "SimpleWebSocket":
        u = urllib.parse.urlsplit(self.url)
        if u.scheme not in ("ws", "wss"):
            raise ValueError(f"not a WebSocket URL: {self.url}")
        host = u.hostname or ""
        port = u.port or (443 if u.scheme == "wss" else 80)
        path = (u.path or "/") + (f"?{u.query}" if u.query else "")
        sock = socket.create_connection((host, port), timeout=self.timeout)
        try:
            if u.scheme == "wss":
                sock = (self.ctx or ssl_context()).wrap_socket(sock, server_hostname=host)
            sock.settimeout(self.timeout)
            key = base64.b64encode(os.urandom(16)).decode()
            host_hdr = host if u.port is None else f"{host}:{port}"
            req = (f"GET {path} HTTP/1.1\r\nHost: {host_hdr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                   f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: {USER_AGENT}\r\n\r\n")
            sock.sendall(req.encode())
            self.sock = sock
            head = self._read_headers()
            lines = head.split("\r\n")
            parts = lines[0].split(" ", 2)
            if len(parts) < 2 or parts[1] != "101":
                raise ConnectionError(f"the server refused the WebSocket upgrade: {lines[0][:120]}")
            hdrs = {}
            for line in lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    hdrs[k.strip().lower()] = v.strip()
            if hdrs.get("sec-websocket-accept") != accept_key(key):
                raise ConnectionError("the server's WebSocket handshake did not match")
            return self
        except Exception:
            try:
                sock.close()
            except Exception:
                pass
            self.sock = None
            raise

    def _read_headers(self) -> str:
        data = self._buf
        while b"\r\n\r\n" not in data:
            if len(data) > 65536:
                raise ConnectionError("the WebSocket handshake reply is too long")
            chunk = self._recv_some()
            data += chunk
        head, _, rest = data.partition(b"\r\n\r\n")
        self._buf = rest
        return head.decode("latin-1")

    # -------------------------------------------------------------- read --
    def _recv_some(self) -> bytes:
        try:
            chunk = self.sock.recv(65536)
        except socket.timeout as exc:
            raise TimeoutError(f"nothing received for {self.timeout:g} s") from exc
        if not chunk:
            raise WsClosed("the connection was closed")
        return chunk

    def _read_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            self._buf += self._recv_some()
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _read_frame(self) -> tuple[bool, int, bytes]:
        b0, b1 = self._read_exact(2)
        fin, opcode = bool(b0 & 0x80), b0 & 0x0F
        masked, n = bool(b1 & 0x80), b1 & 0x7F
        if n == 126:
            n = struct.unpack("!H", self._read_exact(2))[0]
        elif n == 127:
            n = struct.unpack("!Q", self._read_exact(8))[0]
        if n > self.max_message:
            raise ConnectionError(f"a WebSocket frame of {n} bytes is too large")
        key = self._read_exact(4) if masked else b""
        payload = self._read_exact(n) if n else b""
        if masked:
            payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
        return fin, opcode, payload

    def recv(self) -> str:
        """The next whole message (text, or binary decoded as UTF-8). Pings are answered on the way."""
        if self.sock is None:
            raise WsClosed("not connected")
        parts: list[bytes] = []
        while True:
            fin, opcode, payload = self._read_frame()
            if opcode == OP_PING:
                self._send(OP_PONG, payload[:125])
                continue
            if opcode == OP_PONG:
                continue
            if opcode == OP_CLOSE:
                self.close_code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None
                why = payload[2:].decode("utf-8", "replace") if len(payload) > 2 else ""
                try:
                    self._send(OP_CLOSE, payload[:2])
                except Exception:
                    pass
                self._shutdown()
                raise WsClosed(f"the server closed the stream ({self.close_code or 'no code'}{': ' + why if why else ''})")
            if opcode in (OP_TEXT, OP_BINARY, OP_CONT):
                parts.append(payload)
                if sum(len(p) for p in parts) > self.max_message:
                    raise ConnectionError("a WebSocket message is too large")
                if fin:
                    return b"".join(parts).decode("utf-8", "replace")
                continue
            raise ConnectionError(f"unknown WebSocket opcode {opcode}")

    # ------------------------------------------------------------- write --
    def _send(self, opcode: int, payload: bytes) -> None:
        if self.sock is None:
            raise WsClosed("not connected")
        with self._send_lock:
            self.sock.sendall(encode_frame(opcode, payload))

    def send_text(self, text: str) -> None:
        self._send(OP_TEXT, text.encode())

    def _shutdown(self) -> None:
        s, self.sock = self.sock, None
        if s is not None:
            try:
                s.close()
            except Exception:
                pass

    def close(self) -> None:
        if self.sock is not None:
            try:
                self._send(OP_CLOSE, struct.pack("!H", 1000))
            except Exception:
                pass
        self._shutdown()


class _LibraryWebSocket:
    """The websocket-client package behind the same two methods."""
    impl = "websocket-client"

    def __init__(self, url: str, timeout: float, ctx: Optional[ssl.SSLContext]):
        import websocket
        opts = {"timeout": timeout, "header": [f"User-Agent: {USER_AGENT}"]}
        if url.startswith("wss://"):
            opts["sslopt"] = {"context": ctx or ssl_context()}
        self._ws = websocket.create_connection(url, **opts)

    def recv(self) -> str:
        msg = self._ws.recv()
        if isinstance(msg, bytes):
            msg = msg.decode("utf-8", "replace")
        if msg == "" and not getattr(self._ws, "connected", True):
            raise WsClosed("the connection was closed")
        return msg

    def close(self) -> None:
        try:
            self._ws.close()
        except Exception:
            pass


def open_websocket(url: str, timeout: float = 30.0, prefer_library: bool = True,
                   ctx: Optional[ssl.SSLContext] = None):
    """A connected WebSocket: the websocket-client package when it is installed (and preferred), else the
    built-in client. Raises on failure."""
    if prefer_library and library_available():
        return _LibraryWebSocket(url, timeout, ctx)
    return SimpleWebSocket(url, timeout, ctx).connect()
