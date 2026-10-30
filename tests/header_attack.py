"""Minimal HTTP/2 peer for response-frame rejection tests; not a Flight server."""
import argparse
import socket


def read_exact(conn, n):
    data = bytearray()
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise EOFError("client disconnected")
        data.extend(chunk)
    return bytes(data)


def frame(kind, flags, stream_id, body):
    return len(body).to_bytes(3, "big") + bytes([kind, flags]) + stream_id.to_bytes(4, "big") + body


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--mode", choices=("repeat", "budget", "invalid-grpc", "http-failure", "missing-grpc", "status-overwrite"), required=True)
    args = parser.parse_args()
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", args.port))
        listener.listen(1)
        print("ready", flush=True)
        conn, _ = listener.accept()
        with conn:
            conn.settimeout(10)
            if read_exact(conn, 24) != b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n":
                raise ValueError("bad HTTP/2 preface")
            conn.sendall(frame(4, 0, 0, b""))
            while True:
                header = read_exact(conn, 9)
                payload = read_exact(conn, int.from_bytes(header[:3], "big"))
                if header[3] == 0 and header[4] & 1 and int.from_bytes(header[5:9], "big") == 1:
                    break
            if args.mode == "repeat":
                # Indexed HPACK :status: 200, then another non-final header block.
                conn.sendall(frame(1, 4, 1, b"\x88") + frame(1, 4, 1, b"\x88"))
            elif args.mode == "budget":
                # The frame length alone exceeds the deliberately small budget.
                conn.sendall(frame(1, 4, 1, b"\x88" * 100))
            else:
                grpc = b"\x00\x0bgrpc-status\x01"
                if args.mode == "invalid-grpc":
                    response = frame(1, 4, 1, b"\x88") + frame(1, 5, 1, grpc + b"x")
                elif args.mode == "http-failure":
                    response = frame(1, 5, 1, b"\x8e" + grpc + b"0")
                elif args.mode == "missing-grpc":
                    response = frame(1, 5, 1, b"\x88")
                else:
                    response = frame(1, 4, 1, b"\x8e") + frame(1, 5, 1, b"\x88" + grpc + b"0")
                conn.sendall(response)
            try:
                while conn.recv(4096):
                    pass
            except (ConnectionResetError, socket.timeout):
                pass


if __name__ == "__main__":
    main()
