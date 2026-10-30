"""Controlled HTTP/2 peer for local timeout and server deadline tests."""
import argparse
import socket


def read_exact(conn, count):
    result = bytearray()
    while len(result) < count:
        chunk = conn.recv(count - len(result))
        if not chunk:
            raise EOFError("client disconnected before request")
        result.extend(chunk)
    return bytes(result)


def frame(kind, flags, stream, data):
    return len(data).to_bytes(3, "big") + bytes([kind, flags]) + stream.to_bytes(4, "big") + data


def wait_request(conn, stream):
    while True:
        header = read_exact(conn, 9)
        read_exact(conn, int.from_bytes(header[:3], "big"))
        if header[3] == 0 and header[4] & 1 and int.from_bytes(header[5:9], "big") == stream:
            return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--mode", choices=("silent", "server-deadline"), required=True)
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
            wait_request(conn, 1)
            if args.mode == "silent":
                # An unresponsive peer forces the client's own deadline to fire.
                while conn.recv(4096):
                    pass
            else:
                # Complete the first RPC with DEADLINE_EXCEEDED and the next with
                # INTERNAL, showing the connection stayed usable between RPCs.
                grpc = b"\x00\x0bgrpc-status"
                conn.sendall(frame(1, 5, 1, b"\x88" + grpc + b"\x014"))
                wait_request(conn, 3)
                conn.sendall(frame(1, 5, 3, b"\x88" + grpc + b"\x0213"))


if __name__ == "__main__":
    main()
