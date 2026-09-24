from __future__ import annotations

import argparse
import getpass
import http.client
import json
import re
import socket
from pathlib import Path

from .config import Settings


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: Path) -> None:
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(str(self.socket_path))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Authenticate this MeshCore companion to a known repeater.",
    )
    parser.add_argument("public_key", help="64-hex repeater public key")
    parser.add_argument(
        "--socket",
        type=Path,
        default=Settings.from_env().socket_path,
        help="meshpincerd Unix socket",
    )
    args = parser.parse_args()

    public_key = args.public_key.lower()
    if re.fullmatch(r"[0-9a-f]{64}", public_key) is None:
        parser.error("public_key must be exactly 64 hexadecimal characters")

    password = getpass.getpass("Repeater guest or admin password: ")
    if len(password.encode("utf-8")) > 15:
        parser.error("repeater password must be at most 15 UTF-8 bytes")

    body = json.dumps({"password": password}).encode("utf-8")
    connection = UnixHTTPConnection(args.socket)
    try:
        connection.request(
            "POST",
            f"/v1/repeaters/{public_key}/login",
            body=body,
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        payload = response.read().decode("utf-8")
    finally:
        connection.close()

    if response.status >= 400:
        raise SystemExit(f"login failed ({response.status}): {payload}")
    print(payload)


if __name__ == "__main__":
    main()
