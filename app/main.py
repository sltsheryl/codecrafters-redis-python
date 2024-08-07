import asyncio
import argparse
from app.server import RedisServer


async def main(port, replicaof):
    if replicaof:
        master_host, master_port = replicaof.split(" ")
        server = RedisServer(
            role="slave",
            port=port,
            master_host=master_host,
            master_port=int(master_port),
        )
    else:
        server = RedisServer(port=port)
    await server.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis Server")
    parser.add_argument(
        "--port", type=int, default=6379, help="Port to run the server on"
    )
    parser.add_argument("--replicaof", type=str, help="Replicate data from master")
    args = parser.parse_args()
    asyncio.run(main(args.port, args.replicaof))
