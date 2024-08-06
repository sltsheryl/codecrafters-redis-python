import asyncio
import argparse
from app.server import RedisServer

async def main(port):
    server = RedisServer(role='master', port=port)
    await server.start()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis Server")
    parser.add_argument("--port", type=int, default=6379, help="Port to run the server on")
    args = parser.parse_args()
    asyncio.run(main(args.port))
