import asyncio
from app.server import RedisServer

async def main():
    server = RedisServer()
    await server.start()
    
if __name__ == "__main__":
    asyncio.run(main())
