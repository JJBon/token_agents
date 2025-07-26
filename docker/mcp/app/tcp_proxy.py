import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class MCPManager:
    def __init__(self, cmd):
        self.cmd = cmd
        self.proc = None
        self.stdout_queue = asyncio.Queue()

    async def start(self):
        logging.info("Starting MCP server subprocess: %s", " ".join(self.cmd))
        self.proc = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=sys.stderr
        )
        asyncio.create_task(self._stdout_reader())

    async def _stdout_reader(self):
        """Continuously reads MCP stdout and pushes lines into the queue."""
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                logging.info("MCP process stdout closed.")
                break
            await self.stdout_queue.put(line)
            logging.debug(f"[MCP STDOUT] {line.decode().rstrip()}")

    async def read_stdout(self):
        """Fetch the next available line from MCP stdout."""
        return await self.stdout_queue.get()

    async def write_stdin(self, data: bytes):
        """Write data to MCP stdin."""
        if self.proc and self.proc.stdin:
            logging.debug(f"Writing to MCP stdin: {data.decode().rstrip()}")
            self.proc.stdin.write(data)
            await self.proc.stdin.drain()


async def handle_client(reader, writer, mcp_manager: MCPManager):
    client_addr = writer.get_extra_info('peername')
    logging.info(f"New client connected: {client_addr}")

    async def read_from_client():
        while not reader.at_eof():
            data = await reader.readline()
            if not data:
                break
            logging.debug(f"Client -> MCP: {data.decode().rstrip()}")
            await mcp_manager.write_stdin(data)

    async def write_to_client():
        while True:
            line = await mcp_manager.read_stdout()
            if not line:
                break
            logging.debug(f"MCP -> Client: {line.decode().rstrip()}")
            writer.write(line)
            await writer.drain()

    await asyncio.gather(read_from_client(), write_to_client())

    logging.info(f"Client disconnected: {client_addr}")
    writer.close()
    await writer.wait_closed()


async def main():
    mcp_manager = MCPManager(["python", "dbt_semantic_layer_mcp_server.py"])
    await mcp_manager.start()

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, mcp_manager),
        "0.0.0.0",
        8765
    )
    logging.info("TCP Proxy running on %s", server.sockets[0].getsockname())

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down TCP Proxy.")
