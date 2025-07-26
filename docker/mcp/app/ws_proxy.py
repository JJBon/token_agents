# ws_proxy.py
import asyncio
import json
from fastapi import FastAPI, WebSocket
import uvicorn
import subprocess

app = FastAPI()

@app.websocket("/")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    proc = await asyncio.create_subprocess_exec(
        "python", "/app/dbt_semantic_layer_mcp_server.py",
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    async def from_ws():
        try:
            while True:
                data = await websocket.receive_text()
                proc.stdin.write((data + "\n").encode())
                await proc.stdin.drain()
        except Exception:
            await websocket.close()

    async def from_mcp():
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                await websocket.send_text(line.decode().strip())
        except Exception:
            await websocket.close()

    await asyncio.gather(from_ws(), from_mcp())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8765)
