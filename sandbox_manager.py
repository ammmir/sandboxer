from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
import asyncio
import time
import docker
import httpx
import uuid
import json

# configuration
IMAGE_NAME = "fastapi-jupyter-server"
CONTAINER_PREFIX = "sandbox_"
SANDBOX_PORT = 8000
IDLE_TIMEOUT = 60
CHECK_INTERVAL = 60

client = docker.from_env()
hx = httpx.AsyncClient()
last_active = {}

async def terminate_idle_sandboxes():
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()

        for container in await asyncio.to_thread(list_sandboxes):
            sandbox_id = container.id
            last_time = last_active.get(sandbox_id, None)

            if last_time is None:
                print(f"Terminating untracked sandbox {sandbox_id} (server restarted?)")
                try:
                    container.stop()
                    container.remove()
                except docker.errors.NotFound:
                    pass
                continue

            if now - last_time > IDLE_TIMEOUT:
                print(f"Terminating idle sandbox {sandbox_id} (idle for {now - last_time:.1f} seconds)")
                try:
                    container.stop()
                    container.remove()
                    last_active.pop(sandbox_id, None)
                except docker.errors.NotFound:
                    last_active.pop(sandbox_id, None) 

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(terminate_idle_sandboxes())
    yield

app = FastAPI(lifespan=lifespan)

class CreateSandboxRequest(BaseModel):
    lang: str

class ExecuteRequest(BaseModel):
    code: str

def list_sandboxes():
    return client.containers.list(filters={"label": "sbx=1"})

@app.get("/sandboxes")
async def get_sandboxes():
    sandboxes = [
        {"id": container.id, "name": container.name, "status": container.status}
        for container in list_sandboxes()
    ]
    return {"sandboxes": sandboxes}

@app.post("/sandboxes")
async def create_sandbox(request: CreateSandboxRequest):
    if request.lang.lower() != "python":
        raise HTTPException(status_code=400, detail="Only Python sandboxes are supported.")

    container_name = CONTAINER_PREFIX + str(uuid.uuid4())[:8]
    
    try:
        container = client.containers.run(
            IMAGE_NAME,
            name=container_name,
            labels={
                "sbx": "1",
                "sbx_lang": request.lang.lower()
            },
            detach=True,
            stdin_open=False,
            tty=False,
            ports={f"{SANDBOX_PORT}/tcp": 0},  # Auto-assign a port
        )
        last_active[container.id] = time.time()
        return {"id": container.id, "name": container.name, "status": container.status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sandboxes/{sandbox_id}")
async def get_sandbox(sandbox_id: str):
    try:
        container = client.containers.get(sandbox_id)
        if "sbx" not in container.labels:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        ports = container.attrs["NetworkSettings"]["Ports"]
        port_mapping = ports.get(f"{SANDBOX_PORT}/tcp", [])
        if not port_mapping:
            raise HTTPException(status_code=500, detail="No exposed port found")

        host_port = port_mapping[0]["HostPort"]

        return {
            "id": container.id,
            "name": container.name,
            "status": container.status,
            "port": host_port,
        }
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="Sandbox not found")

@app.post("/sandboxes/{sandbox_id}/execute")
async def execute_code(sandbox_id: str, request: ExecuteRequest):
    if not request.code.strip():
        raise HTTPException(status_code=400, detail="Code cannot be empty.")
    try:
        container = client.containers.get(sandbox_id)
        if "sbx" not in container.labels:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        ports = container.attrs["NetworkSettings"]["Ports"]
        port_mapping = ports.get(f"{SANDBOX_PORT}/tcp", [])
        if not port_mapping:
            raise HTTPException(status_code=500, detail="No exposed port found")

        host_port = port_mapping[0]["HostPort"]
        sandbox_url = f"http://localhost:{host_port}/execute"

        async def stream_response():
            async with hx.stream("POST", sandbox_url, json=request.dict()) as response:
                if not response.is_success:
                    raise HTTPException(status_code=response.status_code, detail=f"Execution failed")
                async for chunk in response.aiter_bytes():
                    yield chunk
                    last_active[sandbox_id] = time.time()

        return StreamingResponse(stream_response(), media_type="application/x-ndjson")
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="Sandbox not found")

@app.delete("/sandboxes/{sandbox_id}")
async def delete_sandbox(sandbox_id: str):
    try:
        container = client.containers.get(sandbox_id)
        if "sbx" not in container.labels:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        container.stop()
        container.remove()
        last_active.pop(sandbox_id, None)
        return {"message": f"Sandbox {sandbox_id} deleted"}
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="Sandbox not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)