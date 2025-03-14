from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
import asyncio
import time
import uuid
import pickle
import json
import os

from container_engine import ContainerEngine, Container

# configuration
CONTAINER_PREFIX = "sandbox_"
SANDBOX_DB_PATH = "sandbox.db"
SANDBOX_PORT = 8000
IDLE_TIMEOUT = 60
CHECK_INTERVAL = 60

engine = ContainerEngine()
sandbox_db = {}

async def terminate_idle_sandboxes():
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()

        for sandbox_id in list(sandbox_db.keys()):
            last_time = sandbox_db[sandbox_id]["last_active_at"]
            running = sandbox_db[sandbox_id].get("deleted_at", 0)

            if not running and now - last_time > IDLE_TIMEOUT:
                print(f"Terminating idle sandbox {sandbox_id} (idle for {now - last_time:.1f} seconds)")
                try:
                    container = await engine.get_container(sandbox_id)
                    await container.stop()
                    sandbox_db[sandbox_id]["delete_reason"] = "idle"
                except Exception:
                    sandbox_db[sandbox_id]["delete_reason"] = "exit"
                finally:
                    sandbox_db[sandbox_id]["deleted_at"] = now

def load_db():
    global sandbox_db
    if os.path.exists(SANDBOX_DB_PATH):
        with open(SANDBOX_DB_PATH, "rb") as f:
            sandbox_db = pickle.load(f)

def save_db():
    try:
        with open(f"{SANDBOX_DB_PATH}.tmp", "wb") as f:
            pickle.dump(sandbox_db, f)
            f.flush()
            os.fsync(f.fileno())
        os.rename(f"{SANDBOX_DB_PATH}.tmp", SANDBOX_DB_PATH)
    except Exception as e:
        print(f"Failed to save: {e}", file=sys.stderr)

async def periodic_save():
    while True:
        save_db()
        await asyncio.sleep(10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_db()
    asyncio.create_task(terminate_idle_sandboxes())
    asyncio.create_task(periodic_save())
    yield
    save_db()

app = FastAPI(lifespan=lifespan)

class CreateSandboxRequest(BaseModel):
    image: str
    label: str

class ExecuteRequest(BaseModel):
    code: str

class UpdateSandboxRequest(BaseModel):
    label: str

@app.get("/sandboxes")
async def get_sandboxes():
    sandboxes = []
    all_containers = await engine.list_containers()

    for container in all_containers:
        sandbox_data = sandbox_db.get(container.id, {})

        sandboxes.append({
            "id": container.id,
            "name": container.name,
            "status": container.status,
            "ip_address": container.ip_address,
            "label": sandbox_data.get("label", ""),
            "parent_id": sandbox_data.get("parent_id", None),
            "child_ids": sandbox_data.get("child_ids", []),
        })

    return {"sandboxes": sandboxes}

@app.post("/sandboxes")
async def create_sandbox(request: CreateSandboxRequest):
    container_name = CONTAINER_PREFIX + str(uuid.uuid4())[:8]
    label = request.label if request.label else container_name
    
    try:
        container = await engine.start_container(image=request.image, name=container_name)
        sandbox_db[container.id] = {
            "label": label,
            "last_active_at": time.time(),
            "image": request.image,
            "parent_id": None,
            "child_ids": []
        }
        return {"id": container.id, "name": container.name, "label": label}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sandboxes/{sandbox_id}/fork")
async def fork_sandbox(sandbox_id: str, request: CreateSandboxRequest):
    """Fork an existing sandbox container."""
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")

    parent_data = sandbox_db[sandbox_id]
    child_label = request.label if request.label else f"Fork of {parent_data['label']}"

    try:
        forked_container = await engine.fork_container(sandbox_id, f"{sandbox_id}-fork-{str(uuid.uuid4())[:8]}")
        sandbox_db[forked_container.id] = {
            "label": child_label,
            "last_active_at": time.time(),
            "image": parent_data["image"],
            "parent_id": sandbox_id,
            "child_ids": [],
        }
        sandbox_db[sandbox_id]["child_ids"].append(forked_container.id)

        return {"id": forked_container.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sandboxes/{sandbox_id}")
async def get_sandbox(sandbox_id: str):
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")
    try:
        container = await engine.get_container(sandbox_id)

        return {
            "id": container.id,
            "name": container.name,
            "status": container.status,
            "ip_address": container.ip_address,
            "label": sandbox_db[sandbox_id]["label"],
            "parent_id": sandbox_db[sandbox_id]["parent_id"],
            "child_ids": sandbox_db[sandbox_id]["child_ids"],
        }
    except:
        raise HTTPException(status_code=404, detail="Sandbox not found")

@app.patch("/sandboxes/{sandbox_id}")
async def update_sandbox_label(sandbox_id: str, request: UpdateSandboxRequest):
    if not request.label.strip():
        raise HTTPException(status_code=400, detail="Label cannot be empty.")
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")
    sandbox_db[sandbox_id]["label"] = request.label
    return {"id": sandbox_id, "label": request.label}

@app.get("/sandboxes/{sandbox_id}/tree")
async def get_sandbox_tree(sandbox_id: str):
    """Return the fork tree of a sandbox as both formatted text and JSON."""
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")

    def build_tree(node_id, prefix=""):
        """Builds both string and JSON representations of the tree."""
        node = sandbox_db[node_id]
        node_label = f"{node['label']} [{node_id}]"
        
        # Add to string representation
        tree_repr.append(f"{prefix} 📦 {node_label}")

        # Build JSON representation
        children = [build_tree(child_id, prefix + " ├──") for child_id in node["child_ids"]]
        return {"id": node_id, "label": node["label"], "children": children}

    tree_repr = []
    tree_json = build_tree(sandbox_id)

    return {
        "tree": "\n".join(tree_repr),  # Pretty-print tree
        "tree_json": tree_json         # JSON representation
    }

@app.post("/sandboxes/{sandbox_id}/execute")
async def execute_code(sandbox_id: str, request: ExecuteRequest, stream: bool = False):
    if not request.code.strip():
        raise HTTPException(status_code=400, detail="Code cannot be empty.")
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")

    try:
        container = await engine.get_container(sandbox_id)
        if not container:
            raise HTTPException(status_code=404, detail="Sandbox container not found")

        if stream:
            async def stream_response():
                async for stream_type, data, exit_code in await container.exec(request.code, stream=True):
                    yield json.dumps({"type": stream_type, "output": data, "exit_code": exit_code}) + "\n"

            return StreamingResponse(stream_response(), media_type="application/x-ndjson")

        else:
            stdout, stderr, exit_code = await container.exec(request.code, stream=False)
            return {
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": exit_code
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/sandboxes/{sandbox_id}")
async def delete_sandbox(sandbox_id: str):
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")
    try:
        container = await engine.get_container(sandbox_id)
        container.stop()
        sandbox_db[sandbox_id]["deleted_at"] = time.time()
        # TODO: remove from parent's child list
        return {"message": f"Sandbox {sandbox_id} deleted"}
    except e:
        raise HTTPException(status_code=404, detail="Sandbox not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)