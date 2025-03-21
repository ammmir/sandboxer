from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, PlainTextResponse, FileResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel
from contextlib import asynccontextmanager
import asyncio
import time
import httpx
import uuid
import pickle
import json
import os
import websockets

from container_engine import ContainerEngine, Container

# configuration
CONTAINER_PREFIX = "sandbox_"
SANDBOX_DB_PATH = "sandbox.db"
IDLE_TIMEOUT = 24*60*60
CHECK_INTERVAL = 60
PROXY_TIMEOUT = 30

engine = ContainerEngine()
hx = httpx.AsyncClient()
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

class ForkSandboxRequest(BaseModel):
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
        print(f'Error starting sandbox:', e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sandboxes/{sandbox_id}/fork")
async def fork_sandbox(sandbox_id: str, request: ForkSandboxRequest):
    """Fork an existing sandbox container."""
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")

    parent_data = sandbox_db[sandbox_id]
    child_label = request.label if request.label else f"Fork of {parent_data['label']}"

    try:
        forked_container, _ = await engine.fork_container(sandbox_id, f"{sandbox_id}-fork-{str(uuid.uuid4())[:8]}")
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

@app.post("/sandboxes/{parent_sandbox_id}/coalesce/{final_sandbox_id}")
async def coalesce_sandbox(parent_sandbox_id: str, final_sandbox_id: str):
    """
    Terminates all sandboxes **in the entire subtree** of parent_sandbox_id,
    except for final_sandbox_id, which becomes the new root.
    """

    if parent_sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Parent sandbox not found")

    if final_sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Final sandbox not found")

    # Ensure final_sandbox_id is actually a descendant of parent_sandbox_id
    def collect_tree_nodes(node_id):
        """Recursively collect all sandbox IDs in the tree."""
        nodes = [node_id]
        for child_id in sandbox_db[node_id]["child_ids"]:
            nodes.extend(collect_tree_nodes(child_id))
        return nodes

    full_tree = collect_tree_nodes(parent_sandbox_id)

    if final_sandbox_id not in full_tree:
        raise HTTPException(status_code=400, detail="Final sandbox is not a descendant of the parent sandbox")

    # Remove **only** final_sandbox_id from termination list
    sandboxes_to_terminate = [sbx_id for sbx_id in full_tree if sbx_id != final_sandbox_id]

    print(f'FULL TREE      : {full_tree}')
    print(f'TO TERMINATE   : {sandboxes_to_terminate}')

    if not sandboxes_to_terminate:
        return {"message": "Nothing to coalesce, target is already the root"}

    # Terminate all sandboxes except final_sandbox_id
    for sbx_id in sandboxes_to_terminate:
        try:
            container = await engine.get_container(sbx_id)
            if container:
                await container.stop()
            sandbox_db[sbx_id]["deleted_at"] = time.time()
        except Exception:
            sandbox_db[sbx_id]["deleted_at"] = time.time()

    # Remove references to terminated sandboxes from their parents
    for sbx_id in sandboxes_to_terminate:
        parent_id = sandbox_db[sbx_id]["parent_id"]
        if parent_id and parent_id in sandbox_db:
            sandbox_db[parent_id]["child_ids"].remove(sbx_id)

    # Make final_sandbox_id the **only survivor and root**
    sandbox_db[final_sandbox_id]["parent_id"] = None

    return {
        "message": f"Sandbox {final_sandbox_id} is now the new root, all other branches have been terminated"
    }

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

@app.get("/sandboxes/{sandbox_id}/logs")
async def get_sandbox(sandbox_id: str, stream: bool = False):
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")
    try:
        container = await engine.get_container(sandbox_id)

        if stream:
            # Create a cancellation event
            cancel_event = asyncio.Event()
            
            async def stream_response():
                try:
                    async for line in await container.logs(stream=True, cancel_event=cancel_event):
                        yield line + "\n"
                except asyncio.CancelledError:
                    # Client disconnected, set the cancel event
                    cancel_event.set()
                    raise

            return StreamingResponse(
                stream_response(),
                media_type="text/event-stream",
                background=BackgroundTask(lambda: cancel_event.set())
            )
        else:
            return PlainTextResponse(await container.logs())
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

    def build_tree(node_id, prefix="", is_root=False, is_last=True):
        """Recursively builds the formatted string tree and JSON structure."""
        node = sandbox_db[node_id]
        node_label = f"📦 {node['label']} [{node_id}]"

        # Root node: No connector, just the label
        if is_root:
            tree_repr.append(node_label)
        else:
            connector = "└── " if is_last else "├── "
            tree_repr.append(f"{prefix}{connector}{node_label}")

        # Build JSON representation
        children = sandbox_db[node_id]["child_ids"]
        tree_json = {"id": node_id, "label": node["label"], "children": []}

        new_prefix = prefix + ("    " if is_last else "│   ")

        for i, child_id in enumerate(children):
            tree_json["children"].append(build_tree(child_id, new_prefix, False, i == len(children) - 1))

        return tree_json

    tree_repr = []
    tree_json = build_tree(sandbox_id, "", True, True)

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

@app.api_route("/sandboxes/{sandbox_id}/proxy/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy_request(sandbox_id: str, path: str, request: Request):
    """Proxies HTTP requests into the correct sandbox using its private IP and exposed port."""
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")

    container = await engine.get_container(sandbox_id)
    if not container or not container.ip_address or not container.exposed_port:
        raise HTTPException(status_code=500, detail="Sandbox container is missing network info")

    sandbox_url = f"http://{container.ip_address}:{container.exposed_port}/{path}"
    #print(f'PROXY URL: {sandbox_url}')

    # Extract the real client's IP
    client_ip = request.client.host
    forwarded_for = request.headers.get("x-forwarded-for", "")

    # Append the client IP to the X-Forwarded-For header
    if forwarded_for:
        forwarded_for = f"{forwarded_for}, {client_ip}"
    else:
        forwarded_for = client_ip

    # Sanitize headers for security
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in {"host", "x-forwarded-for", "x-real-ip"}
    }

    headers["X-Forwarded-For"] = forwarded_for

    # Create an HTTP client (without context manager)
    client = httpx.AsyncClient()

    async def stream_request_body(request: Request):
        """Generator that streams the request body"""
        async for chunk in request.stream():
            #print(f'>>>> CHUNK: {chunk}')
            yield chunk

    try:
        req = client.build_request(request.method, sandbox_url,
            headers=headers,
            params=request.query_params,
            content=stream_request_body(request),  # Stream the request body properly
            timeout=PROXY_TIMEOUT,
        )
        response = await client.send(req, stream=True)

        async def stream_response_body():
            """Generator that streams the response body"""
            async for chunk in response.aiter_raw():
                #print(f'<<<< CHUNK: {chunk}')
                yield chunk

        # Extract response headers and send them immediately
        response_headers = {k: v for k, v in response.headers.items()}

        return StreamingResponse(
            stream_response_body(),
            status_code=response.status_code,
            headers=dict(response.headers),
            background=BackgroundTask(response.aclose)
        )
    except httpx.RemoteProtocolError as e:
        print(f"Downstream connection error: {e}")
        return JSONResponse(
            {"error": "Sandbox connection was unexpectedly closed"},
            status_code=502
        )
    except asyncio.CancelledError:
        print("Client disconnected. Closing sandbox request.")
    except Exception as e:
        print(f"Error while proxying: {e}")
        raise HTTPException(status_code=502, detail="Sandbox connection failed")

@app.delete("/sandboxes/{sandbox_id}")
async def delete_sandbox(sandbox_id: str):
    if sandbox_id not in sandbox_db:
        raise HTTPException(status_code=404, detail="Sandbox not found")

    container = await engine.get_container(sandbox_id)
    if not container:
        raise HTTPException(status_code=404, detail="Sandbox not found")

    await container.stop()
    parent_id = sandbox_db[sandbox_id]["parent_id"]
    if parent_id and parent_id in sandbox_db:
        sandbox_db[parent_id]["child_ids"].remove(sandbox_id)
    sandbox_db[sandbox_id]["deleted_at"] = time.time()
    return {"message": f"Sandbox {sandbox_id} deleted"}

@app.get("/")
async def serve_ui():
    return FileResponse("ui.html")

@app.websocket("/sandboxes/{sandbox_id}/proxy/{path:path}")
async def proxy_websocket(websocket: WebSocket, sandbox_id: str, path: str):
    """Proxies WebSocket connections into the sandbox."""
    if sandbox_id not in sandbox_db:
        await websocket.close(code=4004, reason="Sandbox not found")
        return

    container = await engine.get_container(sandbox_id)
    if not container or not container.ip_address or not container.exposed_port:
        await websocket.close(code=4005, reason="Sandbox container is missing network info")
        return

    # Get the requested WebSocket subprotocols from the client
    requested_protocols = websocket.headers.get("sec-websocket-protocol", "").split(",")
    requested_protocols = [p.strip() for p in requested_protocols if p.strip()]
    
    # For VNC connections, ensure we have the right protocol
    if "binary" in requested_protocols:
        selected_protocol = "binary"
    else:
        selected_protocol = requested_protocols[0] if requested_protocols else None

    sandbox_url = f"ws://{container.ip_address}:{container.exposed_port}/{path}"
    print(f"Connecting to sandbox WebSocket: {sandbox_url} with protocol: {selected_protocol}")
    
    try:
        # Accept the WebSocket connection with the selected protocol
        await websocket.accept(subprotocol=selected_protocol)
        
        # Connect to the sandbox WebSocket with the same protocol
        additional_headers = {}
        if selected_protocol:
            additional_headers["Sec-WebSocket-Protocol"] = selected_protocol
            
        async with websockets.connect(
            sandbox_url,
            subprotocols=[selected_protocol] if selected_protocol else None,
            additional_headers=additional_headers if additional_headers else None
        ) as sandbox_ws:
            # Create tasks for bidirectional forwarding
            async def forward_to_sandbox():
                try:
                    while True:
                        try:
                            # Receive raw socket data
                            message = await websocket.receive()
                            if message["type"] == "websocket.disconnect":
                                break
                            
                            # Forward the message
                            if message["type"] == "websocket.receive":
                                if "bytes" in message:
                                    await sandbox_ws.send(message["bytes"])
                                elif "text" in message:
                                    await sandbox_ws.send(message["text"])
                        except Exception as e:
                            print(f"Error forwarding to sandbox: {e}")
                            break
                except Exception as e:
                    print(f"Error in forward_to_sandbox: {e}")

            async def forward_to_client():
                try:
                    while True:
                        try:
                            # Receive and forward data
                            message = await sandbox_ws.recv()
                            if isinstance(message, bytes):
                                await websocket.send_bytes(message)
                            else:
                                await websocket.send_text(message)
                        except Exception as e:
                            print(f"Error forwarding to client: {e}")
                            break
                except Exception as e:
                    print(f"Error in forward_to_client: {e}")

            # Run both forwarding tasks
            forward_tasks = [
                asyncio.create_task(forward_to_sandbox()),
                asyncio.create_task(forward_to_client())
            ]
            
            try:
                # Wait for either task to complete
                done, pending = await asyncio.wait(
                    forward_tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    
            except Exception as e:
                print(f"Error in main proxy loop: {e}")
                
    except websockets.exceptions.InvalidStatusCode as e:
        print(f"Invalid status code connecting to sandbox: {e}")
        await websocket.close(code=4006, reason=f"Failed to connect to sandbox: {str(e)}")
    except Exception as e:
        print(f"WebSocket proxy error: {e}")
        try:
            await websocket.close(code=4007, reason="Internal proxy error")
        except Exception:
            pass  # Connection might already be closed

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)