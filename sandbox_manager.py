from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, PlainTextResponse, FileResponse, RedirectResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel
from contextlib import asynccontextmanager, closing
from dotenv import load_dotenv
import asyncio
import time
import httpx
import uuid
import json
import os
import websockets
import sqlite3

from container_engine import ContainerEngine, Container

load_dotenv()

# configuration
CONTAINER_PREFIX = "sandbox_"
DATABASE = os.getenv('DATABASE', 'sandboxer.sqlite')
IDLE_TIMEOUT = 24*60*60
CHECK_INTERVAL = 60
PROXY_TIMEOUT = 30

engine = ContainerEngine()
hx = httpx.AsyncClient()

def init_db():
    with closing(sqlite3.connect(DATABASE)) as db:
        # Existing tables
        db.execute("""
            CREATE TABLE IF NOT EXISTS sandboxes (
                id TEXT PRIMARY KEY,
                name TEXT,
                label TEXT,
                image TEXT,
                network TEXT,
                parent_id TEXT NULL,
                last_active_at INTEGER,
                deleted_at INTEGER NULL,
                delete_reason TEXT NULL,
                FOREIGN KEY(network) REFERENCES networks(name)
            )
        """)
        
        # New tables
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                name TEXT NOT NULL,
                login TEXT NOT NULL,
                avatar_url TEXT,
                access_token TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        
        db.execute("""
            CREATE TABLE IF NOT EXISTS networks (
                name TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)
        
        # Add indexes
        db.execute("CREATE INDEX IF NOT EXISTS idx_sandboxes_network ON sandboxes(network)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_networks_user ON networks(user_id)")
        
        db.commit()

def get_db():
    db = sqlite3.connect(DATABASE)
    db.row_factory = sqlite3.Row
    return db

async def terminate_idle_sandboxes():
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        with closing(get_db()) as db:
            # Mark idle sandboxes as deleted
            db.execute("UPDATE sandboxes SET deleted_at = ?, delete_reason = 'idle' WHERE deleted_at IS NULL AND ? - last_active_at > ? AND id IN (SELECT id FROM sandboxes)", (now, now, IDLE_TIMEOUT))
            # Get sandboxes to stop
            to_stop = db.execute("SELECT id FROM sandboxes WHERE deleted_at = ? AND delete_reason = 'idle'", (now,)).fetchall()
            db.commit()
            
        # Stop containers outside transaction
        for row in to_stop:
            try:
                container = await engine.get_container(row['id'])
                await container.stop()
            except Exception:
                pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(terminate_idle_sandboxes())
    yield

app = FastAPI(lifespan=lifespan)

class CreateSandboxRequest(BaseModel):
    image: str
    label: str
    env: dict[str, str] = {}
    args: list[str] = []

class ForkSandboxRequest(BaseModel):
    label: str

class ExecuteRequest(BaseModel):
    code: str

class UpdateSandboxRequest(BaseModel):
    label: str

@app.get("/sandboxes")
async def get_sandboxes(request: Request):
    sandboxes = []
    with closing(get_db()) as db:
        rows = db.execute("SELECT * FROM sandboxes WHERE network = ? AND deleted_at IS NULL", (request.state.network,)).fetchall()
        for row in rows:
            try:
                container = await engine.get_container(row['id'])
                sandboxes.append({
                    "id": row['id'],
                    "name": container.name,
                    "status": container.status,
                    "ip_address": container.ip_address,
                    "label": row['label'],
                    "parent_id": row['parent_id'],
                    "child_ids": db.execute("SELECT id FROM sandboxes WHERE parent_id = ?", (row['id'],)).fetchall(),
                })
            except Exception as e:
                print(f'ex: ', e)
                pass
            
    return {"sandboxes": sandboxes}

@app.post("/sandboxes")
async def create_sandbox(request: CreateSandboxRequest, req: Request):
    container_name = CONTAINER_PREFIX + str(uuid.uuid4())[:8]
    label = request.label if request.label else container_name
    
    try:
        container = await engine.start_container(
            image=request.image, 
            name=container_name, 
            env=request.env, 
            args=request.args,
            network=req.state.network
        )
        
        with closing(get_db()) as db:
            db.execute(
                "INSERT INTO sandboxes (id, name, label, image, network, parent_id, last_active_at) VALUES (?, ?, ?, ?, ?, NULL, ?)",
                (container.id, container_name, label, request.image, req.state.network, time.time())
            )
            db.commit()
            
        return {"id": container.id, "name": container.name, "label": label}
    except Exception as e:
        print(f'Error starting sandbox:', e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sandboxes/{sandbox_id}/fork")
async def fork_sandbox(sandbox_id: str, request: ForkSandboxRequest, req: Request):
    """Fork an existing sandbox container."""
    with closing(get_db()) as db:
        parent = db.execute("SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL", (sandbox_id, req.state.network)).fetchone()
        if not parent:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        child_label = request.label if request.label else f"Fork of {parent['label']}"
        try:
            forked_container, _ = await engine.fork_container(
                sandbox_id, 
                f"{sandbox_id}-fork-{str(uuid.uuid4())[:8]}"
            )
            
            db.execute(
                "INSERT INTO sandboxes (id, name, label, image, network, parent_id, last_active_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (forked_container.id, forked_container.name, child_label, parent['image'], req.state.network, sandbox_id, time.time())
            )
            db.commit()
            return {"id": forked_container.id}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/sandboxes/{parent_sandbox_id}/coalesce/{final_sandbox_id}")
async def coalesce_sandbox(parent_sandbox_id: str, final_sandbox_id: str, req: Request):
    with closing(get_db()) as db:
        # Check both sandboxes exist in user's network
        parent = db.execute("SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL", 
            (parent_sandbox_id, req.state.network)).fetchone()
        final = db.execute("SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (final_sandbox_id, req.state.network)).fetchone()
            
        if not parent or not final:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        # Recursively collect all descendants of parent
        def collect_tree_nodes(node_id):
            nodes = [node_id]
            children = db.execute("SELECT id FROM sandboxes WHERE parent_id = ? AND deleted_at IS NULL", (node_id,)).fetchall()
            for child in children:
                nodes.extend(collect_tree_nodes(child['id']))
            return nodes

        full_tree = collect_tree_nodes(parent_sandbox_id)
        if final_sandbox_id not in full_tree:
            raise HTTPException(status_code=400, detail="Final sandbox is not a descendant of the parent sandbox")

        sandboxes_to_terminate = [sbx_id for sbx_id in full_tree if sbx_id != final_sandbox_id]
        if not sandboxes_to_terminate:
            return {"message": "Nothing to coalesce, target is already the root"}

        now = time.time()
        # Mark all sandboxes as deleted except final
        db.execute(
            "UPDATE sandboxes SET deleted_at = ?, delete_reason = 'coalesced' WHERE id IN ({})".format(','.join(['?'] * len(sandboxes_to_terminate))),
            [now] + sandboxes_to_terminate
        )
        # Make final sandbox the root
        db.execute("UPDATE sandboxes SET parent_id = NULL WHERE id = ?", (final_sandbox_id,))
        db.commit()

        # Stop containers outside transaction
        for sbx_id in sandboxes_to_terminate:
            try:
                container = await engine.get_container(sbx_id)
                if container:
                    await container.stop()
            except Exception:
                pass

        return {"message": f"Sandbox {final_sandbox_id} is now the new root"}

@app.get("/sandboxes/{sandbox_id}")
async def get_sandbox(sandbox_id: str, req: Request):
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL", 
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        try:
            container = await engine.get_container(sandbox_id)
            child_ids = [row['id'] for row in db.execute("SELECT id FROM sandboxes WHERE parent_id = ? AND deleted_at IS NULL", (sandbox_id,)).fetchall()]
            
            return {
                "id": container.id,
                "name": container.name,
                "status": container.status,
                "ip_address": container.ip_address,
                "label": sandbox['label'],
                "parent_id": sandbox['parent_id'],
                "child_ids": child_ids,
            }
        except:
            raise HTTPException(status_code=404, detail="Sandbox not found")

@app.get("/sandboxes/{sandbox_id}/logs")
async def get_sandbox_logs(req: Request, sandbox_id: str, stream: bool = False):
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        try:
            container = await engine.get_container(sandbox_id)

            # Update last active timestamp
            db.execute(
                "UPDATE sandboxes SET last_active_at = ? WHERE id = ?",
                (time.time(), sandbox_id)
            )
            db.commit()

            if stream:
                cancel_event = asyncio.Event()
                
                async def stream_response():
                    try:
                        async for line in await container.logs(stream=True, cancel_event=cancel_event):
                            yield line + "\n"
                    except asyncio.CancelledError:
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
async def update_sandbox_label(sandbox_id: str, request: UpdateSandboxRequest, req: Request):
    if not request.label.strip():
        raise HTTPException(status_code=400, detail="Label cannot be empty.")
        
    with closing(get_db()) as db:
        result = db.execute(
            "UPDATE sandboxes SET label = ? WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (request.label, sandbox_id, req.state.network)
        )
        db.commit()
        
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Sandbox not found")
            
        return {"id": sandbox_id, "label": request.label}

@app.get("/sandboxes/{sandbox_id}/tree")
async def get_sandbox_tree(sandbox_id: str, req: Request):
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        def build_tree(node_id, prefix="", is_root=False, is_last=True):
            node = db.execute("SELECT * FROM sandboxes WHERE id = ?", (node_id,)).fetchone()
            node_label = f"📦 {node['label']} [{node_id}]"

            if is_root:
                tree_repr.append(node_label)
            else:
                connector = "└── " if is_last else "├── "
                tree_repr.append(f"{prefix}{connector}{node_label}")

            children = db.execute("SELECT id, label FROM sandboxes WHERE parent_id = ? AND deleted_at IS NULL", (node_id,)).fetchall()
            tree_json = {"id": node_id, "label": node['label'], "children": []}

            new_prefix = prefix + ("    " if is_last else "│   ")
            for i, child in enumerate(children):
                tree_json["children"].append(build_tree(child['id'], new_prefix, False, i == len(children) - 1))

            return tree_json

        tree_repr = []
        tree_json = build_tree(sandbox_id, "", True, True)

        return {
            "tree": "\n".join(tree_repr),
            "tree_json": tree_json
        }

@app.post("/sandboxes/{sandbox_id}/execute")
async def execute_code(req: Request, sandbox_id: str, request: ExecuteRequest, stream: bool = False):
    if not request.code.strip():
        raise HTTPException(status_code=400, detail="Code cannot be empty.")
        
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        try:
            container = await engine.get_container(sandbox_id)
            if not container:
                raise HTTPException(status_code=404, detail="Sandbox container not found")

            # Update last active timestamp
            db.execute(
                "UPDATE sandboxes SET last_active_at = ? WHERE id = ?",
                (time.time(), sandbox_id)
            )
            db.commit()

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
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND deleted_at IS NULL",
            (sandbox_id, )
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        # Update last active timestamp
        db.execute(
            "UPDATE sandboxes SET last_active_at = ? WHERE id = ?",
            (time.time(), sandbox_id)
        )
        db.commit()

    container = await engine.get_container(sandbox_id)
    if not container or not container.ip_address or not container.exposed_port:
        print(f'container ip: {container.ip_address} port: {container.exposed_port}')
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
async def delete_sandbox(sandbox_id: str, req: Request):
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        try:
            container = await engine.get_container(sandbox_id)
            if container:
                await container.stop()
            
            db.execute(
                "UPDATE sandboxes SET deleted_at = ?, delete_reason = 'user_deleted' WHERE id = ?",
                (time.time(), sandbox_id)
            )
            db.commit()
            
            return {"message": f"Sandbox {sandbox_id} deleted"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def serve_ui(request: Request):
    if request.session.get("user_id"):
        return FileResponse("ui.html")
    else:
        return FileResponse("login.html")

@app.websocket("/sandboxes/{sandbox_id}/proxy/{path:path}")
async def proxy_websocket(websocket: WebSocket, sandbox_id: str, path: str):
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND deleted_at IS NULL",
            (sandbox_id, )
        ).fetchone()
        if not sandbox:
            await websocket.close(code=4004, reason="Sandbox not found")
            return

        # Update last active timestamp
        db.execute(
            "UPDATE sandboxes SET last_active_at = ? WHERE id = ?",
            (time.time(), sandbox_id)
        )
        db.commit()

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
