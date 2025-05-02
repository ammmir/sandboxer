from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, File, UploadFile
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, PlainTextResponse, FileResponse, RedirectResponse, Response
from starlette.background import BackgroundTask
from pydantic import BaseModel
from contextlib import asynccontextmanager, closing
from dotenv import load_dotenv
import asyncio
import time
import httpx
from ulid import ULID
import json
import os
import re
import websockets
import sqlite3
from datetime import datetime
import aiofiles
import logging

from uvicorn.config import LOGGING_CONFIG
from uvicorn.logging import DefaultFormatter

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger("uvicorn.error")

from .container_engine import ContainerEngine, Container, ContainerConfig
from .subordinate_manager import SubordinateManager
from .quota_manager import QuotaManager

load_dotenv()

# configuration 
IDLE_TIMEOUT = 24*60*60
CHECK_INTERVAL = 60
PROXY_TIMEOUT = 30

quota = QuotaManager()
engine = ContainerEngine(quota=quota)
subordinate = SubordinateManager()
hx = httpx.AsyncClient()

# Add SSE client management
class SSEClientManager:
    def __init__(self):
        self._clients = {}  # network -> set of queues
        self._lock = asyncio.Lock()

    async def add_client(self, network: str, queue: asyncio.Queue):
        async with self._lock:
            if network not in self._clients:
                self._clients[network] = set()
            self._clients[network].add(queue)

    async def remove_client(self, network: str, queue: asyncio.Queue):
        async with self._lock:
            if network in self._clients:
                self._clients[network].discard(queue)
                if not self._clients[network]:
                    del self._clients[network]

    async def broadcast(self, network: str, event: dict):
        async with self._lock:
            if network in self._clients:
                for queue in self._clients[network]:
                    try:
                        await queue.put(event)
                    except asyncio.CancelledError:
                        pass

    async def shutdown(self):
        async with self._lock:
            for network in list(self._clients.keys()):
                for queue in self._clients[network]:
                    await queue.put(None)

sse_manager = SSEClientManager()

def get_db():
    db = sqlite3.connect(os.getenv('DATABASE'))
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
                if container:
                    await container.stop()
                    #await engine.remove_container(row['id'])
            except:
                pass

async def sync_sandbox_states():
    """Sync sandbox states with actual running containers on startup."""
    with closing(get_db()) as db:
        # Get all non-deleted sandboxes
        sandboxes = db.execute("SELECT id FROM sandboxes WHERE deleted_at IS NULL").fetchall()
        
        now = time.time()
        for sandbox in sandboxes:
            try:
                container = await engine.get_container(sandbox['id'])
                if not container:
                    # Container doesn't exist, mark it as deleted
                    db.execute(
                        "UPDATE sandboxes SET deleted_at = ?, delete_reason = 'startup_sync' WHERE id = ?",
                        (now, sandbox['id'])
                    )
            except Exception as e:
                print(f"Error checking sandbox {sandbox['id']}: {e}")
                # On error, mark as deleted to be safe
                db.execute(
                    "UPDATE sandboxes SET deleted_at = ?, delete_reason = 'startup_sync_error' WHERE id = ?",
                    (now, sandbox['id'])
                )
        
        db.commit()

async def handle_container_events():
    """Background task to handle container lifecycle events."""
    event_queue = await engine.subscribe()
    try:
        while True:
            event = await event_queue.get()
            if event is None:  # Shutdown signal
                break
                
            try:
                with closing(get_db()) as db:
                    if event["type"] == "container_stopped":
                        # Mark sandbox as terminated
                        now = time.time()
                        db.execute(
                            "UPDATE sandboxes SET status = 'stopped', deleted_at = ?, delete_reason = 'container_stopped' WHERE id = ? AND deleted_at IS NULL",
                            (now, event["container_id"])
                        )
                        db.commit()
                        
                        # Get the network for this sandbox
                        sandbox = db.execute(
                            "SELECT network FROM sandboxes WHERE id = ?",
                            (event["container_id"],)
                        ).fetchone()
                        
                        if sandbox:
                            # Broadcast the event to all clients in this network
                            await sse_manager.broadcast(sandbox["network"], event)
                            
                    elif event["type"] == "container_started":
                        # Update sandbox status to running
                        db.execute(
                            "UPDATE sandboxes SET status = 'running', last_active_at = ? WHERE id = ?",
                            (event["timestamp"], event["container_id"])
                        )
                        db.commit()
                        
                        # Get the network for this sandbox
                        sandbox = db.execute(
                            "SELECT network FROM sandboxes WHERE id = ?",
                            (event["container_id"],)
                        ).fetchone()
                        
                        if sandbox:
                            # Broadcast the event to all clients in this network
                            await sse_manager.broadcast(sandbox["network"], event)
                            
                    elif event["type"] == "sandbox_paused":
                        # Update sandbox status to paused
                        db.execute(
                            "UPDATE sandboxes SET status = 'paused', last_active_at = ? WHERE id = ? AND deleted_at IS NULL",
                            (event["timestamp"], event["container_id"])
                        )
                        db.commit()
                        
                        # Get the network for this sandbox
                        sandbox = db.execute(
                            "SELECT network FROM sandboxes WHERE id = ?",
                            (event["container_id"],)
                        ).fetchone()
                        
                        if sandbox:
                            # Broadcast the event to all clients in this network
                            await sse_manager.broadcast(sandbox["network"], event)
                            
                    elif event["type"] == "sandbox_unpaused":
                        # Update sandbox status to running
                        db.execute(
                            "UPDATE sandboxes SET status = 'running', last_active_at = ? WHERE id = ? AND deleted_at IS NULL",
                            (event["timestamp"], event["container_id"])
                        )
                        db.commit()
                        
                        # Get the network for this sandbox
                        sandbox = db.execute(
                            "SELECT network FROM sandboxes WHERE id = ?",
                            (event["container_id"],)
                        ).fetchone()
                        
                        if sandbox:
                            # Broadcast the event to all clients in this network
                            await sse_manager.broadcast(sandbox["network"], event)
                            
            except Exception as e:
                print(f"Error handling container event: {e}")
    except asyncio.CancelledError:
        pass
    finally:
        await engine.unsubscribe(event_queue)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await sync_sandbox_states()  # Sync sandbox states before starting idle checker
    #asyncio.create_task(terminate_idle_sandboxes())
    asyncio.create_task(handle_container_events())  # Start event handler
    await engine.start()
    yield
    await sse_manager.shutdown()
    await engine.close()

app = FastAPI(lifespan=lifespan)

class CreateSandboxRequest(BaseModel):
    image: str
    label: str
    env: dict[str, str] = {}
    args: list[str] = []
    interactive: bool

class ForkSandboxRequest(BaseModel):
    label: str
    hostname: str | None = None

class ExecuteRequest(BaseModel):
    code: str

class UpdateSandboxRequest(BaseModel):
    label: str | None = None
    public: bool | None = None

@app.get("/sandboxes")
async def get_sandboxes(request: Request):
    sandboxes = []
    with closing(get_db()) as db:
        rows = db.execute("SELECT * FROM sandboxes WHERE network = ?", (request.state.network,)).fetchall()
        for row in rows:
            if not row['deleted_at']:
                container = await engine.get_container(row['id'])
                # Construct the URL for the sandbox
                sandbox_url = f"https://{row['name']}.{os.getenv('SANDBOX_VHOST')}"
                
                sandboxes.append({
                    "id": row['id'],
                    "name": container.name,
                    "status": "terminated" if row['deleted_at'] else container.status,
                    "ip_address": container.ip_address,
                    "hostname": container.hostname,
                    "label": row['label'],
                    "parent_id": row['parent_id'],
                    "child_ids": db.execute("SELECT id FROM sandboxes WHERE parent_id = ?", (row['id'],)).fetchall(),
                    "deleted_at": row['deleted_at'],
                    "delete_reason": row['delete_reason'],
                    "url": sandbox_url
                })
            else:
                # If container doesn't exist but is in DB, mark it as DEAD
                sandboxes.append({
                    "id": row['id'],
                    "name": row['name'],
                    "status": "terminated",
                    "ip_address": None,
                    "hostname": None,
                    "label": row['label'],
                    "parent_id": row['parent_id'],
                    "child_ids": db.execute("SELECT id FROM sandboxes WHERE parent_id = ?", (row['id'],)).fetchall(),
                    "deleted_at": row['deleted_at'],
                    "delete_reason": row['delete_reason'],
                    "url": None
                })
            
    return {"sandboxes": sandboxes}

@app.post("/sandboxes")
async def create_sandbox(request: CreateSandboxRequest, req: Request):
    container_name = str(ULID()).lower()
    label = request.label if request.label else container_name
    
    try:
        with closing(get_db()) as db:
            network_row_id = db.execute("SELECT rowid FROM networks WHERE name = ?", (req.state.network,)).fetchone()['rowid']

        container = await engine.start_container(
            image=request.image, 
            name=container_name, 
            env=request.env, 
            args=request.args,
            interactive=request.interactive,
            network=req.state.network,
            subuid=subordinate.get_subuid(network_row_id),
            quota_projname=req.state.network,
            config=ContainerConfig(cpus="1.0", memory="2g")
        )
        
        with closing(get_db()) as db:
            db.execute(
                "INSERT INTO sandboxes (id, name, label, image, network, parent_id, last_active_at, status) VALUES (?, ?, ?, ?, ?, NULL, ?, 'running')",
                (container.id, container_name, label, request.image, req.state.network, time.time())
            )
            db.commit()
            
        return {"id": container.id, "name": container.name, "label": label, "hostname": container.hostname}
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

        if not request.hostname:
            container = await engine.get_container(sandbox_id)
            hostname = container.hostname
            clean_label = re.sub(r'[^a-zA-Z0-9\s-]', '-', request.label.strip())
            clean_label = re.sub(r'[\s-]+', '-', clean_label)
            hostname = f"{hostname}-{clean_label}".rstrip('-')

        child_label = request.label if request.label else f"Fork of {parent['label']}"
        try:
            forked_container, _ = await engine.fork_container(
                sandbox_id, 
                str(ULID()).lower(),
                hostname=hostname
            )
            
            db.execute(
                "INSERT INTO sandboxes (id, name, label, image, network, parent_id, last_active_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (forked_container.id, forked_container.name, child_label, parent['image'], req.state.network, sandbox_id, time.time())
            )
            db.commit()

            # Emit fork event
            await sse_manager.broadcast(req.state.network, {
                "type": "sandbox_forked",
                "container_id": forked_container.id,
                "timestamp": time.time(),
                "data": {
                    "parent_id": sandbox_id,
                    "label": child_label
                }
            })

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

        # Emit coalesce event
        await sse_manager.broadcast(req.state.network, {
            "type": "sandbox_coalesced",
            "container_id": final_sandbox_id,
            "timestamp": time.time(),
            "data": {
                "terminated_sandboxes": sandboxes_to_terminate,
                "final_sandbox": final_sandbox_id
            }
        })

        return {"message": f"Sandbox {final_sandbox_id} is now the new root"}

@app.get("/sandboxes/{sandbox_id}")
async def get_sandbox(sandbox_id: str, req: Request):
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ?", 
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        try:
            container = await engine.get_container(sandbox_id)
            child_ids = [row['id'] for row in db.execute("SELECT id FROM sandboxes WHERE parent_id = ?", (sandbox_id,)).fetchall()]
            
            if not container:
                # If container doesn't exist but is in DB, mark it as DEAD
                return {
                    "id": sandbox['id'],
                    "name": sandbox['name'],
                    "status": "terminated",
                    "ip_address": None,
                    "hostname": None,
                    "label": sandbox['label'],
                    "parent_id": sandbox['parent_id'],
                    "child_ids": db.execute("SELECT id FROM sandboxes WHERE parent_id = ?", (sandbox['id'],)).fetchall(),
                    "deleted_at": sandbox['deleted_at'],
                    "delete_reason": sandbox['delete_reason'],
                    "url": None
                }
            
            # Construct the URL for the sandbox
            sandbox_url = f"https://{container.name}.{os.getenv('SANDBOX_VHOST')}"
            
            return {
                "id": container.id,
                "name": container.name,
                "status": container.status,
                "ip_address": container.ip_address,
                "hostname": container.hostname,
                "label": sandbox['label'],
                "parent_id": sandbox['parent_id'],
                "child_ids": child_ids,
                "url": sandbox_url,
                "is_public": bool(sandbox['is_public']),
                "interactive": container.interactive
            }
        except Exception as e:
            print(e)
            raise HTTPException(status_code=404, detail="Sandbox not found")

@app.get("/sandboxes/{sandbox_id}/logs")
async def get_sandbox_logs(req: Request, sandbox_id: str, stream: bool = False):
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ?",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        container = await engine.get_container(sandbox_id)
        if not container:
            return PlainTextResponse("<container terminated>") # TODO retain logs

        if container.interactive:
            raise HTTPException(status_code=400, detail="Cannot stream logs from interactive sandboxes")

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

@app.get("/sandboxes/{sandbox_id}/execution-logs")
async def get_all_sandbox_logs(sandbox_id: str, req: Request, limit: int = 1000):
    """Get all logs for a sandbox, including command execution logs."""
    with closing(get_db()) as db:
        # Verify sandbox exists and belongs to user's network
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ?",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        # Get all command executions and their logs
        logs = db.execute("""
            SELECT 
                ce.id as execution_id,
                ce.command,
                ce.executed_at,
                ce.exit_code,
                cel.id as log_id,
                cel.fd,
                cel.content,
                cel.created_at
            FROM command_executions ce
            LEFT JOIN command_execution_logs cel ON ce.id = cel.execution_id
            WHERE ce.sandbox_id = ?
            ORDER BY ce.executed_at ASC, cel.id ASC
            LIMIT ?
        """, (sandbox_id, limit)).fetchall()

        # Group logs by execution
        executions = {}
        for log in logs:
            if log['execution_id'] not in executions:
                executions[log['execution_id']] = {
                    "id": log['execution_id'],
                    "command": log['command'],
                    "executed_at": log['executed_at'],
                    "exit_code": log['exit_code'],
                    "logs": []
                }
            if log['log_id']:  # Only add if there are actual logs
                executions[log['execution_id']]["logs"].append({
                    "id": log['log_id'],
                    "fd": log['fd'],  # 1=stdout, 2=stderr
                    "content": log['content'],
                    "created_at": log['created_at']
                })

        return {
            "executions": list(executions.values())
        }

@app.patch("/sandboxes/{sandbox_id}")
async def update_sandbox(sandbox_id: str, request: UpdateSandboxRequest, req: Request):
    with closing(get_db()) as db:
        # First check if sandbox exists and belongs to user's network
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        if request.label is not None and not request.label.strip():
            raise HTTPException(status_code=400, detail="Label cannot be empty.")
            
        # Always update with all fields, using existing values if not provided
        result = db.execute("""
            UPDATE sandboxes 
            SET label = COALESCE(?, label),
                is_public = COALESCE(?, is_public)
            WHERE id = ? AND network = ? AND deleted_at IS NULL
        """, (
            request.label if request.label is not None else None,
            request.public if request.public is not None else None,
            sandbox_id,
            req.state.network
        ))
        db.commit()
        
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Sandbox not found")
            
        # Emit update event
        event_data = {}
        if request.label is not None:
            event_data["label"] = request.label
        if request.public is not None:
            event_data["public"] = request.public
            
        await sse_manager.broadcast(req.state.network, {
            "type": "sandbox_updated",
            "container_id": sandbox_id,
            "timestamp": time.time(),
            "data": event_data
        })
            
        return {"id": sandbox_id, **event_data}

@app.get("/sandboxes/{sandbox_id}/tree")
async def get_sandbox_tree(sandbox_id: str, req: Request):
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ?",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        def build_tree(node_id, prefix="", is_root=False, is_last=True):
            node = db.execute("SELECT * FROM sandboxes WHERE id = ?", (node_id,)).fetchone()
            is_deleted = node['deleted_at'] is not None
            status_prefix = "💀 DEAD " if is_deleted else ""
            node_label = f"{status_prefix}📦 {node['label']} [{node_id}]"

            if is_root:
                tree_repr.append(node_label)
            else:
                connector = "└── " if is_last else "├── "
                tree_repr.append(f"{prefix}{connector}{node_label}")

            children = db.execute("SELECT id, label FROM sandboxes WHERE parent_id = ?", (node_id,)).fetchall()
            tree_json = {
                "id": node_id, 
                "label": node['label'],
                "status": "terminated" if is_deleted else "running",
                "children": []
            }

            new_prefix = prefix + ("    " if is_last else "│   ")
            for i, child in enumerate(children):
                tree_json["children"].append(build_tree(child['id'], new_prefix, False, i == len(children) - 1))

            return tree_json

        tree_repr = []
        tree_json = build_tree(sandbox_id, "", True, True)

        data =  {
            "tree": "\n".join(tree_repr),
            "tree_json": tree_json
        }
        return JSONResponse(content=data, media_type="application/json; charset=utf-8")

@app.post("/sandboxes/{sandbox_id}/execute")
async def execute_code(req: Request, sandbox_id: str, request: ExecuteRequest, stream: bool = False):
    if not request.code.strip():
        raise HTTPException(status_code=400, detail="Code cannot be empty.")
        
    db = get_db()  # Get a database connection that we'll manage ourselves
    try:
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
                # Create command execution record
                execution_id = str(ULID()).lower()
                db.execute("""
                    INSERT INTO command_executions (id, sandbox_id, command, exit_code, executed_at)
                    VALUES (?, ?, ?, 0, ?)
                """, (execution_id, sandbox_id, request.code, time.time()))
                db.commit()

                # Buffer for accumulating lines
                buffer = []
                last_flush = time.time()
                FLUSH_INTERVAL = 5  # seconds
                MAX_BUFFER_SIZE = 50  # lines

                async def stream_response():
                    nonlocal buffer, last_flush
                    
                    async def flush_buffer():
                        nonlocal buffer, last_flush
                        if not buffer:
                            return
                            
                        # Group lines by fd
                        fd_lines = {1: [], 2: []}  # 1=stdout, 2=stderr
                        for line in buffer:
                            if line.startswith("stdout:"):
                                fd_lines[1].append(line[7:])  # Remove "stdout:" prefix
                            elif line.startswith("stderr:"):
                                fd_lines[2].append(line[7:])  # Remove "stderr:" prefix
                        
                        # Insert chunks for each fd
                        for fd, lines in fd_lines.items():
                            if lines:
                                content = "\n".join(lines)
                                db.execute("""
                                    INSERT INTO command_execution_logs (execution_id, fd, content, created_at)
                                    VALUES (?, ?, ?, ?)
                                """, (execution_id, fd, content, time.time()))
                        
                        db.commit()
                        buffer = []
                        last_flush = time.time()

                    try:
                        async for stream_type, data, exit_code in await container.exec(request.code, stream=True):
                            # Add to buffer
                            buffer.append(f"{stream_type}:{data}")
                            
                            # Check if we should flush
                            if len(buffer) >= MAX_BUFFER_SIZE or time.time() - last_flush >= FLUSH_INTERVAL:
                                await flush_buffer()
                            
                            # Yield the current line
                            yield json.dumps({"type": stream_type, "output": data, "exit_code": exit_code}) + "\n"
                        
                        # Flush any remaining buffer
                        if buffer:
                            await flush_buffer()
                            
                        # Update exit code
                        db.execute("""
                            UPDATE command_executions 
                            SET exit_code = ? 
                            WHERE id = ?
                        """, (exit_code, execution_id))
                        db.commit()
                            
                    except Exception as e:
                        # Ensure we flush buffer even on error
                        if buffer:
                            await flush_buffer()
                        raise e
                    finally:
                        # Close the database connection when we're done streaming
                        db.close()

                return StreamingResponse(stream_response(), media_type="application/x-ndjson")
            else:
                stdout, stderr, exit_code = await container.exec(request.code, stream=False)
                
                # Create command execution record
                execution_id = str(ULID()).lower()
                db.execute("""
                    INSERT INTO command_executions (id, sandbox_id, command, exit_code, executed_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (execution_id, sandbox_id, request.code, exit_code, time.time()))
                
                # Insert stdout and stderr as separate chunks
                if stdout:
                    db.execute("""
                        INSERT INTO command_execution_logs (execution_id, fd, content, created_at)
                        VALUES (?, 1, ?, ?)
                    """, (execution_id, stdout, time.time()))
                
                if stderr:
                    db.execute("""
                        INSERT INTO command_execution_logs (execution_id, fd, content, created_at)
                        VALUES (?, 2, ?, ?)
                    """, (execution_id, stderr, time.time()))
                
                db.commit()
                
                return {
                    "stdout": stdout,
                    "stderr": stderr,
                    "exit_code": exit_code
                }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if not stream:  # Only close if we're not streaming
                db.close()
    except Exception as e:
        if 'db' in locals():
            db.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/sandboxes/{sandbox_id_or_name}/proxy/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy_request(sandbox_id_or_name: str, path: str, request: Request):
    with closing(get_db()) as db:
        # Determine if we should look up by ID or name based on the length
        # Container IDs are typically 64 characters long
        if len(sandbox_id_or_name) == 64:
            # Look up by ID
            sandbox = db.execute(
                "SELECT * FROM sandboxes WHERE id = ? AND deleted_at IS NULL",
                (sandbox_id_or_name, )
            ).fetchone()
        else:
            # Look up by name
            sandbox = db.execute(
                "SELECT * FROM sandboxes WHERE name = ? AND deleted_at IS NULL",
                (sandbox_id_or_name, )
            ).fetchone()
            
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        # Check if sandbox is public or belongs to user's network
        if not sandbox['is_public'] and sandbox['network'] != request.state.network:
            raise HTTPException(status_code=403, detail="Access denied")

        # Update last active timestamp
        db.execute(
            "UPDATE sandboxes SET last_active_at = ? WHERE id = ?",
            (time.time(), sandbox['id'])
        )
        db.commit()

    container = await engine.get_container(sandbox['id'])
    if not container or not container.ip_address or not container.exposed_port:
        print(f'container ip: {container.ip_address} port: {container.exposed_port}')
        raise HTTPException(status_code=500, detail="Sandbox container is missing an exposed port")

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
            {"error": "Sandbox connection was unexpectedly closed, is it an HTTP server?"},
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
            
            # Mark the sandbox as deleted instead of removing it
            db.execute(
                "UPDATE sandboxes SET deleted_at = ?, delete_reason = 'user_deleted' WHERE id = ?",
                (time.time(), sandbox_id)
            )
            db.commit()
            
            return {"message": f"Sandbox {sandbox_id} marked as deleted"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/sandboxes/{sandbox_id_or_name}/proxy/{path:path}")
async def proxy_websocket(websocket: WebSocket, sandbox_id_or_name: str, path: str):
    with closing(get_db()) as db:
        # Determine if we should look up by ID or name based on the length
        # Container IDs are typically 64 characters long
        if len(sandbox_id_or_name) == 64:
            # Look up by ID
            sandbox = db.execute(
                "SELECT * FROM sandboxes WHERE id = ? AND deleted_at IS NULL",
                (sandbox_id_or_name, )
            ).fetchone()
        else:
            # Look up by name
            sandbox = db.execute(
                "SELECT * FROM sandboxes WHERE name = ? AND deleted_at IS NULL",
                (sandbox_id_or_name, )
            ).fetchone()
            
        if not sandbox:
            await websocket.close(code=4004, reason="Sandbox not found")
            return

        # Check if sandbox is public or belongs to user's network
        if not sandbox['is_public'] and sandbox['network'] != websocket.state.network:
            await websocket.close(code=4003, reason="Access denied")
            return

        # Update last active timestamp
        db.execute(
            "UPDATE sandboxes SET last_active_at = ? WHERE id = ?",
            (time.time(), sandbox['id'])
        )
        db.commit()

    container = await engine.get_container(sandbox['id'])
    if not container or not container.ip_address or not container.exposed_port:
        await websocket.close(code=4005, reason="Sandbox container is missing an exposed port")
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

@app.get("/sandboxes/{sandbox_id}/command-executions/{execution_id}/logs")
async def get_command_execution_logs(sandbox_id: str, execution_id: str, req: Request, start_id: int = 0, limit: int = 100):
    """Get logs for a specific command execution, starting from a specific log ID."""
    with closing(get_db()) as db:
        # Verify sandbox exists and belongs to user's network
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ?",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        # Verify command execution exists and belongs to this sandbox
        execution = db.execute("""
            SELECT * FROM command_executions 
            WHERE id = ? AND sandbox_id = ?
        """, (execution_id, sandbox_id)).fetchone()
        if not execution:
            raise HTTPException(status_code=404, detail="Command execution not found")

        # Get log chunks
        logs = db.execute("""
            SELECT id, fd, content, created_at
            FROM command_execution_logs
            WHERE execution_id = ? AND id > ?
            ORDER BY id ASC
            LIMIT ?
        """, (execution_id, start_id, limit)).fetchall()

        return {
            "execution": {
                "id": execution["id"],
                "command": execution["command"],
                "exit_code": execution["exit_code"],
                "executed_at": execution["executed_at"]
            },
            "logs": [
                {
                    "id": log["id"],
                    "fd": log["fd"],  # 1=stdout, 2=stderr
                    "content": log["content"],
                    "created_at": log["created_at"]
                }
                for log in logs
            ]
        }

@app.get("/sandboxes/{sandbox_id}/command-executions")
async def get_sandbox_command_executions(sandbox_id: str, req: Request, limit: int = 100):
    """Get all command executions for a sandbox."""
    with closing(get_db()) as db:
        # Verify sandbox exists and belongs to user's network
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ?",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        # Get command executions with their logs
        executions = db.execute("""
            SELECT 
                ce.id,
                ce.command,
                ce.exit_code,
                ce.executed_at,
                GROUP_CONCAT(
                    json_object(
                        'id', cel.id,
                        'fd', cel.fd,
                        'content', cel.content,
                        'created_at', cel.created_at
                    )
                ) as logs
            FROM command_executions ce
            LEFT JOIN command_execution_logs cel ON ce.id = cel.execution_id
            WHERE ce.sandbox_id = ?
            GROUP BY ce.id
            ORDER BY ce.executed_at DESC
            LIMIT ?
        """, (sandbox_id, limit)).fetchall()

        # Process the results
        result = []
        for execution in executions:
            logs = []
            if execution['logs']:
                # Parse the concatenated JSON strings
                for log_json in execution['logs'].split(','):
                    log = json.loads(log_json)
                    logs.append({
                        "id": log["id"],
                        "fd": log["fd"],
                        "content": log["content"],
                        "created_at": log["created_at"]
                    })

            result.append({
                "id": execution["id"],
                "command": execution["command"],
                "exit_code": execution["exit_code"],
                "executed_at": execution["executed_at"],
                "logs": logs
            })

        return {"executions": result}

@app.get("/events")
async def events(request: Request):
    """SSE endpoint for container events."""
    async def event_stream():
        queue = asyncio.Queue()
        await sse_manager.add_client(request.state.network, queue)
        try:
            while True:
                event = await queue.get()
                if event is None:  # Shutdown signal
                    break
                yield f"data: {json.dumps(event)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            await sse_manager.remove_client(request.state.network, queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@app.websocket("/sandboxes/{sandbox_id}/attach")
async def attach_sandbox(websocket: WebSocket, sandbox_id: str):
    """WebSocket endpoint for attaching to a sandbox container's terminal."""
    with closing(get_db()) as db:
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND deleted_at IS NULL",
            (sandbox_id,)
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

    try:
        container = await engine.get_container(sandbox_id)
        if not container:
            await websocket.close(code=4004, reason="Container not found")
            return

        # Accept the WebSocket connection
        await websocket.accept()

        # Attach to the container
        cancel, send_data, receive_data = await engine.attach_container(sandbox_id)

        async def forward_output():
            """Forward container output to WebSocket."""
            try:
                async for chunk in receive_data():
                    if chunk:  # Only send non-empty chunks
                        await websocket.send_bytes(chunk)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"Error forwarding output: {e}")
            finally:
                await cancel()

        async def forward_input():
            """Forward WebSocket input to container."""
            try:
                while True:
                    try:
                        # Receive data from WebSocket
                        data = await websocket.receive_bytes()
                        
                        # Handle escape sequences
                        if data.startswith(b'\x1b'):
                            # For escape sequences, we'll send them as-is
                            await send_data(data)
                        else:
                            # For regular input, send it directly
                            await send_data(data)
                    except WebSocketDisconnect:
                        break
                    except Exception as e:
                        print(f"Error forwarding input: {e}")
                        break
            finally:
                await cancel()

        # Start both forwarding tasks
        output_task = asyncio.create_task(forward_output())
        input_task = asyncio.create_task(forward_input())

        try:
            # Wait for either task to complete
            done, pending = await asyncio.wait(
                [output_task, input_task],
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

    except Exception as e:
        print(f"WebSocket attach error: {e}")
        try:
            await websocket.close(code=4007, reason="Internal server error")
        except Exception:
            pass  # Connection might already be closed

@app.get("/sandboxes/{sandbox_id}/files/{path:path}")
async def get_files(sandbox_id: str, path: str, req: Request):
    """Get file content or list directory contents."""
    with closing(get_db()) as db:
        # Verify sandbox exists and belongs to user's network
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        try:
            # Update last active timestamp
            db.execute(
                "UPDATE sandboxes SET last_active_at = ? WHERE id = ?",
                (time.time(), sandbox_id)
            )
            db.commit()

            # Get file/directory info
            info = await engine.stat(sandbox_id, path)
            
            if info["is_dir"]:
                # List directory contents
                return await engine.list_files(sandbox_id, path)
            else:
                # Stream file content
                content_type, filename, file_path = await engine.get_file(sandbox_id, path)
                
                # Create a streaming response with async file reading
                async def file_stream():
                    async with aiofiles.open(file_path, 'rb') as f:
                        while chunk := await f.read(8192):  # Read in 8KB chunks
                            yield chunk
                
                return StreamingResponse(
                    file_stream(),
                    media_type=content_type,
                    headers={
                        "Content-Disposition": f'attachment; filename="{filename}"'
                    }
                )
                
        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/sandboxes/{sandbox_id}/files/{path:path}")
async def upload_files(
    sandbox_id: str, 
    path: str, 
    req: Request,
    files: list[UploadFile] = File(...)
):
    """Upload files to a directory in the sandbox."""
    with closing(get_db()) as db:
        # Verify sandbox exists and belongs to user's network
        sandbox = db.execute(
            "SELECT * FROM sandboxes WHERE id = ? AND network = ? AND deleted_at IS NULL",
            (sandbox_id, req.state.network)
        ).fetchone()
        if not sandbox:
            raise HTTPException(status_code=404, detail="Sandbox not found")

        try:
            # Update last active timestamp
            db.execute(
                "UPDATE sandboxes SET last_active_at = ? WHERE id = ?",
                (time.time(), sandbox_id)
            )
            db.commit()

            # Verify the target path is a directory
            info = await engine.stat(sandbox_id, path)
            if not info["is_dir"]:
                raise HTTPException(status_code=400, detail="Target path must be a directory")

            # Upload each file
            for file in files:
                if not file.filename:
                    continue

                # Combine the target directory path with the relative path
                full_path = os.path.join(path, file.filename).replace('\\', '/')
                
                # Create any necessary parent directories
                parent_dir = os.path.dirname(full_path)
                if parent_dir:
                    await engine.mkdir(sandbox_id, parent_dir, parents=True)

                # Stream the file directly to disk
                await engine.put_file(sandbox_id, full_path, file)

            return {"message": "Files uploaded successfully"}

        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.put("/sandboxes/{sandbox_id}/pause")
async def pause_sandbox(sandbox_id: str, req: Request):
    with closing(get_db()) as db:
        # Check if sandbox exists and belongs to user's network
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
            
            await container.pause()
            
            # Emit pause event
            await sse_manager.broadcast(req.state.network, {
                "type": "sandbox_paused",
                "container_id": sandbox_id,
                "timestamp": time.time()
            })
            
            return {"id": sandbox_id, "status": "paused"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.delete("/sandboxes/{sandbox_id}/pause")
async def unpause_sandbox(sandbox_id: str, req: Request):
    with closing(get_db()) as db:
        # Check if sandbox exists and belongs to user's network
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
            
            await container.unpause()
            
            # Emit unpause event
            await sse_manager.broadcast(req.state.network, {
                "type": "sandbox_unpaused",
                "container_id": sandbox_id,
                "timestamp": time.time()
            })
            
            return {"id": sandbox_id, "status": "running"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
