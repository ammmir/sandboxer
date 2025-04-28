from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, PlainTextResponse, FileResponse, RedirectResponse
from fastapi.security import HTTPBearer
from starlette.background import BackgroundTask
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager, closing
from pathlib import Path
import asyncio
import time
import httpx
import uuid
import pickle
import json
import os
import websockets
import secrets
import argparse
import sqlite3
import sys
import logging

from sandboxer_server import sandbox_manager
from sandboxer_server.sandbox_manager import app, get_db, quota

# Default values from environment variables
GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET")
GITHUB_REDIRECT_URI = os.getenv("GITHUB_REDIRECT_URI")
SESSION_SECRET = os.getenv("SESSION_SECRET")
STATIC_DIR = Path(__file__).resolve().parent / "static"
QUOTA_BLOCK_HARD = int(os.getenv("QUOTA_BLOCK_HARD", "1073741824")) # 1GB
QUOTA_INODE_HARD = int(os.getenv("QUOTA_INODE_HARD", "1000000")) # 1M inodes

# Global auth methods configuration
AUTH_METHODS = {"token"}  # Default to token auth

class AuthMiddleware:
    def __init__(self, app, auth_methods=None):
        self.app = app
        self.auth_methods = auth_methods or {"token"}  # Default to token auth if not specified

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ["http", "websocket"]:
            return await self.app(scope, receive, send)

        path = scope.get("path", "")
        protected_prefixes = ["/sandboxes", "/volumes", "/events", "/auth/me"]
        requires_auth = any(path.startswith(prefix) for prefix in protected_prefixes)
        
        if not requires_auth:
            return await self.app(scope, receive, send)

        is_public = False
        user_id = None

        # Try session-based auth
        if "session" in scope:
            session = scope.get("session", {})
            user_id = session.get("user_id")

        # Try token-based auth if enabled and no session found
        if not user_id and "token" in self.auth_methods:
            headers = dict(scope.get("headers", []))
            auth_header = headers.get(b"authorization", b"").decode()
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]  # Remove "Bearer " prefix
                with closing(get_db()) as db:
                    token_record = db.execute(
                        "SELECT user_id FROM static_auth_tokens WHERE token = ?",
                        (token,)
                    ).fetchone()
                    if token_record:
                        user_id = token_record["user_id"]

        # Check if this is a proxy request to a public sandbox
        if path.startswith("/sandboxes/") and "/proxy/" in path:
            parts = path.split("/")
            if len(parts) >= 4:  # /sandboxes/{name}/proxy/...
                sandbox_name = parts[2]
                
                with closing(get_db()) as db:
                    # Get sandbox and its owner
                    sandbox = db.execute("""
                        SELECT s.*, n.user_id as owner_id
                        FROM sandboxes s
                        JOIN networks n ON s.network = n.name
                        WHERE s.name = ? AND s.is_public = TRUE AND s.deleted_at IS NULL
                    """, (sandbox_name,)).fetchone()
                        
                    if sandbox:
                        is_public = True
                        user_id = sandbox['owner_id']

        # sandbox is either private or doesn't exist
        if not is_public and not user_id:
            response = JSONResponse(
                status_code=401,
                content={"detail": "Authentication required"},
                headers={"WWW-Authenticate": "Bearer" if "token" in self.auth_methods else "Session"}
            )
            return await response(scope, receive, send)

        # Get user from database
        with closing(get_db()) as db:
            user = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()

            # Add user and network to request state
            scope["state"] = {
                "user": dict(user),
                "user_id": user_id,
                "network": f"user_{user_id}"
            }
        
        return await self.app(scope, receive, send)

hx = httpx.AsyncClient()

@app.get("/auth/github", include_in_schema=False)
async def auth_github():
    if "github" not in AUTH_METHODS:
        raise HTTPException(status_code=403, detail="GitHub authentication is not enabled")
        
    github_auth_url = (
        "https://github.com/login/oauth/authorize"
        f"?client_id={GITHUB_CLIENT_ID}"
        f"&redirect_uri={GITHUB_REDIRECT_URI}"
        "&scope=read:user user:email"
    )
    return RedirectResponse(github_auth_url)

async def create_network_bg(network_name: str):
    """Background task to create network and project entry, ignoring errors"""
    try:
        # Create network first
        await sandbox_manager.engine.create_network(network_name)
        print(f"Network created: {network_name}")
    except Exception as e:
        print(f"Network/project creation failed (ignoring): {e}")

    # Add project entry if quota is supported
    if quota.is_supported():
        print(f"Adding project entry for network: {network_name}")
        await quota.add_project(network_name, block_hard=QUOTA_BLOCK_HARD, inode_hard=QUOTA_INODE_HARD)
        print(f"Project entry created for network: {network_name}")
    else:
        print(f"Quota is not supported, skipping project entry creation for network: {network_name}")

@app.get("/auth/github/callback", include_in_schema=False)
async def auth_github_callback(code: str, request: Request):
    if "github" not in AUTH_METHODS:
        raise HTTPException(status_code=403, detail="GitHub authentication is not enabled")
        
    # Exchange code for access token
    token_url = "https://github.com/login/oauth/access_token"
    headers = {"Accept": "application/json"}
    data = {
        "client_id": GITHUB_CLIENT_ID,
        "client_secret": GITHUB_CLIENT_SECRET,
        "code": code,
        "redirect_uri": GITHUB_REDIRECT_URI,
    }

    try:
        token_response = await hx.post(token_url, headers=headers, data=data)
        token_response.raise_for_status()
        token_json = token_response.json()
        access_token = token_json.get("access_token")
        if not access_token:
            raise HTTPException(status_code=400, detail="Failed to get access token")

        # Fetch user info
        user_response = await hx.get(
            "https://api.github.com/user",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        user_response.raise_for_status()
        user = user_response.json()

        # Fetch email (we want verified, preferred email)
        email_response = await hx.get(
            "https://api.github.com/user/emails",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        email_response.raise_for_status()
        emails = email_response.json()

        # Pick primary verified email
        email = next(
            (e["email"] for e in emails if e.get("verified") and e.get("primary")),
            emails[0]["email"] if emails else None
        )

        user_id = str(user["id"])
        network_name = f"user_{user_id}"
        
        with closing(get_db()) as db:
            # Insert or update user
            db.execute("""
                INSERT INTO users (id, email, name, login, avatar_url, access_token, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    email=excluded.email,
                    name=excluded.name,
                    login=excluded.login,
                    avatar_url=excluded.avatar_url,
                    access_token=excluded.access_token,
                    updated_at=excluded.updated_at
                """,
                (
                    user_id,
                    email,
                    user.get("name") or user.get("login"),
                    user.get("login"),
                    user.get("avatar_url"),
                    access_token,
                    int(time.time()),
                    int(time.time())
                )
            )
            
            # Create network record
            db.execute(
                "INSERT OR IGNORE INTO networks (name, user_id, created_at) VALUES (?, ?, ?)",
                (network_name, user_id, int(time.time()))
            )
            db.commit()

        request.session["user_id"] = user_id
        return RedirectResponse(url="/", status_code=303, background=BackgroundTask(create_network_bg, network_name))
    except Exception as e:
        print(f"GitHub OAuth error: {e}")
        raise HTTPException(status_code=500, detail="Authentication failed")

@app.post("/auth/logout", include_in_schema=False)
async def logout(request: Request):
    """Clear the session and log out the user."""
    request.session.clear()
    return RedirectResponse(
        url="/",  # Redirect to home page after logout
        status_code=303
    )

@app.get("/auth/me", include_in_schema=False)
async def get_current_user(request: Request):
    """Get the current authenticated user's information."""
    if not request.state.user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
        
    user = request.state.user

    return {
        "id": user["id"],
        "email": user["email"],
        "name": user["name"],
        "avatar_url": user["avatar_url"] or "https://github.com/identicons/github.png"
    }

class TokenLoginRequest(BaseModel):
    token: str

@app.post("/auth/token", include_in_schema=False)
async def auth_token(request: Request, data: TokenLoginRequest):
    """Create a session from a token."""
    with closing(get_db()) as db:
        token_record = db.execute(
            "SELECT user_id FROM static_auth_tokens WHERE token = ?",
            (data.token,)
        ).fetchone()
        
        if not token_record:
            raise HTTPException(status_code=401, detail="Invalid token")
            
        user_id = token_record["user_id"]
        
        # Get user from database
        user = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
            
        # Add user and network to request state
        request.state.user = dict(user)
        request.state.user_id = user_id
        request.state.network = f"user_{user_id}"
        
        # Set user_id in session
        request.session["user_id"] = user_id
        return {"status": "ok"}

@app.get("/", include_in_schema=False)
async def serve_ui(request: Request):
    # Check for session-based auth
    if "session" in request and request.session.get("user_id"):
        return FileResponse(STATIC_DIR / "ui.html")
    
    # No valid auth, show login page
    return FileResponse(STATIC_DIR / "login.html")

def init_db():
    with closing(sqlite3.connect(os.getenv('DATABASE'))) as db:
        # Existing tables
        db.execute("""
            CREATE TABLE IF NOT EXISTS sandboxes (
                id TEXT PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                label TEXT,
                image TEXT,
                network TEXT,
                parent_id TEXT NULL,
                last_active_at INTEGER,
                deleted_at INTEGER NULL,
                delete_reason TEXT NULL,
                is_public BOOLEAN DEFAULT FALSE,
                status TEXT DEFAULT 'unknown',
                FOREIGN KEY(network) REFERENCES networks(name)
            )
        """)
        
        # New tables
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT,
                login TEXT,
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

        # Command executions table
        db.execute("""
            CREATE TABLE IF NOT EXISTS command_executions (
                id TEXT PRIMARY KEY,
                sandbox_id TEXT NOT NULL,
                command TEXT NOT NULL,
                exit_code INTEGER NOT NULL,
                executed_at INTEGER NOT NULL,
                FOREIGN KEY(sandbox_id) REFERENCES sandboxes(id)
            )
        """)

        # Command execution logs table with autoincrementing sequence
        db.execute("""
            CREATE TABLE IF NOT EXISTS command_execution_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                fd INTEGER NOT NULL,  -- 1 for stdout, 2 for stderr
                content TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(execution_id) REFERENCES command_executions(id)
            )
        """)
        
        # Static authentication tokens table
        db.execute("""
            CREATE TABLE IF NOT EXISTS static_auth_tokens (
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                description TEXT,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)
        
        # Add indexes
        db.execute("CREATE INDEX IF NOT EXISTS idx_sandboxes_network ON sandboxes(network)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_networks_user ON networks(user_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_command_executions_sandbox ON command_executions(sandbox_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_command_execution_logs_execution ON command_execution_logs(execution_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_command_execution_logs_created ON command_execution_logs(created_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_static_auth_tokens_user ON static_auth_tokens(user_id)")
        
        db.commit()

def is_static_auth_tokens_empty():
    """Check if the static_auth_tokens table is empty."""
    with closing(get_db()) as db:
        result = db.execute("SELECT COUNT(*) as count FROM static_auth_tokens").fetchone()
        return result["count"] == 0

async def first_run():
    init_db()

    if not is_static_auth_tokens_empty():
        return

    # Generate a random user ID and token
    user_id = str(uuid.uuid4())
    token = secrets.token_urlsafe(32)
    network_name = f"user_{user_id}"
    current_time = int(time.time())

    with closing(get_db()) as db:
        # Create user
        db.execute("""
            INSERT INTO users (id, email, name, login, avatar_url, access_token, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            "admin@localhost",
            "Admin User",
            "admin",
            None,
            None,
            current_time,
            current_time
        ))

        # Create network
        db.execute(
            "INSERT INTO networks (name, user_id, created_at) VALUES (?, ?, ?)",
            (network_name, user_id, current_time)
        )

        # Create token
        db.execute(
            "INSERT INTO static_auth_tokens (token, user_id, description, created_at) VALUES (?, ?, ?, ?)",
            (token, user_id, "Initial admin token", current_time)
        )

        db.commit()

    # Create network in background
    await create_network_bg(network_name)

    print("\nInitial admin token created!")
    print(f"Token: {token}")
    print("Use this token to log in to the system.\n")

if __name__ == "__main__":
    import uvicorn
    
    parser = argparse.ArgumentParser(description="Sandbox Server")
    parser.add_argument("--host", "-H", default="127.0.0.1", help="Host to listen on")
    parser.add_argument("--port", "-p", type=int, default=8000, help="Port to listen on")
    parser.add_argument("--vhost", default=os.getenv("SANDBOX_VHOST"), help="Virtual host domain")
    parser.add_argument("--auth", action="append", choices=["token", "github"], 
                       help="Authentication methods (can be specified multiple times)")
    parser.add_argument("--db", default=os.getenv('DATABASE', 'sandboxer.sqlite'), help="Database file path")
    parser.add_argument("--generate-caddy-config", action="store_true", 
                       help="Generate Caddy configuration")
    
    args = parser.parse_args()
    
    # Handle --generate-caddy-config first
    if args.generate_caddy_config:
        template_path = Path(__file__).resolve().parent / "Caddyfile.prod.in"
        if not template_path.exists():
            print(f"Error: {template_path} not found", file=sys.stderr)
            sys.exit(1)
            
        vhost = args.vhost
        if not vhost:
            print("Error: SANDBOX_VHOST not set", file=sys.stderr)
            sys.exit(1)
            
        with open(template_path) as f:
            content = f.read()
            content = content.replace("SANDBOX_VHOST", vhost)
            content = content.replace("HOST_PORT", f"{args.host}:{args.port}")
            print(content)
        sys.exit(0)
    
    # Update SANDBOX_VHOST if provided via command line
    if args.vhost:
        os.environ["SANDBOX_VHOST"] = args.vhost
    
    # Set DATABASE environment variable if --db is provided
    if args.db:
        os.environ["DATABASE"] = args.db
    
    # Configure authentication methods
    AUTH_METHODS = set(args.auth) if args.auth else {"token"}
    
    # Add auth middleware with configured auth methods
    app.add_middleware(AuthMiddleware, auth_methods=AUTH_METHODS)

    app.add_middleware(
        SessionMiddleware,
        secret_key=SESSION_SECRET,
        session_cookie="SID",
        max_age=24 * 60 * 60,  # 24 hours
        same_site="none",
        https_only=True,
        domain=f'.{os.getenv("SANDBOX_VHOST")}'
    )

    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    
    # Create database if needed
    asyncio.run(first_run(), debug=True)
    
    uvicorn.run(app, host=args.host, port=args.port, timeout_graceful_shutdown=5)
