from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, PlainTextResponse, FileResponse, RedirectResponse
from fastapi.security import HTTPBearer
from starlette.background import BackgroundTask
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager, closing
import asyncio
import time
import httpx
import uuid
import pickle
import json
import os
import websockets
import secrets

import sandbox_manager
from sandbox_manager import app, get_db, init_db

GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET")
GITHUB_REDIRECT_URI = os.getenv("GITHUB_REDIRECT_URI")
SESSION_SECRET = os.getenv("SESSION_SECRET")
SANDBOX_VHOST = os.getenv("SANDBOX_VHOST")

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        protected_prefixes = ["/sandboxes", "/volumes", "/events"]
        path = request.url.path
        requires_auth = any(path.startswith(prefix) for prefix in protected_prefixes)
        
        if not requires_auth:
            return await call_next(request)

        user_id = request.session.get("user_id")
        if not user_id:
            return JSONResponse(
                status_code=401,
                content={"detail": "Authentication required"},
                headers={"WWW-Authenticate": "Session"}
            )

        # Get user from database instead of dict
        with closing(get_db()) as db:
            user = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            if not user:
                request.session.clear()
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Authentication required"},
                    headers={"WWW-Authenticate": "Session"}
                )

            # Add user and default network to request state
            request.state.user = dict(user)
            request.state.user_id = user_id
            request.state.network = f"user_{user_id}"  # Default network per user
        
        return await call_next(request)

# Add session middleware with secure configuration
app.add_middleware(AuthMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="SID",
    max_age=24 * 60 * 60,  # 24 hours
    same_site="none",
    https_only=True,
    domain=f'.{SANDBOX_VHOST}'
)

# Add authentication middleware

hx = httpx.AsyncClient()

@app.get("/auth/github", include_in_schema=False)
async def auth_github():
    github_auth_url = (
        "https://github.com/login/oauth/authorize"
        f"?client_id={GITHUB_CLIENT_ID}"
        f"&redirect_uri={GITHUB_REDIRECT_URI}"
        "&scope=read:user user:email"
    )
    return RedirectResponse(github_auth_url)

async def create_network_bg(network_name: str):
    """Background task to create network, ignoring errors"""
    try:
        await sandbox_manager.engine.create_network(network_name)
        print(f"Network created: {network_name}")
    except Exception as e:
        print(f"Network creation failed (ignoring): {e}")

@app.get("/auth/github/callback", include_in_schema=False)
async def auth_github_callback(code: str, request: Request):
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
    """Get the current logged-in user's information."""
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
        
    with closing(get_db()) as db:
        user = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            request.session.clear()
            raise HTTPException(status_code=401, detail="Not authenticated")
        return dict(user)

@app.get("/", include_in_schema=False)
async def serve_ui(request: Request):
    if request.session.get("user_id"):
        return FileResponse("ui.html")
    else:
        return FileResponse("login.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=443, ssl_keyfile="key.pem", ssl_certfile="cert.pem", timeout_graceful_shutdown=10)
