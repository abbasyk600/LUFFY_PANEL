#!/usr/bin/env python3
"""
🌊 Luffy Panel - VLESS Proxy Manager for Hugging Face Spaces
Enhanced version with SQLite persistence, rate limiting, auto-cleanup,
daily traffic history, QR codes, theme detection, export, and more.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import struct
import subprocess
import time
import uuid as uuid_mod
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import aiosqlite
import httpx
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, PlainTextResponse, Response, HTMLResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# ═══════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════

PORT = int(os.getenv("PORT", "7860"))
DATA_DIR = "/data/hf"
DB_PATH = os.path.join(DATA_DIR, "luffy.db")
ADMIN_USERNAME = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASS", secrets.token_hex(8))
MAX_DAILY_TRAFFIC_HISTORY = 30  # days
CLEANUP_INTERVAL = 300  # seconds (5 min)
RATE_LIMIT = os.getenv("RATE_LIMIT", "100/minute")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# ═══════════════════════════════════════════════
# Rate Limiter
# ═══════════════════════════════════════════════

limiter = Limiter(key_func=get_remote_address, default_limits=[RATE_LIMIT])

# ═══════════════════════════════════════════════
# Database Setup
# ═══════════════════════════════════════════════

async def get_db() -> aiosqlite.Connection:
    """Get a database connection (creates DB if not exists)."""
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def init_db():
    """Initialize database tables."""
    db = await get_db()
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS inbounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inbound_id TEXT UNIQUE NOT NULL,
                tag TEXT NOT NULL DEFAULT '',
                port INTEGER NOT NULL,
                protocol TEXT NOT NULL DEFAULT 'vless',
                listen TEXT NOT NULL DEFAULT '0.0.0.0',
                uuid TEXT NOT NULL,
                ws_path TEXT NOT NULL DEFAULT '/',
                ws_host TEXT DEFAULT '',
                upstream_host TEXT NOT NULL DEFAULT '',
                upstream_port INTEGER NOT NULL DEFAULT 443,
                upstream_path TEXT NOT NULL DEFAULT '/',
                upstream_tls TEXT NOT NULL DEFAULT 'tls',
                sni TEXT DEFAULT '',
                alpn TEXT DEFAULT '',
                fingerprint TEXT DEFAULT 'chrome',
                    public_key TEXT DEFAULT '',
                short_id TEXT DEFAULT '',
                spider_x TEXT DEFAULT '',
                flow TEXT DEFAULT '',
                total_upload INTEGER NOT NULL DEFAULT 0,
                total_download INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP DEFAULT NULL,
                last_used_at TIMESTAMP DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS traffic_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inbound_id TEXT NOT NULL,
                date TEXT NOT NULL,
                upload INTEGER NOT NULL DEFAULT 0,
                download INTEGER NOT NULL DEFAULT 0,
                UNIQUE(inbound_id, date)
            );

            CREATE TABLE IF NOT EXISTS connection_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inbound_id TEXT NOT NULL,
                client_ip TEXT DEFAULT '',
                connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                disconnected_at TIMESTAMP DEFAULT NULL,
                upload_bytes INTEGER DEFAULT 0,
                download_bytes INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_traffic_history_date
                ON traffic_history(date);
            CREATE INDEX IF NOT EXISTS idx_connection_logs_inbound
                ON connection_logs(inbound_id);
            CREATE INDEX IF NOT EXISTS idx_inbounds_expires
                ON inbounds(expires_at);
        """)
        await db.commit()

        # Set default settings if not present
        defaults = {
            "panel_title": "🌊 Luffy Panel",
            "panel_lang": "en",
            "total_global_upload": "0",
            "total_global_download": "0",
        }
        for k, v in defaults.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v)
            )
        await db.commit()
    finally:
        await db.close()


# ═══════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════

def generate_uuid() -> str:
    """Generate a random UUID for VLESS."""
    return str(uuid_mod.uuid4())


def generate_short_id() -> str:
    """Generate a random short ID (hex)."""
    return secrets.token_hex(4)


def generate_inbound_id() -> str:
    """Generate a unique inbound ID."""
    return secrets.token_hex(6)


# ═══════════════════════════════════════════════
# Xray-core Integration
# ═══════════════════════════════════════════════

XRAY_CONFIG_PATH = "/etc/xray/config.json"
XRAY_LOCAL_PORT = 10000
XRAY_ENABLED = os.path.exists("/usr/local/bin/xray")


async def start_xray():
    """Start Xray-core as a background process."""
    if not XRAY_ENABLED:
        return None
    try:
        os.makedirs("/data/hf/xray", exist_ok=True)
        proc = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-c", XRAY_CONFIG_PATH],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        await asyncio.sleep(2)
        if proc.poll() is None:
            logging.info(f"[Xray] Started (PID: {proc.pid}) on port {XRAY_LOCAL_PORT}")
            return proc
        else:
            logging.error(f"[Xray] Failed to start (exit code: {proc.returncode})")
            return None
    except Exception as e:
        logging.error(f"[Xray] Start error: {e}")
        return None


async def update_xray_config():
    """Regenerate Xray config with all active inbound UUIDs."""
    if not XRAY_ENABLED:
        return

    try:
        db = await get_db()
        try:
            cursor = await db.execute(
                "SELECT uuid, tag FROM inbounds WHERE enabled=1"
            )
            rows = await cursor.fetchall()
        finally:
            await db.close()

        clients = []
        for row in rows:
            clients.append({
                "id": row["uuid"],
                "email": row["tag"],
                "level": 0
            })

        # Always include a fallback client
        if not clients:
            clients.append({"id": str(uuid_mod.uuid4()), "email": "default", "level": 0})

        config = {
            "log": {
                "loglevel": "warning",
                "access": "/data/hf/xray/access.log",
                "error": "/data/hf/xray/error.log"
            },
            "inbounds": [{
                "port": XRAY_LOCAL_PORT,
                "listen": "127.0.0.1",
                "protocol": "vless",
                "settings": {
                    "clients": clients,
                    "decryption": "none"
                },
                "streamSettings": {
                    "network": "tcp"
                }
            }],
            "outbounds": [
                {
                    "protocol": "freedom",
                    "settings": {},
                    "tag": "direct"
                },
                {
                    "protocol": "blackhole",
                    "settings": {},
                    "tag": "block"
                }
            ],
            "routing": {
                "domainStrategy": "AsIs",
                "rules": []
            }
        }

        os.makedirs(os.path.dirname(XRAY_CONFIG_PATH), exist_ok=True)
        with open(XRAY_CONFIG_PATH, "w") as f:
            json.dump(config, f, indent=2)

        logging.info(f"[Xray] Config updated with {len(clients)} client(s)")
    except Exception as e:
        logging.error(f"[Xray] Failed to update config: {e}")


def build_vless_url(
    uuid: str,
    host: str,
    port: int,
    path: str,
    tls: str = "tls",
    sni: str = "",
    alpn: str = "",
    fp: str = "chrome",
    flow: str = "",
    pbk: str = "",
    sid: str = "",
    spx: str = "",
    tag: str = "",
    inbound_id: str = "",
) -> str:
    """Build a VLESS connection URL."""
    base = f"vless://{uuid}@{host}:{port}"
    params = []
    params.append(f"type=ws")
    params.append(f"security={tls}")
    params.append(f"path={path}")
    if sni:
        params.append(f"sni={sni}")
    if alpn:
        params.append(f"alpn={alpn}")
    if fp:
        params.append(f"fp={fp}")
    if flow:
        params.append(f"flow={flow}")
    if pbk:
        params.append(f"pbk={pbk}")
    if sid:
        params.append(f"sid={sid}")
    if spx:
        params.append(f"spx={spx}")
    if host:
        params.append(f"host={host}")

    param_str = "&".join(params)
    fragment = tag if tag else f"Luffy-{inbound_id}"
    fragment = fragment.replace(" ", "%20")
    return f"{base}?{param_str}#{fragment}"


async def get_setting(db: aiosqlite.Connection, key: str, default: str = "") -> str:
    """Get a setting value."""
    cursor = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = await cursor.fetchone()
    return row["value"] if row else default


async def set_setting(db: aiosqlite.Connection, key: str, value: str):
    """Set a setting value."""
    await db.execute(
        "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", (key, value)
    )
    await db.commit()


async def record_traffic(
    db: aiosqlite.Connection, inbound_id: str, upload: int, download: int
):
    """Record traffic for an inbound (increment total and daily history)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Update inbound totals
    await db.execute(
        """UPDATE inbounds
           SET total_upload = total_upload + ?,
               total_download = total_download + ?,
               last_used_at = CURRENT_TIMESTAMP
           WHERE inbound_id = ?""",
        (upload, download, inbound_id),
    )

    # Update daily history
    await db.execute(
        """INSERT INTO traffic_history(inbound_id, date, upload, download)
           VALUES(?, ?, ?, ?)
           ON CONFLICT(inbound_id, date) DO UPDATE SET
           upload = upload + ?,
           download = download + ?""",
        (inbound_id, today, upload, download, upload, download),
    )

    # Update global totals in settings
    current_up = int(await get_setting(db, "total_global_upload", "0"))
    current_down = int(await get_setting(db, "total_global_download", "0"))
    await set_setting(db, "total_global_upload", str(current_up + upload))
    await set_setting(db, "total_global_download", str(current_down + download))

    await db.commit()


async def cleanup_expired(db: aiosqlite.Connection) -> int:
    """Remove expired inbounds. Returns count of removed items."""
    cursor = await db.execute(
        "SELECT inbound_id, tag FROM inbounds WHERE expires_at IS NOT NULL AND expires_at <= datetime('now')"
    )
    expired = await cursor.fetchall()
    count = 0
    for row in expired:
        await db.execute("DELETE FROM inbounds WHERE inbound_id=?", (row["inbound_id"],))
        await db.execute(
            "DELETE FROM traffic_history WHERE inbound_id=?", (row["inbound_id"],)
        )
        await db.execute(
            "DELETE FROM connection_logs WHERE inbound_id=?", (row["inbound_id"],)
        )
        count += 1
    if count:
        await db.commit()
    return count


async def cleanup_old_traffic_history(db: aiosqlite.Connection):
    """Remove traffic history older than MAX_DAILY_TRAFFIC_HISTORY days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=MAX_DAILY_TRAFFIC_HISTORY)).strftime("%Y-%m-%d")
    await db.execute("DELETE FROM traffic_history WHERE date < ?", (cutoff,))
    await db.commit()


async def log_connection(
    db: aiosqlite.Connection,
    inbound_id: str,
    client_ip: str = "",
    upload: int = 0,
    download: int = 0,
):
    """Log a connection event."""
    await db.execute(
        """INSERT INTO connection_logs(inbound_id, client_ip, connected_at, upload_bytes, download_bytes)
           VALUES(?, ?, CURRENT_TIMESTAMP, ?, ?)""",
        (inbound_id, client_ip, upload, download),
    )
    await db.commit()


def format_bytes(b: int) -> str:
    """Format bytes to human-readable string."""
    if b < 1024:
        return f"{b} B"
    elif b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    elif b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.2f} MB"
    else:
        return f"{b / (1024 * 1024 * 1024):.2f} GB"


def format_datetime(dt_str: str) -> str:
    """Format a datetime string nicely."""
    if not dt_str:
        return "—"
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return dt_str


# ═══════════════════════════════════════════════
# Security Headers Middleware
# ═══════════════════════════════════════════════

async def security_headers_middleware(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-Permitted-Cross-Domain-Policies"] = "none"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data: blob: https:; "
        "connect-src 'self' ws: wss:; "
        "frame-ancestors 'none';"
    )
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


# ═══════════════════════════════════════════════
# Background Tasks
# ═══════════════════════════════════════════════

async def background_cleanup_loop():
    """Periodically clean up expired inbounds and old traffic history."""
    while True:
        try:
            db = await get_db()
            try:
                removed = await cleanup_expired(db)
                await cleanup_old_traffic_history(db)
                if removed:
                    print(f"[Cleanup] Removed {removed} expired inbounds")
            finally:
                await db.close()
        except Exception as e:
            print(f"[Cleanup] Error: {e}")
        await asyncio.sleep(CLEANUP_INTERVAL)


# ═══════════════════════════════════════════════
# App Lifecycle
# ═══════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    await init_db()
    print(f"[Luffy Panel] Database initialized at {DB_PATH}")
    print(f"[Luffy Panel] Admin password: {ADMIN_PASSWORD}")
    if XRAY_ENABLED:
        await update_xray_config()
        xray_proc = await start_xray()
        if xray_proc:
            app.state.xray_proc = xray_proc
            print(f"[Luffy Panel] Xray-core running on port {XRAY_LOCAL_PORT}")
        else:
            print("[Luffy Panel] WARNING: Xray-core failed to start")
    else:
        print("[Luffy Panel] Xray-core not found - proxy-only mode")
    cleanup_task = asyncio.create_task(background_cleanup_loop())
    app.state.cleanup_task = cleanup_task
    yield
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    print("[Luffy Panel] Shutting down")


# ═══════════════════════════════════════════════
# FastAPI App
# ═══════════════════════════════════════════════

app = FastAPI(
    title="Luffy Panel",
    description="VLESS Proxy Manager for Hugging Face Spaces",
    version="2.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.middleware("http")(security_headers_middleware)


# ═══════════════════════════════════════════════
# API Routes — Inbound Management
# ═══════════════════════════════════════════════

@app.get("/api/inbounds")
@limiter.limit("30/minute")
async def list_inbounds(request: Request):
    """List all inbounds."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM inbounds ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
        inbounds = []
        for row in rows:
            inbounds.append({
                "id": row["id"],
                "inbound_id": row["inbound_id"],
                "tag": row["tag"],
                "port": row["port"],
                "protocol": row["protocol"],
                "uuid": row["uuid"],
                "ws_path": row["ws_path"],
                "ws_host": row["ws_host"],
                "upstream_host": row["upstream_host"],
                "upstream_port": row["upstream_port"],
                "upstream_path": row["upstream_path"],
                "upstream_tls": row["upstream_tls"],
                "sni": row["sni"],
                "alpn": row["alpn"],
                "fingerprint": row["fingerprint"],
                "public_key": row["public_key"],
                "short_id": row["short_id"],
                "spider_x": row["spider_x"],
                "flow": row["flow"],
                "total_upload": row["total_upload"],
                "total_download": row["total_download"],
                "enabled": bool(row["enabled"]),
                "notes": row["notes"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "last_used_at": row["last_used_at"],
            })
        return {"success": True, "data": inbounds, "count": len(inbounds)}
    finally:
        await db.close()


@app.post("/api/inbounds")
@limiter.limit("10/minute")
async def create_inbound(request: Request):
    """Create a new inbound."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    inbound_id = generate_inbound_id()
    tag = data.get("tag", f"Luffy-{inbound_id}")
    port = int(data.get("port", 443))
    uuid = data.get("uuid", generate_uuid())
    ws_path = data.get("ws_path", "/ws")
    ws_host = data.get("ws_host", "")
    upstream_host = data.get("upstream_host", "")
    upstream_port = int(data.get("upstream_port", 443))
    upstream_path = data.get("upstream_path", "/")
    upstream_tls = data.get("upstream_tls", "tls")
    sni = data.get("sni", "")
    alpn = data.get("alpn", "")
    fingerprint = data.get("fingerprint", "chrome")
    public_key = data.get("public_key", "")
    short_id = data.get("short_id", "")
    spider_x = data.get("spider_x", "")
    flow = data.get("flow", "")
    notes = data.get("notes", "")
    expires_in_days = data.get("expires_in_days")

    if not upstream_host:
        if XRAY_ENABLED:
            upstream_host = "127.0.0.1"
            upstream_port = XRAY_LOCAL_PORT
            upstream_tls = "none"
        else:
            raise HTTPException(status_code=400, detail="upstream_host is required")

    expires_at = None
    if expires_in_days:
        expires_at = (datetime.now(timezone.utc) + timedelta(days=int(expires_in_days))).strftime("%Y-%m-%d %H:%M:%S")

    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO inbounds(inbound_id, tag, port, protocol, uuid, ws_path, ws_host,
               upstream_host, upstream_port, upstream_path, upstream_tls,
               sni, alpn, fingerprint, public_key, short_id, spider_x, flow,
               notes, expires_at)
               VALUES(?, ?, ?, 'vless', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                inbound_id, tag, port, uuid, ws_path, ws_host,
                upstream_host, upstream_port, upstream_path, upstream_tls,
                sni, alpn, fingerprint, public_key, short_id, spider_x, flow,
                notes, expires_at,
            ),
        )
        await db.commit()

        cursor = await db.execute(
            "SELECT * FROM inbounds WHERE inbound_id=?", (inbound_id,)
        )
        row = await cursor.fetchone()

        result = {
            "success": True,
            "data": {
                "id": row["id"],
                "inbound_id": row["inbound_id"],
                "tag": row["tag"],
                "port": row["port"],
                "protocol": row["protocol"],
                "uuid": row["uuid"],
                "ws_path": row["ws_path"],
                "upstream_host": row["upstream_host"],
                "upstream_port": row["upstream_port"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
            },
        }
        await update_xray_config()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await db.close()


@app.delete("/api/inbounds/{inbound_id}")
@limiter.limit("10/minute")
async def delete_inbound(request: Request, inbound_id: str):
    """Delete an inbound and all associated data."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id FROM inbounds WHERE inbound_id=?", (inbound_id,)
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail="Inbound not found")

        await db.execute("DELETE FROM inbounds WHERE inbound_id=?", (inbound_id,))
        await db.execute("DELETE FROM traffic_history WHERE inbound_id=?", (inbound_id,))
        await db.execute("DELETE FROM connection_logs WHERE inbound_id=?", (inbound_id,))
        await db.commit()
        await update_xray_config()
        return {"success": True, "message": "Inbound deleted"}
    finally:
        await db.close()


@app.get("/api/inbounds/{inbound_id}/vless")
@limiter.limit("30/minute")
async def get_vless_url(
    request: Request,
    inbound_id: str,
    host: Optional[str] = Query(None),
):
    """Generate the VLESS URL for an inbound."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM inbounds WHERE inbound_id=?", (inbound_id,)
        )
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Inbound not found")

        request_host = host or str(request.url.hostname or "localhost")
        vless_url = build_vless_url(
            uuid=row["uuid"],
            host=request_host,
            port=row["port"],
            path=row["ws_path"],
            tls=row["upstream_tls"],
            sni=row["sni"] or request_host,
            alpn=row["alpn"],
            fp=row["fingerprint"],
            flow=row["flow"],
            pbk=row["public_key"],
            sid=row["short_id"],
            spx=row["spider_x"],
            tag=row["tag"],
            inbound_id=row["inbound_id"],
        )
        return {"success": True, "vless_url": vless_url, "inbound_id": inbound_id}
    finally:
        await db.close()


@app.get("/api/traffic")
@limiter.limit("30/minute")
async def get_traffic_stats(request: Request):
    """Get traffic statistics."""
    db = await get_db()
    try:
        # Global totals
        total_up = int(await get_setting(db, "total_global_upload", "0"))
        total_down = int(await get_setting(db, "total_global_download", "0"))

        # Daily history
        cursor = await db.execute(
            """SELECT date, SUM(upload) as total_upload, SUM(download) as total_download
               FROM traffic_history
               GROUP BY date
               ORDER BY date DESC
               LIMIT ?""",
            (MAX_DAILY_TRAFFIC_HISTORY,),
        )
        daily = []
        async for row in cursor:
            daily.append({
                "date": row["date"],
                "upload": row["total_upload"],
                "download": row["total_download"],
            })

        # Per-inbound stats
        cursor2 = await db.execute(
            "SELECT inbound_id, tag, total_upload, total_download, enabled, last_used_at FROM inbounds ORDER BY total_download DESC"
        )
        per_inbound = []
        async for row in cursor2:
            per_inbound.append({
                "inbound_id": row["inbound_id"],
                "tag": row["tag"],
                "upload": row["total_upload"],
                "download": row["total_download"],
                "enabled": bool(row["enabled"]),
                "last_used": row["last_used_at"],
            })

        return {
            "success": True,
            "data": {
                "global_upload": total_up,
                "global_download": total_down,
                "daily_history": daily,
                "per_inbound": per_inbound,
            },
        }
    finally:
        await db.close()


@app.get("/api/connections")
@limiter.limit("30/minute")
async def get_connection_logs(request: Request, limit: int = 50):
    """Get recent connection logs."""
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT cl.*, i.tag
               FROM connection_logs cl
               LEFT JOIN inbounds i ON cl.inbound_id = i.inbound_id
               ORDER BY cl.connected_at DESC
               LIMIT ?""",
            (limit,),
        )
        logs = []
        async for row in cursor:
            logs.append({
                "id": row["id"],
                "inbound_id": row["inbound_id"],
                "tag": row["tag"] or "",
                "client_ip": row["client_ip"],
                "connected_at": row["connected_at"],
                "disconnected_at": row["disconnected_at"],
                "upload_bytes": row["upload_bytes"],
                "download_bytes": row["download_bytes"],
            })
        return {"success": True, "data": logs, "count": len(logs)}
    finally:
        await db.close()


@app.get("/api/export")
@limiter.limit("10/minute")
async def export_configs(request: Request, host: Optional[str] = Query(None)):
    """Export all inbounds as JSON with VLESS URLs."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM inbounds ORDER BY created_at DESC")
        rows = await cursor.fetchall()

        request_host = host or str(request.url.hostname or "localhost")
        configs = []
        for row in rows:
            vless_url = build_vless_url(
                uuid=row["uuid"],
                host=request_host,
                port=row["port"],
                path=row["ws_path"],
                tls=row["upstream_tls"],
                sni=row["sni"] or request_host,
                alpn=row["alpn"],
                fp=row["fingerprint"],
                flow=row["flow"],
                pbk=row["public_key"],
                sid=row["short_id"],
                spx=row["spider_x"],
                tag=row["tag"],
                inbound_id=row["inbound_id"],
            )
            configs.append({
                "inbound_id": row["inbound_id"],
                "tag": row["tag"],
                "protocol": row["protocol"],
                "uuid": row["uuid"],
                "ws_path": row["ws_path"],
                "host": request_host,
                "port": row["port"],
                "upstream_host": row["upstream_host"],
                "upstream_port": row["upstream_port"],
                "upstream_tls": row["upstream_tls"],
                "vless_url": vless_url,
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "total_upload": row["total_upload"],
                "total_download": row["total_download"],
                "notes": row["notes"],
            })

        export_data = {
            "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "panel": "Luffy Panel v2.0.0",
            "host": request_host,
            "total_configs": len(configs),
            "configs": configs,
        }

        return JSONResponse(
            content=export_data,
            headers={
                "Content-Disposition": "attachment; filename=luffy-configs.json"
            },
        )
    finally:
        await db.close()


@app.get("/api/stats")
@limiter.limit("30/minute")
async def get_system_stats(request: Request):
    """Get system resource stats."""
    import psutil

    try:
        cpu = psutil.cpu_percent(interval=0.5)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(DATA_DIR)
        uptime = time.time() - psutil.boot_time()

        db = await get_db()
        try:
            c1 = await db.execute("SELECT COUNT(*) as c FROM inbounds")
            inbound_count = (await c1.fetchone())["c"]
            c2 = await db.execute("SELECT COUNT(*) as c FROM connection_logs")
            connection_count = (await c2.fetchone())["c"]
        finally:
            await db.close()

        return {
            "success": True,
            "data": {
                "cpu_percent": cpu,
                "memory_percent": mem.percent,
                "memory_used_gb": round(mem.used / (1024**3), 2),
                "memory_total_gb": round(mem.total / (1024**3), 2),
                "disk_percent": disk.percent,
                "disk_free_gb": round(disk.free / (1024**3), 2),
                "uptime_seconds": int(uptime),
                "inbound_count": inbound_count,
                "connection_count": connection_count,
                "db_size_kb": round(os.path.getsize(DB_PATH) / 1024, 2) if os.path.exists(DB_PATH) else 0,
                "python_version": os.sys.version,
            },
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ═══════════════════════════════════════════════
# WebSocket Proxy / Tunnel
# ═══════════════════════════════════════════════

@app.websocket("/ws/{inbound_id}")
async def websocket_proxy(websocket: WebSocket, inbound_id: str):
    """Main VLESS-over-WebSocket proxy endpoint."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM inbounds WHERE inbound_id=? AND enabled=1", (inbound_id,)
        )
        inbound = await cursor.fetchone()
    finally:
        await db.close()

    if not inbound:
        await websocket.close(code=4000, reason="Inbound not found")
        return

    # Check expiration
    if inbound["expires_at"]:
        try:
            expires = datetime.fromisoformat(inbound["expires_at"].replace("Z", "+00:00"))
            if datetime.now(timezone.utc) > expires:
                await websocket.close(code=4001, reason="Inbound expired")
                return
        except Exception:
            pass

    await websocket.accept()

    client_ip = websocket.client.host if websocket.client else ""
    upload_total = 0
    download_total = 0
    upstream_ws = None

    try:
        # Connect to upstream
        upstream_url = (
            f"{'wss' if inbound['upstream_tls'] == 'tls' else 'ws'}://"
            f"{inbound['upstream_host']}:{inbound['upstream_port']}"
            f"{inbound['upstream_path']}"
        )

        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            async with client.stream(
                "GET",
                upstream_url.replace("ws://", "http://").replace("wss://", "https://"),
                headers={
                    "Upgrade": "websocket",
                    "Connection": "Upgrade",
                    "Sec-WebSocket-Version": "13",
                    "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
                    "Host": inbound["sni"] or inbound["upstream_host"],
                },
            ) as upstream_response:
                if upstream_response.status_code not in (101, 200):
                    await websocket.close(code=4002, reason="Upstream connection failed")
                    return

                # Bidirectional relay
                async def client_to_upstream():
                    nonlocal upload_total
                    while True:
                        try:
                            data = await websocket.receive_bytes()
                            upload_total += len(data)
                            # Write to upstream via raw TCP-like relay
                            # For WS-to-WS relay, we establish a separate WS connection
                        except WebSocketDisconnect:
                            break
                        except Exception:
                            break

                async def upstream_to_client():
                    nonlocal download_total
                    try:
                        async for chunk in upstream_response.aiter_bytes(8192):
                            download_total += len(chunk)
                            try:
                                await websocket.send_bytes(chunk)
                            except Exception:
                                break
                    except Exception:
                        pass

                # Run relay
                relay_task = asyncio.create_task(upstream_to_client())
                await client_to_upstream()
                relay_task.cancel()
                try:
                    await relay_task
                except asyncio.CancelledError:
                    pass

    except Exception as e:
        print(f"[WS Proxy] Error for {inbound_id}: {e}")
    finally:
        # Record traffic
        if upload_total > 0 or download_total > 0:
            try:
                db = await get_db()
                try:
                    await record_traffic(db, inbound_id, upload_total, download_total)
                    await log_connection(db, inbound_id, client_ip, upload_total, download_total)
                finally:
                    await db.close()
            except Exception as e:
                print(f"[WS Proxy] Failed to record traffic: {e}")

        try:
            await websocket.close()
        except Exception:
            pass


@app.websocket("/ws-raw/{inbound_id}")
async def websocket_tunnel(websocket: WebSocket, inbound_id: str):
    """Alternative raw WebSocket tunnel using httpx WebSocket."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM inbounds WHERE inbound_id=? AND enabled=1", (inbound_id,)
        )
        inbound = await cursor.fetchone()
    finally:
        await db.close()

    if not inbound:
        await websocket.close(code=4000, reason="Inbound not found")
        return

    await websocket.accept()
    client_ip = websocket.client.host if websocket.client else ""
    upload_total = 0
    download_total = 0

    upstream_url = (
        f"{'wss' if inbound['upstream_tls'] == 'tls' else 'ws'}://"
        f"{inbound['upstream_host']}:{inbound['upstream_port']}"
        f"{inbound['upstream_path']}"
    )

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
            async with client.stream("GET", upstream_url.replace("ws://", "http://").replace("wss://", "https://"),
                headers={
                    "Upgrade": "websocket",
                    "Connection": "Upgrade",
                    "Sec-WebSocket-Version": "13",
                    "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
                    "Host": inbound["sni"] or inbound["upstream_host"],
                }) as up_resp:

                if up_resp.status_code not in (101, 200):
                    await websocket.close(code=4002)
                    return

                async def relay_c2u():
                    nonlocal upload_total
                    try:
                        while True:
                            data = await websocket.receive_bytes()
                            upload_total += len(data)
                    except WebSocketDisconnect:
                        pass
                    except Exception:
                        pass

                async def relay_u2c():
                    nonlocal download_total
                    try:
                        async for chunk in up_resp.aiter_bytes(65536):
                            download_total += len(chunk)
                            try:
                                await websocket.send_bytes(chunk)
                            except Exception:
                                break
                    except Exception:
                        pass

                task = asyncio.create_task(relay_u2c())
                await relay_c2u()
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    except Exception as e:
        print(f"[WS Tunnel] Error for {inbound_id}: {e}")
    finally:
        if upload_total > 0 or download_total > 0:
            try:
                db = await get_db()
                try:
                    await record_traffic(db, inbound_id, upload_total, download_total)
                    await log_connection(db, inbound_id, client_ip, upload_total, download_total)
                finally:
                    await db.close()
            except Exception as e:
                print(f"[WS Tunnel] Failed to record traffic: {e}")
        try:
            await websocket.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════
# HTML Dashboard
# ═══════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
@limiter.exempt
async def dashboard(request: Request):
    """Serve the main dashboard HTML."""
    host = str(request.url.hostname or "localhost")
    scheme = "https"  # HF Spaces always serve HTTPS
    base_url = f"{scheme}://{host}"

    return HTMLResponse(content=DASHBOARD_HTML.replace("__BASE_URL__", base_url))


DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en" dir="auto">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🌊 Luffy Panel</title>
    <style>
        :root {
            --bg: #f0f4f8;
            --bg-card: #ffffff;
            --bg-input: #f8fafc;
            --text: #1a202c;
            --text-secondary: #64748b;
            --border: #e2e8f0;
            --primary: #3b82f6;
            --primary-hover: #2563eb;
            --danger: #ef4444;
            --danger-hover: #dc2626;
            --success: #22c55e;
            --success-bg: #dcfce7;
            --warning: #f59e0b;
            --warning-bg: #fef3c7;
            --shadow: 0 1px 3px rgba(0,0,0,0.1), 0 1px 2px rgba(0,0,0,0.06);
            --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.1), 0 4px 6px rgba(0,0,0,0.05);
            --radius: 12px;
            --transition: 0.2s ease;
        }
        @media (prefers-color-scheme: dark) {
            :root {
                --bg: #0f172a;
                --bg-card: #1e293b;
                --bg-input: #334155;
                --text: #e2e8f0;
                --text-secondary: #94a3b8;
                --border: #334155;
                --primary: #60a5fa;
                --primary-hover: #3b82f6;
                --danger: #f87171;
                --danger-hover: #ef4444;
                --success: #4ade80;
                --success-bg: #14532d;
                --warning: #fbbf24;
                --warning-bg: #422006;
                --shadow: 0 1px 3px rgba(0,0,0,0.3), 0 1px 2px rgba(0,0,0,0.2);
                --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.4), 0 4px 6px rgba(0,0,0,0.3);
            }
        }
        [data-theme="light"] {
            --bg: #f0f4f8; --bg-card: #ffffff; --bg-input: #f8fafc;
            --text: #1a202c; --text-secondary: #64748b; --border: #e2e8f0;
            --primary: #3b82f6; --primary-hover: #2563eb; --danger: #ef4444;
            --danger-hover: #dc2626; --success: #22c55e; --success-bg: #dcfce7;
            --warning: #f59e0b; --warning-bg: #fef3c7;
            --shadow: 0 1px 3px rgba(0,0,0,0.1); --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.1);
        }
        [data-theme="dark"] {
            --bg: #0f172a; --bg-card: #1e293b; --bg-input: #334155;
            --text: #e2e8f0; --text-secondary: #94a3b8; --border: #334155;
            --primary: #60a5fa; --primary-hover: #3b82f6; --danger: #f87171;
            --danger-hover: #ef4444; --success: #4ade80; --success-bg: #14532d;
            --warning: #fbbf24; --warning-bg: #422006;
            --shadow: 0 1px 3px rgba(0,0,0,0.3); --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.4);
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
            line-height: 1.6;
            transition: background var(--transition), color var(--transition);
        }

        .container { max-width: 1200px; margin: 0 auto; padding: 16px; }

        /* Header */
        header {
            background: var(--bg-card);
            border-bottom: 1px solid var(--border);
            padding: 16px 24px;
            position: sticky;
            top: 0;
            z-index: 100;
            box-shadow: var(--shadow);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
        }
        header h1 { font-size: 1.5rem; display: flex; align-items: center; gap: 8px; }
        .header-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }

        /* Buttons */
        .btn {
            display: inline-flex; align-items: center; gap: 6px;
            padding: 8px 16px; border: none; border-radius: 8px;
            cursor: pointer; font-size: 0.875rem; font-weight: 500;
            transition: all var(--transition); text-decoration: none;
            white-space: nowrap;
        }
        .btn-primary { background: var(--primary); color: #fff; }
        .btn-primary:hover { background: var(--primary-hover); transform: translateY(-1px); }
        .btn-danger { background: var(--danger); color: #fff; }
        .btn-danger:hover { background: var(--danger-hover); }
        .btn-outline {
            background: transparent; border: 1px solid var(--border); color: var(--text);
        }
        .btn-outline:hover { background: var(--bg-input); }
        .btn-sm { padding: 4px 10px; font-size: 0.75rem; }
        .btn-icon { padding: 6px 8px; min-width: 32px; justify-content: center; }

        /* Theme toggle */
        .theme-toggle {
            background: var(--bg-input); border: 1px solid var(--border);
            border-radius: 20px; padding: 6px; cursor: pointer;
            display: flex; align-items: center; gap: 4px;
            font-size: 1.1rem; color: var(--text);
            transition: all var(--transition);
        }
        .theme-toggle:hover { background: var(--border); }

        /* Cards */
        .card {
            background: var(--bg-card); border: 1px solid var(--border);
            border-radius: var(--radius); padding: 20px;
            box-shadow: var(--shadow); transition: all var(--transition);
            margin-bottom: 16px;
        }
        .card-header {
            display: flex; justify-content: space-between; align-items: center;
            margin-bottom: 16px; flex-wrap: wrap; gap: 8px;
        }
        .card-title { font-size: 1.1rem; font-weight: 600; }

        /* Stats grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: var(--bg-card); border: 1px solid var(--border);
            border-radius: var(--radius); padding: 16px;
            text-align: center; box-shadow: var(--shadow);
        }
        .stat-value { font-size: 1.5rem; font-weight: 700; color: var(--primary); }
        .stat-label { font-size: 0.75rem; color: var(--text-secondary); margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }

        /* Forms */
        .form-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
        }
        .form-group { display: flex; flex-direction: column; gap: 4px; }
        .form-group label { font-size: 0.8rem; font-weight: 500; color: var(--text-secondary); }
        .form-group input, .form-group select, .form-group textarea {
            padding: 8px 12px; border: 1px solid var(--border);
            border-radius: 8px; background: var(--bg-input); color: var(--text);
            font-size: 0.875rem; transition: border var(--transition);
            font-family: inherit;
        }
        .form-group input:focus, .form-group select:focus, .form-group textarea:focus {
            outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(59,130,246,0.1);
        }
        .form-group textarea { resize: vertical; min-height: 60px; }
        .form-full { grid-column: 1 / -1; }

        /* Table */
        .table-container { overflow-x: auto; -webkit-overflow-scrolling: touch; }
        table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
        th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--border); }
        th { font-weight: 600; color: var(--text-secondary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; white-space: nowrap; }
        tr:hover { background: var(--bg-input); }
        .text-mono { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.8rem; }
        .text-truncate { max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

        /* Badge */
        .badge {
            display: inline-block; padding: 2px 8px; border-radius: 12px;
            font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
        }
        .badge-active { background: var(--success-bg); color: var(--success); }
        .badge-expired { background: var(--warning-bg); color: var(--warning); }
        .badge-disabled { background: var(--bg-input); color: var(--text-secondary); }

        /* Tabs */
        .tabs { display: flex; gap: 4px; border-bottom: 2px solid var(--border); margin-bottom: 16px; overflow-x: auto; }
        .tab {
            padding: 8px 16px; cursor: pointer; border: none; background: none;
            color: var(--text-secondary); font-size: 0.875rem; font-weight: 500;
            border-bottom: 2px solid transparent; margin-bottom: -2px;
            transition: all var(--transition); white-space: nowrap;
        }
        .tab:hover { color: var(--text); }
        .tab.active { color: var(--primary); border-bottom-color: var(--primary); }
        .tab-content { display: none; }
        .tab-content.active { display: block; }

        /* Modal */
        .modal-overlay {
            display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(0,0,0,0.5); z-index: 200; align-items: center; justify-content: center;
        }
        .modal-overlay.active { display: flex; }
        .modal {
            background: var(--bg-card); border-radius: var(--radius);
            padding: 24px; max-width: 600px; width: 90%; max-height: 90vh;
            overflow-y: auto; box-shadow: var(--shadow-lg);
        }
        .modal h3 { margin-bottom: 16px; }
        .modal-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 16px; }

        /* QR */
        .qr-container { text-align: center; padding: 16px; }
        .qr-container canvas, .qr-container img { max-width: 200px; border-radius: 8px; }

        /* Toast */
        .toast-container {
            position: fixed; bottom: 20px; right: 20px; z-index: 300;
            display: flex; flex-direction: column; gap: 8px;
        }
        .toast {
            padding: 12px 20px; border-radius: 8px; color: #fff;
            font-size: 0.875rem; box-shadow: var(--shadow-lg);
            animation: slideIn 0.3s ease; cursor: pointer;
            white-space: nowrap;
        }
        .toast-success { background: var(--success); }
        .toast-error { background: var(--danger); }
        .toast-info { background: var(--primary); }
        @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }

        /* Chart placeholder */
        .chart-bar {
            display: flex; align-items: flex-end; gap: 4px; height: 120px;
            padding: 8px 0;
        }
        .chart-bar-item {
            flex: 1; background: var(--primary); border-radius: 4px 4px 0 0;
            min-height: 2px; opacity: 0.8; transition: opacity var(--transition);
            position: relative;
        }
        .chart-bar-item:hover { opacity: 1; }
        .chart-labels { display: flex; gap: 4px; font-size: 0.65rem; color: var(--text-secondary); }
        .chart-labels span { flex: 1; text-align: center; overflow: hidden; text-overflow: ellipsis; }

        /* Copy button feedback */
        .copied { animation: pulse 0.3s ease; }
        @keyframes pulse { 0%, 100% { transform: scale(1); } 50% { transform: scale(1.05); } }

        /* Mobile */
        @media (max-width: 768px) {
            .container { padding: 8px; }
            header { padding: 12px 16px; }
            header h1 { font-size: 1.2rem; }
            .card { padding: 12px; }
            .form-grid { grid-template-columns: 1fr; }
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
            th, td { padding: 8px 6px; font-size: 0.75rem; }
            .modal { width: 95%; padding: 16px; }
            .text-truncate { max-width: 100px; }
            .hide-mobile { display: none; }
        }

        /* RTL support */
        [dir="rtl"] th, [dir="rtl"] td { text-align: right; }
        [dir="rtl"] .header-actions { flex-direction: row-reverse; }
        [dir="rtl"] .btn { flex-direction: row-reverse; }

        /* Loading spinner */
        .spinner {
            display: inline-block; width: 20px; height: 20px; border: 2px solid var(--border);
            border-top-color: var(--primary); border-radius: 50%; animation: spin 0.6s linear infinite;
        }
        @keyframes spin { to { transform: rotate(360deg); } }

        .empty-state { text-align: center; padding: 40px 20px; color: var(--text-secondary); }
        .empty-state .icon { font-size: 3rem; margin-bottom: 12px; }
    </style>
</head>
<body>

<header>
    <h1>🌊 Luffy Panel <span style="font-size:0.7rem;color:var(--text-secondary);font-weight:400;">v2.0</span></h1>
    <div class="header-actions">
        <button class="theme-toggle" onclick="toggleTheme()" title="Toggle theme" id="themeToggle">🌓</button>
        <button class="btn btn-outline btn-sm" onclick="refreshAll()">🔄 Refresh</button>
        <button class="btn btn-primary btn-sm" onclick="openAddModal()">➕ Add Inbound</button>
        <button class="btn btn-outline btn-sm" onclick="exportConfigs()">📤 Export</button>
    </div>
</header>

<div class="container">
    <!-- Stats -->
    <div class="stats-grid" id="statsGrid">
        <div class="stat-card"><div class="stat-value" id="statInbounds">—</div><div class="stat-label" data-en="Active Inbounds" data-fa="اینباندهای فعال">Active Inbounds</div></div>
        <div class="stat-card"><div class="stat-value" id="statUpload">—</div><div class="stat-label" data-en="Total Upload" data-fa="آپلود کل">Total Upload</div></div>
        <div class="stat-card"><div class="stat-value" id="statDownload">—</div><div class="stat-label" data-en="Total Download" data-fa="دانلود کل">Total Download</div></div>
        <div class="stat-card"><div class="stat-value" id="statConnections">—</div><div class="stat-label" data-en="Connections" data-fa="اتصالات">Connections</div></div>
    </div>

    <!-- Tabs -->
    <div class="card">
        <div class="tabs">
            <button class="tab active" onclick="switchTab('inbounds')" data-en="Inbounds" data-fa="اینباندها">Inbounds</button>
            <button class="tab" onclick="switchTab('traffic')" data-en="Traffic Chart" data-fa="نمودار ترافیک">Traffic Chart</button>
            <button class="tab" onclick="switchTab('connections')" data-en="Connection Logs" data-fa="لاگ اتصالات">Connection Logs</button>
            <button class="tab" onclick="switchTab('system')" data-en="System" data-fa="سیستم">System</button>
        </div>

        <!-- Inbounds Tab -->
        <div class="tab-content active" id="tab-inbounds">
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th data-en="Tag" data-fa="برچسب">Tag</th>
                            <th class="hide-mobile" data-en="UUID" data-fa="UUID">UUID (Short)</th>
                            <th data-en="WS Path" data-fa="مسیر WS">WS Path</th>
                            <th class="hide-mobile" data-en="Traffic" data-fa="ترافیک">Traffic</th>
                            <th data-en="Status" data-fa="وضعیت">Status</th>
                            <th data-en="Actions" data-fa="عملیات">Actions</th>
                        </tr>
                    </thead>
                    <tbody id="inboundsTableBody">
                        <tr><td colspan="6" class="empty-state"><div class="spinner"></div><br>Loading...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Traffic Tab -->
        <div class="tab-content" id="tab-traffic">
            <div class="chart-bar" id="trafficChart"></div>
            <div class="chart-labels" id="trafficChartLabels"></div>
        </div>

        <!-- Connections Tab -->
        <div class="tab-content" id="tab-connections">
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th data-en="Time" data-fa="زمان">Time</th>
                            <th data-en="Tag" data-fa="برچسب">Tag</th>
                            <th data-en="Upload" data-fa="آپلود">Upload</th>
                            <th data-en="Download" data-fa="دانلود">Download</th>
                            <th class="hide-mobile" data-en="Client IP" data-fa="IP کلاینت">Client IP</th>
                        </tr>
                    </thead>
                    <tbody id="connectionsTableBody">
                        <tr><td colspan="5" class="empty-state"><div class="spinner"></div><br>Loading...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- System Tab -->
        <div class="tab-content" id="tab-system">
            <div class="stats-grid" id="systemStatsGrid"></div>
        </div>
    </div>
</div>

<!-- Add/Edit Modal -->
<div class="modal-overlay" id="addModal">
    <div class="modal">
        <h3 data-en="Add New Inbound" data-fa="افزودن اینباند جدید">Add New Inbound</h3>
        <form id="addForm" onsubmit="submitInbound(event)">
            <div class="form-grid">
                <div class="form-group">
                    <label data-en="Tag (Name)" data-fa="برچسب (نام)">Tag (Name)</label>
                    <input type="text" name="tag" placeholder="My Server" id="fieldTag">
                </div>
                <div class="form-group">
                    <label data-en="Port" data-fa="پورت">Port</label>
                    <input type="number" name="port" value="443" id="fieldPort">
                </div>
                <div class="form-group">
                    <label data-en="UUID (or leave empty to generate)" data-fa="UUID (خالی بگذارید تا ساخته شود)">UUID</label>
                    <input type="text" name="uuid" placeholder="Auto-generated" id="fieldUUID">
                </div>
                <div class="form-group">
                    <label data-en="WebSocket Path" data-fa="مسیر WebSocket">WS Path</label>
                    <input type="text" name="ws_path" value="/ws" id="fieldWSPath">
                </div>
                <div class="form-group">
                    <label data-en="WS Host Header (optional)" data-fa="هدر Host (اختیاری)">WS Host</label>
                    <input type="text" name="ws_host" placeholder="" id="fieldWSHost">
                </div>
                <div class="form-group">
                    <label data-en="Upstream Host *" data-fa="هاست آپ‌استریم *">Upstream Host *</label>
                    <input type="text" name="upstream_host" placeholder="example.com" required id="fieldUpstreamHost">
                </div>
                <div class="form-group">
                    <label data-en="Upstream Port" data-fa="پورت آپ‌استریم">Upstream Port</label>
                    <input type="number" name="upstream_port" value="443" id="fieldUpstreamPort">
                </div>
                <div class="form-group">
                    <label data-en="Upstream Path" data-fa="مسیر آپ‌استریم">Upstream Path</label>
                    <input type="text" name="upstream_path" value="/" id="fieldUpstreamPath">
                </div>
                <div class="form-group">
                    <label data-en="TLS" data-fa="TLS">TLS</label>
                    <select name="upstream_tls" id="fieldTLS">
                        <option value="tls">TLS</option>
                        <option value="none">None</option>
                    </select>
                </div>
                <div class="form-group">
                    <label data-en="SNI" data-fa="SNI">SNI</label>
                    <input type="text" name="sni" placeholder="Same as host" id="fieldSNI">
                </div>
                <div class="form-group">
                    <label data-en="Fingerprint" data-fa="اثر انگشت">Fingerprint</label>
                    <select name="fingerprint" id="fieldFingerprint">
                        <option value="chrome">Chrome</option>
                        <option value="firefox">Firefox</option>
                        <option value="safari">Safari</option>
                        <option value="randomized">Randomized</option>
                        <option value="ios">iOS</option>
                        <option value="android">Android</option>
                    </select>
                </div>
                <div class="form-group">
                    <label data-en="Flow" data-fa="Flow">Flow</label>
                    <select name="flow" id="fieldFlow">
                        <option value="">None</option>
                        <option value="xtls-rprx-vision">XTLS Vision</option>
                    </select>
                </div>
                <div class="form-group">
                    <label data-en="Expires In (days, optional)" data-fa="انقضا (روز، اختیاری)">Expires In (days)</label>
                    <input type="number" name="expires_in_days" placeholder="Never" id="fieldExpires">
                </div>
                <div class="form-group form-full">
                    <label data-en="Notes" data-fa="یادداشت">Notes</label>
                    <textarea name="notes" placeholder="Optional notes..." id="fieldNotes"></textarea>
                </div>
            </div>
            <div class="modal-actions">
                <button type="button" class="btn btn-outline" onclick="closeAddModal()" data-en="Cancel" data-fa="لغو">Cancel</button>
                <button type="submit" class="btn btn-primary" data-en="Create Inbound" data-fa="ایجاد اینباند">Create Inbound</button>
            </div>
        </form>
    </div>
</div>

<!-- QR Modal -->
<div class="modal-overlay" id="qrModal">
    <div class="modal">
        <h3 data-en="VLESS Config" data-fa="کانفیگ VLESS">VLESS Config</h3>
        <div class="qr-container">
            <div id="qrCodeContainer"></div>
        </div>
        <div style="margin-top:12px">
            <label style="font-size:0.8rem;color:var(--text-secondary);" data-en="VLESS URL (click to copy):" data-fa="لینک VLESS (کلیک کنید تا کپی شود):">VLESS URL:</label>
            <input type="text" id="vlessUrlDisplay" readonly
                   style="width:100%;padding:8px;border:1px solid var(--border);border-radius:8px;background:var(--bg-input);color:var(--text);font-family:monospace;font-size:0.75rem;cursor:pointer;margin-top:4px;"
                   onclick="copyVlessUrl()" data-en="Click to copy" data-fa="کلیک برای کپی">
        </div>
        <div class="modal-actions">
            <button class="btn btn-outline btn-sm" onclick="copyVlessUrl()">📋 <span data-en="Copy" data-fa="کپی">Copy</span></button>
            <button class="btn btn-outline btn-sm" onclick="downloadQR()">💾 <span data-en="Save QR" data-fa="ذخیره QR">Save QR</span></button>
            <button class="btn btn-outline" onclick="closeQrModal()" data-en="Close" data-fa="بستن">Close</button>
        </div>
    </div>
</div>

<!-- Toast container -->
<div class="toast-container" id="toastContainer"></div>

<!-- Scripts -->
<script src="https://cdn.jsdelivr.net/npm/qrcodejs@1.0.0/qrcode.min.js"></script>
<script>
// ═══════════════════════════════════════
// State & Configuration
// ═══════════════════════════════════════
const BASE_URL = '__BASE_URL__';
let currentTheme = localStorage.getItem('luffy-theme') || 'auto';
let currentLang = localStorage.getItem('luffy-lang') || 'en';
let currentVlessUrl = '';
let activeTab = 'inbounds';
let refreshTimer = null;

// ═══════════════════════════════════════
// Theme Management
// ═══════════════════════════════════════
function applyTheme(theme) {
    if (theme === 'auto') {
        document.documentElement.removeAttribute('data-theme');
    } else {
        document.documentElement.setAttribute('data-theme', theme);
    }
    currentTheme = theme;
    localStorage.setItem('luffy-theme', theme);
    updateThemeIcon();
}

function toggleTheme() {
    const themes = ['auto', 'light', 'dark'];
    const idx = themes.indexOf(currentTheme);
    const next = themes[(idx + 1) % themes.length];
    applyTheme(next);
    showToast('Theme: ' + next, 'info');
}

function updateThemeIcon() {
    const icons = { auto: '🌓', light: '☀️', dark: '🌙' };
    document.getElementById('themeToggle').textContent = icons[currentTheme] || '🌓';
}

// Listen for system theme changes
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
    if (currentTheme === 'auto') applyTheme('auto');
});

// ═══════════════════════════════════════
// Language Support
// ═══════════════════════════════════════
function t(key, lang) {
    const el = document.querySelector(`[data-${lang}]`);
    return el ? el.getAttribute(`data-${lang}`) : key;
}

function updateLanguage(lang) {
    currentLang = lang;
    localStorage.setItem('luffy-lang', lang);
    document.querySelectorAll('[data-en][data-fa]').forEach(el => {
        // Keep original text; for now just update dir
    });
    document.documentElement.dir = lang === 'fa' ? 'rtl' : 'ltr';
    document.documentElement.lang = lang === 'fa' ? 'fa' : 'en';
}

// ═══════════════════════════════════════
// Toast Notifications
// ═══════════════════════════════════════
function showToast(msg, type = 'info') {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = msg;
    toast.onclick = () => toast.remove();
    container.appendChild(toast);
    setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 4000);
}

// ═══════════════════════════════════════
// Tab Switching
// ═══════════════════════════════════════
function switchTab(tabName) {
    activeTab = tabName;
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    document.querySelector(`[onclick="switchTab('${tabName}')"]`).classList.add('active');
    document.getElementById(`tab-${tabName}`).classList.add('active');

    if (tabName === 'traffic') loadTrafficChart();
    else if (tabName === 'connections') loadConnections();
    else if (tabName === 'system') loadSystemStats();
    else loadInbounds();
}

// ═══════════════════════════════════════
// API Helpers
// ═══════════════════════════════════════
async function api(url, options = {}) {
    try {
        const res = await fetch(BASE_URL + url, {
            headers: { 'Content-Type': 'application/json', ...options.headers },
            ...options
        });
        if (!res.ok) {
            const err = await res.text();
            throw new Error(err || `HTTP ${res.status}`);
        }
        return await res.json();
    } catch (err) {
        showToast('Error: ' + err.message, 'error');
        throw err;
    }
}

// ═══════════════════════════════════════
// Load Data
// ═══════════════════════════════════════
async function loadInbounds() {
    try {
        const data = await api('/api/inbounds');
        const tbody = document.getElementById('inboundsTableBody');
        if (!data.data || data.data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="6" class="empty-state"><div class="icon">📭</div><p>No inbounds yet. Click "Add Inbound" to create one.</p></td></tr>`;
        } else {
            tbody.innerHTML = data.data.map(ib => {
                const shortUUID = ib.uuid ? ib.uuid.substring(0, 12) + '...' : '—';
                const traffic = formatBytes(ib.total_upload + ib.total_download);
                const created = formatDate(ib.created_at);
                const isExpired = ib.expires_at && new Date(ib.expires_at + 'Z') < new Date();
                const statusClass = !ib.enabled ? 'badge-disabled' : (isExpired ? 'badge-expired' : 'badge-active');
                const statusText = !ib.enabled ? 'Disabled' : (isExpired ? 'Expired' : 'Active');
                return `<tr>
                    <td><strong>${escHtml(ib.tag || '—')}</strong><br><small class="text-mono">${ib.inbound_id}</small></td>
                    <td class="hide-mobile text-mono text-truncate" title="${ib.uuid}">${shortUUID}</td>
                    <td class="text-mono">${escHtml(ib.ws_path)}</td>
                    <td class="hide-mobile">${traffic}<br><small style="color:var(--text-secondary)">${created}</small></td>
                    <td><span class="badge ${statusClass}">${statusText}</span></td>
                    <td>
                        <button class="btn btn-primary btn-sm" onclick="showQR('${ib.inbound_id}')">📷 QR</button>
                        <button class="btn btn-outline btn-sm" onclick="copyLink('${ib.inbound_id}')">📋</button>
                        <button class="btn btn-danger btn-sm" onclick="deleteInbound('${ib.inbound_id}')">🗑</button>
                    </td>
                </tr>`;
            }).join('');
        }
    } catch (err) {
        console.error('Failed to load inbounds', err);
    }
}

async function loadStats() {
    try {
        const data = await api('/api/traffic');
        if (data.success) {
            document.getElementById('statUpload').textContent = formatBytes(data.data.global_upload);
            document.getElementById('statDownload').textContent = formatBytes(data.data.global_download);
            document.getElementById('statInbounds').textContent = data.data.per_inbound.filter(i => i.enabled).length;
        }
        const conns = await api('/api/connections?limit=1');
        document.getElementById('statConnections').textContent = conns.count || 0;
    } catch (err) {
        console.error('Failed to load stats', err);
    }
}

async function loadTrafficChart() {
    try {
        const data = await api('/api/traffic');
        if (!data.success || !data.data.daily_history.length) {
            document.getElementById('trafficChart').innerHTML = '<div class="empty-state"><div class="icon">📊</div><p>No traffic data yet.</p></div>';
            document.getElementById('trafficChartLabels').innerHTML = '';
            return;
        }
        const daily = data.data.daily_history.reverse(); // oldest first
        const maxVal = Math.max(...daily.map(d => d.upload + d.download), 1);

        document.getElementById('trafficChart').innerHTML = daily.map(d => {
            const h = Math.max(((d.upload + d.download) / maxVal * 100), 2);
            return `<div class="chart-bar-item" style="height:${h}%" title="${d.date}: ${formatBytes(d.upload + d.download)}"></div>`;
        }).join('');
        document.getElementById('trafficChartLabels').innerHTML = daily.map(d => {
            const label = d.date.substring(5); // MM-DD
            return `<span>${label}</span>`;
        }).join('');
    } catch (err) {
        console.error('Failed to load traffic chart', err);
    }
}

async function loadConnections() {
    try {
        const data = await api('/api/connections?limit=100');
        const tbody = document.getElementById('connectionsTableBody');
        if (!data.data || data.data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="5" class="empty-state"><div class="icon">📡</div><p>No connections logged yet.</p></td></tr>`;
        } else {
            tbody.innerHTML = data.data.map(l => `
                <tr>
                    <td style="white-space:nowrap">${formatDate(l.connected_at)}</td>
                    <td>${escHtml(l.tag || l.inbound_id)}</td>
                    <td style="color:var(--success)">${formatBytes(l.upload_bytes)}</td>
                    <td style="color:var(--primary)">${formatBytes(l.download_bytes)}</td>
                    <td class="hide-mobile text-mono">${escHtml(l.client_ip || '—')}</td>
                </tr>
            `).join('');
        }
    } catch (err) {
        console.error('Failed to load connections', err);
    }
}

async function loadSystemStats() {
    try {
        const data = await api('/api/stats');
        if (data.success) {
            const s = data.data;
            document.getElementById('systemStatsGrid').innerHTML = `
                <div class="stat-card"><div class="stat-value">${s.cpu_percent}%</div><div class="stat-label">CPU</div></div>
                <div class="stat-card"><div class="stat-value">${s.memory_percent}%</div><div class="stat-label">Memory (${s.memory_used_gb}/${s.memory_total_gb} GB)</div></div>
                <div class="stat-card"><div class="stat-value">${s.disk_percent}%</div><div class="stat-label">Disk (${s.disk_free_gb} GB free)</div></div>
                <div class="stat-card"><div class="stat-value">${s.inbound_count}</div><div class="stat-label">Inbounds</div></div>
                <div class="stat-card"><div class="stat-value">${s.connection_count}</div><div class="stat-label">Total Connections</div></div>
                <div class="stat-card"><div class="stat-value">${s.db_size_kb} KB</div><div class="stat-label">DB Size</div></div>
                <div class="stat-card"><div class="stat-value">${formatUptime(s.uptime_seconds)}</div><div class="stat-label">System Uptime</div></div>
                <div class="stat-card"><div class="stat-value" style="font-size:0.8rem" title="${s.python_version}">${s.python_version.split('\\n')[0]}</div><div class="stat-label">Python</div></div>
            `;
        }
    } catch (err) {
        console.error('Failed to load system stats', err);
    }
}

function refreshAll() {
    loadStats();
    if (activeTab === 'inbounds') loadInbounds();
    else if (activeTab === 'traffic') loadTrafficChart();
    else if (activeTab === 'connections') loadConnections();
    else if (activeTab === 'system') loadSystemStats();
}

// ═══════════════════════════════════════
// Inbound CRUD
// ═══════════════════════════════════════
function openAddModal() {
    document.getElementById('addModal').classList.add('active');
    document.getElementById('addForm').reset();
    // Set defaults
    document.getElementById('fieldPort').value = '443';
    document.getElementById('fieldWSPath').value = '/ws';
    document.getElementById('fieldUpstreamPort').value = '443';
    document.getElementById('fieldUpstreamPath').value = '/';
    document.getElementById('fieldTLS').value = 'tls';
    document.getElementById('fieldFingerprint').value = 'chrome';
}

function closeAddModal() {
    document.getElementById('addModal').classList.remove('active');
}

async function submitInbound(event) {
    event.preventDefault();
    const form = event.target;
    const formData = new FormData(form);
    const data = Object.fromEntries(formData.entries());

    // Clean empty strings
    Object.keys(data).forEach(k => { if (data[k] === '') delete data[k]; });

    // Convert numeric fields
    if (data.port) data.port = parseInt(data.port);
    if (data.upstream_port) data.upstream_port = parseInt(data.upstream_port);
    if (data.expires_in_days) data.expires_in_days = parseInt(data.expires_in_days);

    try {
        const result = await api('/api/inbounds', {
            method: 'POST',
            body: JSON.stringify(data)
        });
        if (result.success) {
            showToast('Inbound created successfully!', 'success');
            closeAddModal();
            await loadInbounds();
            await loadStats();
        }
    } catch (err) {
        // error already shown by api()
    }
}

async function deleteInbound(inbound_id) {
    if (!confirm('Delete this inbound? This cannot be undone.')) return;
    try {
        const result = await api('/api/inbounds/' + inbound_id, { method: 'DELETE' });
        if (result.success) {
            showToast('Inbound deleted', 'success');
            await loadInbounds();
            await loadStats();
        }
    } catch (err) {
        // error already shown
    }
}

async function copyLink(inbound_id) {
    try {
        const data = await api('/api/inbounds/' + inbound_id + '/vless');
        if (data.success && data.vless_url) {
            await navigator.clipboard.writeText(data.vless_url);
            showToast('VLESS URL copied!', 'success');
        }
    } catch (err) {
        // Try showing QR modal instead
        showQR(inbound_id);
    }
}

async function exportConfigs() {
    try {
        const data = await api('/api/export');
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'luffy-configs-' + new Date().toISOString().split('T')[0] + '.json';
        a.click();
        URL.revokeObjectURL(url);
        showToast('Configs exported!', 'success');
    } catch (err) {
        // error shown by api()
    }
}

// ═══════════════════════════════════════
// QR Code
// ═══════════════════════════════════════
async function showQR(inbound_id) {
    try {
        const data = await api('/api/inbounds/' + inbound_id + '/vless');
        if (data.success && data.vless_url) {
            currentVlessUrl = data.vless_url;
            document.getElementById('vlessUrlDisplay').value = data.vless_url;
            document.getElementById('qrModal').classList.add('active');

            // Generate QR code client-side
            const container = document.getElementById('qrCodeContainer');
            container.innerHTML = '';
            try {
                new QRCode(container, {
                    text: data.vless_url,
                    width: 200,
                    height: 200,
                    colorDark: '#000000',
                    colorLight: '#ffffff',
                    correctLevel: QRCode.CorrectLevel.M
                });
            } catch (e) {
                container.innerHTML = '<p style="color:var(--danger)">QR generation failed. Use the URL below.</p>';
            }
        }
    } catch (err) {
        // error shown by api()
    }
}

function closeQrModal() {
    document.getElementById('qrModal').classList.remove('active');
    currentVlessUrl = '';
}

function copyVlessUrl() {
    const input = document.getElementById('vlessUrlDisplay');
    if (input.value) {
        navigator.clipboard.writeText(input.value).then(() => {
            showToast('Copied to clipboard!', 'success');
        }).catch(() => {
            input.select();
            document.execCommand('copy');
            showToast('Copied!', 'success');
        });
    }
}

function downloadQR() {
    const canvas = document.querySelector('#qrCodeContainer canvas');
    if (!canvas) {
        showToast('No QR code to save', 'error');
        return;
    }
    const link = document.createElement('a');
    link.download = 'luffy-qr-' + Date.now() + '.png';
    link.href = canvas.toDataURL('image/png');
    link.click();
    showToast('QR code saved!', 'success');
}

// ═══════════════════════════════════════
// Utility Functions
// ═══════════════════════════════════════
function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDate(dateStr) {
    if (!dateStr) return '—';
    try {
        const d = new Date(dateStr + (dateStr.endsWith('Z') ? '' : 'Z'));
        return d.toLocaleString();
    } catch (e) {
        return dateStr;
    }
}

function formatUptime(seconds) {
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const parts = [];
    if (d) parts.push(d + 'd');
    if (h) parts.push(h + 'h');
    if (m) parts.push(m + 'm');
    return parts.join(' ') || '<1m';
}

function escHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// ═══════════════════════════════════════
// Keyboard shortcuts
// ═══════════════════════════════════════
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeAddModal();
        closeQrModal();
    }
});

// Close modals on overlay click
document.getElementById('addModal').addEventListener('click', function(e) {
    if (e.target === this) closeAddModal();
});
document.getElementById('qrModal').addEventListener('click', function(e) {
    if (e.target === this) closeQrModal();
});

// ═══════════════════════════════════════
// Initialization
// ═══════════════════════════════════════
function init() {
    applyTheme(currentTheme);
    updateLanguage(currentLang);

    // Turn off RTL for now since the UI is primarily LTR-designed
    document.documentElement.dir = 'ltr';
    document.documentElement.lang = 'en';

    loadStats();
    loadInbounds();

    // Auto-refresh every 30 seconds
    refreshTimer = setInterval(refreshAll, 30000);
}

document.addEventListener('DOMContentLoaded', init);
</script>

</body>
</html>
"""


# ═══════════════════════════════════════════════
# Health Check
# ═══════════════════════════════════════════════

@app.get("/health")
@limiter.exempt
async def health_check():
    """Health check endpoint."""
    db_exists = os.path.exists(DB_PATH)
    return {
        "status": "healthy",
        "version": "2.0.0",
        "database": "connected" if db_exists else "initializing",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/ping")
@limiter.exempt
async def ping():
    """Simple ping endpoint."""
    return {"ping": "pong", "timestamp": int(time.time())}


# ═══════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
