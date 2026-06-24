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
# Logging
# ═══════════════════════════════════════════════

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LuffyPanel")

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
    # Include inbound_id in the WS path: /ws/{inbound_id}
    full_path = f"{path.rstrip('/')}/{inbound_id}" if inbound_id else path
    params.append(f"path={full_path}")
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


async def record_traffic(db: aiosqlite.Connection, inbound_id: str, upload: int, download: int):
    """Record traffic usage."""
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
           upload = upload + ?, download = download + ?""",
        (inbound_id, today, upload, download, upload, download),
    )

    # Update global totals
    for direction, amount in [("total_global_upload", upload), ("total_global_download", download)]:
        current = int(await get_setting(db, direction, "0"))
        await set_setting(db, direction, str(current + amount))

    await db.commit()


async def log_connection(db: aiosqlite.Connection, inbound_id: str, client_ip: str,
                         upload: int, download: int):
    """Log a connection."""
    await db.execute(
        """INSERT INTO connection_logs(inbound_id, client_ip, upload_bytes, download_bytes)
           VALUES(?, ?, ?, ?)""",
        (inbound_id, client_ip, upload, download),
    )
    await db.commit()


async def background_cleanup_loop():
    """Background task to clean up expired inbounds."""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        try:
            db = await get_db()
            try:
                cursor = await db.execute(
                    """SELECT inbound_id FROM inbounds
                       WHERE expires_at IS NOT NULL AND enabled=1
                         AND expires_at < datetime('now')"""
                )
                expired = await cursor.fetchall()
                for row in expired:
                    await db.execute(
                        "UPDATE inbounds SET enabled=0 WHERE inbound_id=?",
                        (row["inbound_id"],),
                    )
                    logging.info(f"[Cleanup] Disabled expired inbound: {row['inbound_id']}")
                if expired:
                    await db.commit()
                    await update_xray_config()
            finally:
                await db.close()
        except Exception as e:
            logging.error(f"[Cleanup] Error: {e}")


# ═══════════════════════════════════════════════
# Security Headers Middleware
# ═══════════════════════════════════════════════

async def security_headers_middleware(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response


# ═══════════════════════════════════════════════
# Lifespan
# ═══════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    await init_db()
    logging.info(f"Database initialized at {DB_PATH}")
    logging.info(f"Admin password: {ADMIN_PASSWORD}")
    if XRAY_ENABLED:
        await update_xray_config()
        xray_proc = await start_xray()
        if xray_proc:
            app.state.xray_proc = xray_proc
            logging.info(f"Xray-core running on port {XRAY_LOCAL_PORT}")
        else:
            logging.warning("Xray-core failed to start")
    else:
        logging.info("Xray-core not found - proxy-only mode")
    cleanup_task = asyncio.create_task(background_cleanup_loop())
    app.state.cleanup_task = cleanup_task
    yield
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    logging.info("Shutting down")


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
# API Routes - Inbounds
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
        result = []
        for row in rows:
            result.append({
                "id": row["id"],
                "inbound_id": row["inbound_id"],
                "tag": row["tag"],
                "port": row["port"],
                "protocol": row["protocol"],
                "uuid": row["uuid"],
                "ws_path": row["ws_path"],
                "upstream_host": row["upstream_host"],
                "upstream_port": row["upstream_port"],
                "total_upload": row["total_upload"],
                "total_download": row["total_download"],
                "enabled": bool(row["enabled"]),
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "last_used_at": row["last_used_at"],
            })
        return {"success": True, "data": result, "count": len(result)}
    finally:
        await db.close()


@app.post("/api/inbounds")
@limiter.limit("20/minute")
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
        # Client connects to HF Space via HTTPS, so VLESS URL always uses tls
        client_tls = "tls" if request.url.scheme in ("https", "wss") else "tls"
        vless_url = build_vless_url(
            uuid=row["uuid"],
            host=request_host,
            port=row["port"],
            path=row["ws_path"],
            tls=client_tls,
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
        total_up = int(await get_setting(db, "total_global_upload", "0"))
        total_down = int(await get_setting(db, "total_global_download", "0"))

        cursor = await db.execute(
            """SELECT date, SUM(upload) as total_upload, SUM(download) as total_download
               FROM traffic_history
               GROUP BY date
               ORDER BY date DESC
               LIMIT ?""",
            (MAX_DAILY_TRAFFIC_HISTORY,),
        )
        daily = [dict(row) for row in await cursor.fetchall()]

        return {
            "success": True,
            "data": {
                "total_upload": total_up,
                "total_download": total_down,
                "total_usage_gb": round((total_up + total_down) / (1024**3), 4),
                "daily_history": daily,
            },
        }
    finally:
        await db.close()


@app.get("/api/connections")
@limiter.limit("30/minute")
async def get_connection_logs(
    request: Request,
    limit: int = Query(100, ge=1, le=500),
):
    """Get recent connection logs."""
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT * FROM connection_logs
               ORDER BY connected_at DESC LIMIT ?""",
            (limit,),
        )
        rows = await cursor.fetchall()
        return {"success": True, "data": [dict(r) for r in rows]}
    finally:
        await db.close()


@app.get("/api/export")
@limiter.limit("10/minute")
async def export_configs(
    request: Request,
    host: Optional[str] = Query(None),
):
    """Export all inbound configs as JSON."""
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
                tls="tls",
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
                "uuid": row["uuid"],
                "vless_url": vless_url,
                "enabled": bool(row["enabled"]),
            })
        return {
            "success": True,
            "data": {
                "inbounds": configs,
                "export_time": datetime.now(timezone.utc).isoformat(),
            },
        }
    finally:
        await db.close()


@app.get("/api/stats")
@limiter.limit("30/minute")
async def get_system_stats(request: Request):
    """Get system statistics."""
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(DATA_DIR)
    except Exception:
        cpu, mem, disk = 0, type('obj', (object,), {'percent': 0, 'used': 0, 'total': 0, 'free': 0})(), type('obj', (object,), {'percent': 0, 'free': 0})()

    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) as c FROM inbounds WHERE enabled=1")
        inbound_count = (await cursor.fetchone())["c"]
        cursor = await db.execute("SELECT COUNT(*) as c FROM connection_logs")
        conn_count = (await cursor.fetchone())["c"]
        db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    finally:
        await db.close()

    return {
        "success": True,
        "data": {
            "cpu_percent": cpu,
            "memory_percent": mem.percent,
            "memory_used_gb": round(mem.used / (1024**3), 2),
            "memory_total_gb": round(mem.total / (1024**3), 2),
            "disk_percent": disk.percent if hasattr(disk, 'percent') else 0,
            "disk_free_gb": round(disk.free / (1024**3), 2) if hasattr(disk, 'free') else 0,
            "uptime_seconds": int(time.time() - psutil.boot_time()) if 'psutil' in dir() else 0,
            "inbound_count": inbound_count,
            "connection_count": conn_count,
            "db_size_kb": round(db_size / 1024, 1),
            "python_version": os.popen("python3 --version").read().strip() if os.path.exists("/usr/bin/python3") else "unknown",
        },
    }


# ═══════════════════════════════════════════════
# WebSocket Proxy - VLESS over WS → TCP upstream
# ═══════════════════════════════════════════════

@app.websocket("/ws/{inbound_id}")
async def websocket_proxy(websocket: WebSocket, inbound_id: str):
    """VLESS-over-WebSocket proxy: Client WS ↔ Xray TCP relay."""
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

    upstream_host = inbound["upstream_host"]
    upstream_port = int(inbound["upstream_port"])

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(upstream_host, upstream_port),
            timeout=10.0
        )

        async def ws_to_tcp():
            nonlocal upload_total
            try:
                while True:
                    data = await websocket.receive_bytes()
                    upload_total += len(data)
                    writer.write(data)
                    await writer.drain()
            except WebSocketDisconnect:
                pass
            except Exception:
                pass
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        async def tcp_to_ws():
            nonlocal download_total
            try:
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        break
                    download_total += len(chunk)
                    await websocket.send_bytes(chunk)
            except Exception:
                pass

        tcp_task = asyncio.create_task(tcp_to_ws())
        await ws_to_tcp()
        tcp_task.cancel()
        try:
            await tcp_task
        except asyncio.CancelledError:
            pass

    except asyncio.TimeoutError:
        logging.warning(f"[Proxy] Timeout connecting to {upstream_host}:{upstream_port}")
        await websocket.close(code=4003, reason="Upstream timeout")
    except ConnectionRefusedError:
        logging.error(f"[Proxy] Xray not running on {upstream_host}:{upstream_port}")
        await websocket.close(code=4004, reason="Upstream unavailable")
    except Exception as e:
        logging.error(f"[Proxy] Error for {inbound_id}: {e}")
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
                logging.error(f"[Proxy] Failed to record traffic: {e}")
        try:
            await websocket.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════
# Health & Ping
# ═══════════════════════════════════════════════

@app.get("/health")
async def health():
    """Health check endpoint."""
    db_ok = os.path.exists(DB_PATH)
    xray_status = "running" if XRAY_ENABLED else "disabled"
    return {
        "status": "healthy" if db_ok else "degraded",
        "version": "2.1.0",
        "database": "connected" if db_ok else "disconnected",
        "xray": xray_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/ping")
async def ping():
    """Simple ping endpoint."""
    return {"ping": "pong", "timestamp": int(time.time())}


# ═══════════════════════════════════════════════
# Dashboard HTML
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
            --bg: #f0f2f5;
            --bg-card: #fff;
            --text: #1a1a2e;
            --text-secondary: #666;
            --primary: #e74c3c;
            --primary-hover: #c0392b;
            --accent: #e74c3c;
            --border: #e0e0e0;
            --success: #27ae60;
            --warning: #f39c12;
            --danger: #e74c3c;
            --shadow: 0 1px 3px rgba(0,0,0,0.08);
        }
        @media (prefers-color-scheme: dark) {
            :root {
                --bg: #0d1117;
                --bg-card: #161b22;
                --text: #e6edf3;
                --text-secondary: #8b949e;
                --border: #30363d;
                --shadow: 0 1px 3px rgba(0,0,0,0.3);
            }
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
        }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        header {
            background: var(--bg-card);
            border-bottom: 1px solid var(--border);
            padding: 16px 24px;
            box-shadow: var(--shadow);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
        }
        header h1 { font-size: 1.5rem; color: var(--primary); }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin: 20px 0;
        }
        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 16px;
            box-shadow: var(--shadow);
        }
        .stat-card .label { font-size: 0.8rem; color: var(--text-secondary); text-transform: uppercase; }
        .stat-card .value { font-size: 1.8rem; font-weight: 700; margin-top: 4px; }
        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
            box-shadow: var(--shadow);
        }
        .card h2 { font-size: 1.1rem; margin-bottom: 16px; color: var(--primary); }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 8px; text-align: left; border-bottom: 1px solid var(--border); font-size: 0.9rem; }
        th { color: var(--text-secondary); font-weight: 600; text-transform: uppercase; font-size: 0.75rem; }
        .text-mono { font-family: 'Courier New', monospace; font-size: 0.8rem; word-break: break-all; }
        button, .btn {
            background: var(--primary);
            color: #fff;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.9rem;
            font-weight: 500;
            transition: background 0.2s;
        }
        button:hover { background: var(--primary-hover); }
        button.danger { background: var(--danger); }
        button.secondary { background: var(--text-secondary); }
        input, select {
            background: var(--bg);
            border: 1px solid var(--border);
            color: var(--text);
            padding: 8px 12px;
            border-radius: 6px;
            font-size: 0.9rem;
            width: 100%;
            margin: 4px 0 12px;
        }
        .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
        @media (max-width: 768px) {
            .form-row { grid-template-columns: 1fr; }
            .stats-grid { grid-template-columns: 1fr 1fr; }
        }
        .badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.7rem;
            font-weight: 600;
        }
        .badge-active { background: var(--success); color: #fff; }
        .badge-inactive { background: var(--text-secondary); color: #fff; }
        .empty-state { text-align: center; padding: 40px; color: var(--text-secondary); }
        .toast {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: var(--success);
            color: #fff;
            padding: 12px 24px;
            border-radius: 8px;
            font-weight: 500;
            z-index: 9999;
            animation: fadeIn 0.3s;
        }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        .spinner {
            width: 30px; height: 30px;
            border: 3px solid var(--border);
            border-top-color: var(--primary); border-radius: 50%; animation: spin 0.6s linear infinite;
            margin: 20px auto;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        .qrcode-container { text-align: center; margin: 16px 0; }
        #qrcode-modal { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.6); z-index: 9999; align-items: center; justify-content: center; }
        #qrcode-modal.show { display: flex; }
        #qrcode-modal .modal-content { background: var(--bg-card); padding: 24px; border-radius: 12px; text-align: center; max-width: 400px; }
    </style>
</head>
<body>
    <header>
        <h1>🏴‍☠️ Luffy Panel</h1>
        <div>
            <button onclick="refreshAll()">🔄 Refresh</button>
            <button onclick="showCreateForm()" style="margin-left:8px">➕ New Inbound</button>
            <button onclick="exportConfigs()" class="secondary" style="margin-left:8px">📥 Export</button>
        </div>
    </header>

    <div class="container">
        <div class="stats-grid" id="stats-grid">
            <div class="stat-card"><div class="label">Active Inbounds</div><div class="value" id="stat-inbounds">-</div></div>
            <div class="stat-card"><div class="label">Connections</div><div class="value" id="stat-connections">-</div></div>
            <div class="stat-card"><div class="label">Total Traffic</div><div class="value" id="stat-traffic">-</div></div>
            <div class="stat-card"><div class="label">CPU / Memory</div><div class="value" id="stat-cpu">-</div></div>
        </div>

        <div class="card">
            <h2>📡 Inbounds</h2>
            <table>
                <thead>
                    <tr><th>Tag</th><th>UUID</th><th>Path</th><th>Upstream</th><th>Traffic</th><th>Actions</th></tr>
                </thead>
                <tbody id="inbounds-table">
                    <tr><td colspan="6" class="empty-state"><div class="spinner"></div><br>Loading...</td></tr>
                </tbody>
            </table>
        </div>

        <div class="card" id="create-card" style="display:none">
            <h2>➕ Create Inbound</h2>
            <form id="create-form" onsubmit="createInbound(event)">
                <div class="form-row">
                    <div><label>Label</label><input name="label" placeholder="My VPN" required></div>
                    <div><label>Upstream Host</label><input name="upstream_host" placeholder="127.0.0.1 (default: Xray)"></div>
                </div>
                <div class="form-row">
                    <div><label>Upstream Port</label><input name="upstream_port" type="number" value="10000"></div>
                    <div><label>WS Path</label><input name="ws_path" value="/ws"></div>
                </div>
                <div class="form-row">
                    <div><label>Data Limit (GB)</label><input name="limit_value" type="number" value="0" placeholder="0 = unlimited"></div>
                    <div><label>Max Connections</label><input name="max_connections" type="number" value="0" placeholder="0 = unlimited"></div>
                </div>
                <div class="form-row">
                    <div><label>SNI</label><input name="sni" placeholder="Auto"></div>
                    <div><label>Expires (days)</label><input name="expires_in_days" type="number" placeholder="0 = never"></div>
                </div>
                <div style="margin-top:12px">
                    <button type="submit">✅ Create</button>
                    <button type="button" class="secondary" onclick="document.getElementById('create-card').style.display='none'">Cancel</button>
                </div>
            </form>
        </div>

        <div class="card" id="qrcode-modal">
            <div class="modal-content">
                <h3>📱 QR Code</h3>
                <div id="qrcode-container" class="qrcode-container"></div>
                <button class="secondary" onclick="hideQR()" style="margin-top:12px">Close</button>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/qrcodejs@1.0.0/qrcode.min.js"></script>
    <script>
        const BASE_URL = '__BASE_URL__';

        async function api(url, opts = {}) {
            const res = await fetch(BASE_URL + url, {
                headers: { 'Content-Type': 'application/json', ...opts.headers },
                ...opts,
            });
            return res.json();
        }

        function escHtml(s) { return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
        function fmtBytes(b) { return b > 1e9 ? (b/1e9).toFixed(2)+' GB' : b > 1e6 ? (b/1e6).toFixed(1)+' MB' : (b/1024).toFixed(1)+' KB'; }

        async function refreshAll() {
            const [inbounds, stats] = await Promise.all([
                api('/api/inbounds'),
                api('/api/stats'),
            ]);

            if (stats.success) {
                const d = stats.data;
                document.getElementById('stat-inbounds').textContent = d.inbound_count;
                document.getElementById('stat-connections').textContent = d.connection_count;
                document.getElementById('stat-traffic').textContent = fmtBytes((d.uptime_seconds || 0) * 1024);
                document.getElementById('stat-cpu').textContent = d.cpu_percent + '% / ' + d.memory_percent + '%';
            }

            if (inbounds.success) {
                const tbody = document.getElementById('inbounds-table');
                if (inbounds.data.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No inbounds yet. Click ➕ New Inbound</td></tr>';
                } else {
                    tbody.innerHTML = inbounds.data.map(ib =>
                        `<tr>
                            <td><strong>${escHtml(ib.tag)}</strong></td>
                            <td class="text-mono">${escHtml(ib.uuid).substring(0,12)}...</td>
                            <td class="text-mono">${escHtml(ib.ws_path)}</td>
                            <td>${escHtml(ib.upstream_host)}:${ib.upstream_port}</td>
                            <td>⬆${fmtBytes(ib.total_upload||0)} ⬇${fmtBytes(ib.total_download||0)}</td>
                            <td>
                                <button onclick="showQR('${ib.inbound_id}')">📱</button>
                                <button onclick="deleteInbound('${ib.inbound_id}')" class="danger">🗑</button>
                            </td>
                        </tr>`
                    ).join('');
                }
            }
        }

        async function createInbound(e) {
            e.preventDefault();
            const form = document.getElementById('create-form');
            const fd = new FormData(form);
            const data = Object.fromEntries(fd.entries());
            const result = await api('/api/inbounds', {
                method: 'POST',
                body: JSON.stringify(data),
            });
            if (result.success) {
                showToast('✅ Inbound created!');
                form.reset();
                document.getElementById('create-card').style.display = 'none';
                refreshAll();
            } else {
                alert('Error: ' + (result.detail || 'Unknown error'));
            }
        }

        async function deleteInbound(id) {
            if (!confirm('Delete inbound ' + id + '?')) return;
            await api('/api/inbounds/' + id, { method: 'DELETE' });
            showToast('🗑 Deleted');
            refreshAll();
        }

        async function showQR(inboundId) {
            const res = await api('/api/inbounds/' + inboundId + '/vless');
            if (res.success && res.vless_url) {
                const modal = document.getElementById('qrcode-modal');
                modal.classList.add('show');
                const container = document.getElementById('qrcode-container');
                container.innerHTML = '';
                new QRCode(container, { text: res.vless_url, width: 256, height: 256 });
            }
        }

        function hideQR() {
            document.getElementById('qrcode-modal').classList.remove('show');
        }

        function showCreateForm() {
            document.getElementById('create-card').style.display = 'block';
            window.scrollTo({ top: document.getElementById('create-card').offsetTop, behavior: 'smooth' });
        }

        async function exportConfigs() {
            const res = await api('/api/export');
            if (res.success) {
                const blob = new Blob([JSON.stringify(res.data, null, 2)], { type: 'application/json' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url; a.download = 'luffy-configs.json'; a.click();
                URL.revokeObjectURL(url);
                showToast('📥 Exported!');
            }
        }

        function showToast(msg) {
            const t = document.createElement('div');
            t.className = 'toast';
            t.textContent = msg;
            document.body.appendChild(t);
            setTimeout(() => t.remove(), 3000);
        }

        document.addEventListener('DOMContentLoaded', () => {
            refreshAll();
            setInterval(refreshAll, 30000);
        });
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
