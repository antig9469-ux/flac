#!/usr/bin/env python3
import asyncio
import base64
import hashlib
import json
import logging
import os
import random
import re
import time
import urllib.parse
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Union

import httpx
import uvicorn
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

logger = logging.getLogger(__name__)

load_dotenv()

API_VERSION = "2.10"

# Shared HTTP client is created in app lifespan for connection reuse
_http_client: Optional[httpx.AsyncClient] = None
_http_client_proxy_url: Optional[str] = None
_http_client_lock = asyncio.Lock()

# One lock per credential to avoid global contention during token refreshes
_refresh_locks: Dict[str, asyncio.Lock] = {}

# Loaded credential set from token.json; each entry will be enriched with access cache
_creds: List[dict] = []

# Optional credential reserved for catalog/metadata traffic. It is deliberately
# kept out of the playback pool and is not request-serialized.
_catalog_cred: Optional[dict] = None

# Global semaphore to limit concurrent album track fetches across all requests
_album_tracks_sem = asyncio.Semaphore(20)

# List of proxies loaded from file at startup
_proxies: List[str] = []

# Cache of the last proxy confirmed to be working
_last_known_good_proxy: Optional[str] = None


def _build_http_client(proxy_url: Optional[str] = None) -> httpx.AsyncClient:
    # Pack common settings into a dictionary to keep things DRY
    client_kwargs = {
        "headers": _tidal_headers(),
        "timeout": httpx.Timeout(connect=3.0, read=12.0, write=8.0, pool=12.0),
        "limits": httpx.Limits(
            max_keepalive_connections=500,
            max_connections=1000,
            keepalive_expiry=30.0,
        ),
    }

    try:
        return httpx.AsyncClient(proxy=proxy_url, http2=True, **client_kwargs)
    except (TypeError, ImportError):
        try:
            return httpx.AsyncClient(proxy=proxy_url, http2=False, **client_kwargs)
        except TypeError:
            legacy_proxies = {"all://": proxy_url} if proxy_url else None
            return httpx.AsyncClient(proxies=legacy_proxies, **client_kwargs)


def _build_proxy_test_client(proxy_url: str) -> httpx.AsyncClient:
    try:
        # Modern httpx
        return httpx.AsyncClient(proxy=proxy_url, timeout=5.0)
    except TypeError:
        # Legacy httpx
        return httpx.AsyncClient(proxies={"all://": proxy_url}, timeout=5.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client, _http_client_proxy_url
    if DEV_MODE:
        logger.warning("DEV_MODE is enabled — upstream responses will be logged at DEBUG level")
    if _http_client is None:
        proxy_url = None
        if USE_PROXIES:
            proxy_url = await get_working_proxy()
            if not proxy_url and not FALLBACK_TO_DIRECT_CONNECTION:
                logger.error("Could not find a working proxy and FALLBACK_TO_DIRECT_CONNECTION is False. Shutting down.")
                raise RuntimeError("No working proxies available")
            elif not proxy_url and FALLBACK_TO_DIRECT_CONNECTION:
                logger.warning("Could not find a working proxy, falling back to direct connection. HOST IP MAY BE EXPOSED!")
        _http_client = _build_http_client(proxy_url)
        _http_client_proxy_url = proxy_url
        if not _creds:
            try:
                await fetch_archivepool_sources()
            except Exception as e:
                logger.warning(f"Initial ArchivePool fetch failed: {e}")
    try:
        yield
    finally:
        if _http_client:
            await _http_client.aclose()
            _http_client = None
            _http_client_proxy_url = None

app = FastAPI(
    title="HiFi-RestAPI",
    version=API_VERSION,
    description="Tidal Music Proxy",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Config (defaults act as fallback if token file missing)
CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
REFRESH_TOKEN: Optional[str] = os.getenv("REFRESH_TOKEN")
USER_ID = os.getenv("USER_ID")
TOKEN_FILE = os.getenv("TOKEN_FILE", "token.json")
COUNTRY_CODE = os.getenv("COUNTRY_CODE", "US")

# Catalog traffic can use either a static bearer token or a refreshable account.
# A token.json entry with `"role": "catalog"` takes precedence over these.
CATALOG_TOKEN = os.getenv("CATALOG_TOKEN") or os.getenv("CATALOG_ACCESS_TOKEN")
CATALOG_CLIENT_ID = os.getenv("CATALOG_CLIENT_ID") or CLIENT_ID
CATALOG_CLIENT_SECRET = os.getenv("CATALOG_CLIENT_SECRET") or CLIENT_SECRET
CATALOG_REFRESH_TOKEN = os.getenv("CATALOG_REFRESH_TOKEN")
CATALOG_USER_ID = os.getenv("CATALOG_USER_ID")

USE_PROXIES = os.getenv("USE_PROXIES", "False").lower() in ("true", "1", "yes")
ROTATE_PROXIES_ON_REFRESH = os.getenv("ROTATE_PROXIES_ON_REFRESH", "False").lower() in ("true", "1", "yes")
PROXIES_FILE = os.getenv("PROXIES_FILE", "proxies.txt")
FALLBACK_TO_DIRECT_CONNECTION = os.getenv("FALLBACK_TO_DIRECT_CONNECTION", "False").lower() in ("true", "1", "yes")
# Maximum number of proxy candidates to test per get_working_proxy() call
MAX_PROXY_CANDIDATES = 10
# Maximum number of concurrent proxy tests inside get_working_proxy()
_PROXY_TEST_CONCURRENCY = 5
_max_retries_raw = os.getenv("MAX_RETRIES", "2")
USER_AGENT = os.getenv("USER_AGENT", "okhttp/5.3.2")


def _tidal_headers(extra: dict | None = None) -> dict:
    h = {
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
        "Accept-Encoding": "gzip",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Platform": "android",
        "X-Tidal-Platform": "android",
    }
    if extra:
        h.update(extra)
    return h


_TIDAL_DEFAULT_HEADERS = _tidal_headers()

DEV_MODE = os.getenv("DEV_MODE", "False").lower() in ("true", "1", "yes")

_RATE_LIMIT_MAX_RETRIES = 3
_RATE_LIMIT_BASE_DELAY = 1.0
_RATE_LIMIT_MAX_DELAY = 10.0

def _log_response(method: str, url: str, resp: httpx.Response):
    if not DEV_MODE:
        return
    logger.info(
        "[DEV] %s %s → %s\n  headers: %s\n  body: %s",
        method,
        url,
        resp.status_code,
        dict(resp.headers),
        resp.text[:2000],
    )

try:
    MAX_RETRIES = int(_max_retries_raw)
except ValueError:
    MAX_RETRIES = 2
if MAX_RETRIES < 1:
    MAX_RETRIES = 1
def load_proxies():
    """Load proxies from file into the global _proxies list."""
    global _proxies
    if not os.path.exists(PROXIES_FILE):
        logger.warning(f"Proxies file {PROXIES_FILE} not found.")
        _proxies = []
        return
    with open(PROXIES_FILE, "r") as f:
        _proxies = [line.strip() for line in f if line.strip()]
    logger.info(f"Loaded {len(_proxies)} proxies.")


async def test_proxy(proxy_url: str) -> bool:
    try:
        async with _build_proxy_test_client(proxy_url) as client:
            resp = await client.get("http://example.com")
            return resp.status_code == 200
    except Exception:
        return False


async def get_working_proxy(avoid_proxy: Optional[str] = None) -> Optional[str]:
    global _last_known_good_proxy

    if not _proxies:
        return None

    # Try the cached proxy first (unless it is the one we want to avoid)
    if _last_known_good_proxy and _last_known_good_proxy != avoid_proxy:
        if await test_proxy(_last_known_good_proxy):
            return _last_known_good_proxy

    shuffled_proxies = _proxies[:]
    random.shuffle(shuffled_proxies)

    if avoid_proxy:
        candidate_proxies = [p for p in shuffled_proxies if p != avoid_proxy]
        if not candidate_proxies:
            candidate_proxies = shuffled_proxies
    else:
        candidate_proxies = shuffled_proxies

    # Exclude the already-tested cached proxy and cap the candidate list
    if _last_known_good_proxy:
        candidate_proxies = [p for p in candidate_proxies if p != _last_known_good_proxy]
    candidate_proxies = candidate_proxies[:MAX_PROXY_CANDIDATES]

    # Test candidates concurrently, returning the first one that succeeds
    sem = asyncio.Semaphore(_PROXY_TEST_CONCURRENCY)
    found_event = asyncio.Event()
    selected_proxy: List[Optional[str]] = [None]

    async def probe(proxy: str) -> None:
        if found_event.is_set():
            return
        async with sem:
            if found_event.is_set():
                return
            if await test_proxy(proxy):
                if not found_event.is_set():
                    selected_proxy[0] = proxy
                    found_event.set()

    await asyncio.gather(*[probe(p) for p in candidate_proxies], return_exceptions=True)

    if selected_proxy[0]:
        _last_known_good_proxy = selected_proxy[0]
    return selected_proxy[0]

async def _delayed_close(client: httpx.AsyncClient):
    await asyncio.sleep(15)
    await client.aclose()

async def update_global_client(force_new_proxy: bool = False):
    global _http_client, _http_client_proxy_url
    async with _http_client_lock:
        proxy_to_avoid = None
        if force_new_proxy and _http_client_proxy_url:
            proxy_to_avoid = _http_client_proxy_url

        proxy_url = None
        if USE_PROXIES:
            proxy_url = await get_working_proxy(avoid_proxy=proxy_to_avoid)
            if not proxy_url:
                if FALLBACK_TO_DIRECT_CONNECTION:
                    logger.warning("Could not find a working proxy, falling back to direct connection. HOST IP MAY BE EXPOSED!")
                else:
                    logger.error("Could not find a working proxy and FALLBACK_TO_DIRECT_CONNECTION is False.")
                    raise HTTPException(status_code=503, detail="Service Unavailable")

        # Only create a new client if the proxy is actually different
        if _http_client and _http_client_proxy_url == proxy_url:
            return

        new_client = _build_http_client(proxy_url)
        old_client = _http_client
        _http_client = new_client
        _http_client_proxy_url = proxy_url

        if old_client is not None:
            asyncio.create_task(_delayed_close(old_client))


if USE_PROXIES:
    load_proxies()

if os.path.exists(TOKEN_FILE):
    with open(TOKEN_FILE, "r") as tok:
        token_data = json.load(tok)
        if isinstance(token_data, dict):
            token_data = [token_data]

        for entry in token_data:
            is_catalog = entry.get("role") == "catalog" or entry.get("catalog") is True
            cred = {
                "client_id": entry.get("client_ID") or CLIENT_ID,
                "client_secret": entry.get("client_secret") or CLIENT_SECRET,
                "refresh_token": entry.get("refresh_token") or REFRESH_TOKEN,
                "user_id": entry.get("userID") or USER_ID,
                # Access tokens in file have unknown expiry; force refresh on first use
                "access_token": None,
                "expires_at": 0,
            }
            if cred["refresh_token"]:
                if is_catalog:
                    if _catalog_cred is None:
                        _catalog_cred = cred
                    else:
                        logger.warning("Ignoring additional catalog credential in %s", TOKEN_FILE)
                elif not any(c["refresh_token"] == cred["refresh_token"] for c in _creds):
                    _creds.append(cred)

# Add env var credential if available and unique (simple check)
if REFRESH_TOKEN:
    env_cred = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "user_id": USER_ID,
        "access_token": None,
        "expires_at": 0,
    }
    # Avoid adding duplicate if it was already loaded from file with same refresh token
    if not any(c["refresh_token"] == REFRESH_TOKEN for c in _creds):
        _creds.append(env_cred)

if _catalog_cred is None and CATALOG_REFRESH_TOKEN:
    _catalog_cred = {
        "client_id": CATALOG_CLIENT_ID,
        "client_secret": CATALOG_CLIENT_SECRET,
        "refresh_token": CATALOG_REFRESH_TOKEN,
        "user_id": CATALOG_USER_ID,
        "access_token": None,
        "expires_at": 0,
    }

if _catalog_cred is not None:
    # A credential explicitly reserved for catalog traffic must never also
    # become a playback slot, even if it was duplicated via environment vars.
    _creds[:] = [
        cred for cred in _creds
        if cred["refresh_token"] != _catalog_cred["refresh_token"]
    ]

if _creds:
    CLIENT_ID = _creds[0]["client_id"]
    CLIENT_SECRET = _creds[0]["client_secret"]
    REFRESH_TOKEN = _creds[0]["refresh_token"]


def _pick_credential() -> dict:
    if not _creds:
        raise HTTPException(status_code=500, detail="No Tidal credentials available; populate token.json")
    return random.choice(_creds)


def _lock_for_cred(cred: dict) -> asyncio.Lock:
    key = f"{cred['client_id']}:{cred['refresh_token']}"
    lock = _refresh_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _refresh_locks[key] = lock
    return lock


class PlaybackCredentialPool:
    """Lease each playback credential to at most one request at a time."""

    def __init__(self, credentials: List[dict]):
        self._available: asyncio.Queue[dict] = asyncio.Queue()
        self._size = len(credentials)
        for cred in credentials:
            self._available.put_nowait(cred)

    @property
    def size(self) -> int:
        return self._size

    @property
    def available(self) -> int:
        return self._available.qsize()

    def try_acquire(self) -> Optional[dict]:
        if self._size == 0:
            raise HTTPException(status_code=500, detail="No Tidal playback credentials available; populate token.json")
        try:
            return self._available.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def release(self, cred: dict) -> None:
        self._available.put_nowait(cred)

    def add_credential(self, cred: dict) -> None:
        self._size += 1
        self._available.put_nowait(cred)

    @asynccontextmanager
    async def lease(self) -> AsyncIterator[dict]:
        if self._size == 0:
            raise HTTPException(status_code=500, detail="No Tidal playback credentials available; populate token.json")

        cred = await self._available.get()
        try:
            yield cred
        finally:
            self.release(cred)


_playback_pool = PlaybackCredentialPool(_creds)


@dataclass
class PlaybackJob:
    request_id: str
    operation: Optional[Callable[[dict], Awaitable[Any]]]
    created_at: float
    state: str = "pending"
    result: Any = None
    error: Optional[str] = None
    error_status: int = 500
    finished_at: Optional[float] = None
    task: Optional[asyncio.Task] = None


_playback_jobs: Dict[str, PlaybackJob] = {}
_PLAYBACK_JOB_TTL_SECONDS = 300


def _prune_playback_jobs() -> None:
    cutoff = time.time() - _PLAYBACK_JOB_TTL_SECONDS
    expired = [
        request_id
        for request_id, job in _playback_jobs.items()
        if job.finished_at is not None and job.finished_at < cutoff
    ]
    for request_id in expired:
        _playback_jobs.pop(request_id, None)


def _playback_job_position(job: PlaybackJob) -> int:
    if job.state != "pending":
        return 0
    pending = [queued for queued in _playback_jobs.values() if queued.state == "pending"]
    try:
        return pending.index(job) + 1
    except ValueError:
        return 0


def _playback_job_payload(job: PlaybackJob) -> dict:
    status_url = f"/playback/requests/{job.request_id}"
    return {
        "status": job.state,
        "requestId": job.request_id,
        "queuePosition": _playback_job_position(job),
        "statusUrl": status_url,
        "cancelUrl": status_url,
        "playbackAccounts": _playback_pool.size,
        "activePlaybackRequests": _playback_pool.size - _playback_pool.available,
    }


def _pending_playback_response(job: PlaybackJob) -> JSONResponse:
    position = _playback_job_position(job)
    return JSONResponse(
        status_code=202,
        content=_playback_job_payload(job),
        headers={
            "Location": f"/playback/requests/{job.request_id}",
            "Retry-After": "1",
            "X-Playback-Queue-Position": str(position),
            "X-Playback-Request-Id": job.request_id,
        },
    )


async def _run_queued_playback_job(job: PlaybackJob) -> None:
    try:
        async with _playback_pool.lease() as cred:
            job.state = "processing"
            if job.operation is None:
                raise RuntimeError("Playback operation is missing")
            job.result = await job.operation(cred)
            job.state = "completed"
    except asyncio.CancelledError:
        job.state = "cancelled"
        raise
    except HTTPException as exc:
        job.state = "failed"
        job.error = str(exc.detail)
        job.error_status = exc.status_code
    except Exception:
        logger.exception("Queued playback request %s failed", job.request_id)
        job.state = "failed"
        job.error = "Playback request failed"
        job.error_status = 500
    finally:
        job.finished_at = time.time()
        job.operation = None


async def dispatch_playback_request(operation: Callable[[dict], Awaitable[Any]]):
    """Run immediately when possible; otherwise return a pollable 202 job."""
    cred = _playback_pool.try_acquire()
    if cred is not None:
        try:
            return await operation(cred)
        finally:
            _playback_pool.release(cred)

    _prune_playback_jobs()
    job = PlaybackJob(
        request_id=uuid.uuid4().hex,
        operation=operation,
        created_at=time.time(),
    )
    _playback_jobs[job.request_id] = job
    job.task = asyncio.create_task(_run_queued_playback_job(job))
    return _pending_playback_response(job)


async def get_http_client() -> httpx.AsyncClient:
    global _http_client, _http_client_proxy_url
    if _http_client is None:
        async with _http_client_lock:
            if _http_client is None:
                proxy_url = None
                if USE_PROXIES:
                    proxy_url = await get_working_proxy()
                    if not proxy_url and not FALLBACK_TO_DIRECT_CONNECTION:
                        raise HTTPException(status_code=503, detail="Service Unavailable")
                    elif not proxy_url and FALLBACK_TO_DIRECT_CONNECTION:
                        logger.warning("Could not find a working proxy, falling back to direct connection. HOST IP MAY BE EXPOSED!")
                _http_client = _build_http_client(proxy_url)
                _http_client_proxy_url = proxy_url
    return _http_client


async def refresh_tidal_token(cred: Optional[dict] = None):
    """Refresh a token for the provided credential set."""
    cred = cred or _pick_credential()

    async with _lock_for_cred(cred):
        if cred["access_token"] and time.time() < cred["expires_at"]:
            return cred["access_token"]

        if USE_PROXIES and ROTATE_PROXIES_ON_REFRESH:
            await update_global_client(force_new_proxy=True)

        max_retries = MAX_RETRIES if USE_PROXIES else 1
        for attempt in range(max_retries):
            try:
                client = await get_http_client()
                res = await client.post(
                    "https://auth.tidal.com/v1/oauth2/token",
                    data={
                        "client_id": cred["client_id"],
                        "refresh_token": cred["refresh_token"],
                        "grant_type": "refresh_token",
                        "scope": "r_usr+w_usr+w_sub",
                    },
                    auth=(cred["client_id"], cred["client_secret"]),
                )
                _log_response("POST", "https://auth.tidal.com/v1/oauth2/token", res)

                if res.status_code in [400, 401]:
                    try:
                        error_data = res.json()
                        if error_data.get("error") in ["invalid_client", "invalid_grant"]:
                            logger.error(f"Tidal Auth Error: {error_data}")
                            raise HTTPException(status_code=401, detail=f"Tidal Auth Error: {error_data.get('error_description')}")
                    except ValueError:
                        pass

                res.raise_for_status()
                data = res.json()
                new_token = data["access_token"]
                expires_in = data.get("expires_in", 3600)

                cred["access_token"] = new_token
                cred["expires_at"] = time.time() + expires_in - 60

                return new_token
            except httpx.RequestError as e:
                if USE_PROXIES and attempt < max_retries - 1:
                    logger.warning(f"Proxy failed during token refresh: {e}. Healing proxy...")
                    await update_global_client(force_new_proxy=True)
                    continue
                raise HTTPException(status_code=401, detail=f"Token refresh failed: {str(e)}")
            except httpx.HTTPStatusError as e:
                if USE_PROXIES and e.response.status_code in [403, 429] and attempt < max_retries - 1:
                    logger.warning(f"Proxy blocked during token refresh ({e.response.status_code}). Healing proxy...")
                    await update_global_client(force_new_proxy=True)
                    continue
                raise HTTPException(status_code=401, detail=f"Token refresh failed: {str(e)}")


async def get_tidal_token(force_refresh: bool = False):
    return await get_tidal_token_for_cred(force_refresh=force_refresh)


async def get_tidal_token_for_cred(force_refresh: bool = False, cred: Optional[dict] = None):
    """Retrieve an access token for a specific credential; pick random if not provided."""
    cred = cred or _pick_credential()

    if not force_refresh and cred["access_token"] and time.time() < cred["expires_at"]:
        return cred["access_token"], cred

    token = await refresh_tidal_token(cred)
    return token, cred


async def get_catalog_token_for_cred(force_refresh: bool = False, cred: Optional[dict] = None):
    """Return the unrestricted catalog credential, falling back for compatibility."""
    if cred is not None:
        return await get_tidal_token_for_cred(force_refresh=force_refresh, cred=cred)
    if _catalog_cred is not None:
        return await get_tidal_token_for_cred(force_refresh=force_refresh, cred=_catalog_cred)
    if CATALOG_TOKEN:
        return CATALOG_TOKEN, None
    return await get_tidal_token_for_cred(force_refresh=force_refresh)


async def _get_request_token(*, catalog: bool, force_refresh: bool = False, cred: Optional[dict] = None):
    if catalog:
        return await get_catalog_token_for_cred(force_refresh=force_refresh, cred=cred)
    return await get_tidal_token_for_cred(force_refresh=force_refresh, cred=cred)


async def make_request(
    url: str,
    token: Optional[str] = None,
    params: Optional[dict] = None,
    cred: Optional[dict] = None,
    *,
    catalog: bool = True,
):
    if token is None:
        token, cred = await _get_request_token(catalog=catalog, cred=cred)
    client = await get_http_client()
    headers = {"authorization": f"Bearer {token}"}

    try:
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            resp = await client.get(url, headers=headers, params=params)
            _log_response("GET", url, resp)

            if resp.status_code == 401:
                token, cred = await _get_request_token(catalog=catalog, force_refresh=True, cred=cred)
                headers = {"authorization": f"Bearer {token}"}
                resp = await client.get(url, headers=headers, params=params)
                _log_response("GET (retry after 401)", url, resp)

            if resp.status_code == 429 and attempt < _RATE_LIMIT_MAX_RETRIES:
                delay = min(_RATE_LIMIT_BASE_DELAY * (2 ** attempt), _RATE_LIMIT_MAX_DELAY)
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = min(delay, max(float(retry_after), 0))
                    except ValueError:
                        pass
                delay = min(delay, _RATE_LIMIT_MAX_DELAY)
                logger.warning("Upstream 429 for %s, retrying in %.1fs (attempt %d/%d)", url, delay, attempt + 1, _RATE_LIMIT_MAX_RETRIES)
                await asyncio.sleep(delay)
                continue

            if resp.status_code == 404:
                fresh_token, fresh_cred = await _get_request_token(catalog=catalog, force_refresh=True, cred=cred)
                if fresh_token != token:
                    headers = {"authorization": f"Bearer {fresh_token}"}
                    resp = await client.get(url, headers=headers, params=params)
                    _log_response("GET (retry after 404 token refresh)", url, resp)
                    token, cred = fresh_token, fresh_cred

            break

        resp.raise_for_status()
        return {"version": API_VERSION, "data": resp.json()}
    except httpx.HTTPStatusError as e:
        logger.error(
            "Upstream API error %s %s %s",
            e.response.status_code,
            url,
            e.response.text[:1000],
            exc_info=e,
        )
        raise HTTPException(status_code=e.response.status_code, detail="Upstream API error")
    except httpx.RequestError as e:
        if isinstance(e, httpx.TimeoutException):
            raise HTTPException(status_code=429, detail="Upstream timeout")
        raise HTTPException(status_code=503, detail="Connection error to Tidal")


async def authed_get_json(
    url: str,
    *,
    params: Optional[dict] = None,
    token: Optional[str] = None,
    cred: Optional[dict] = None,
    catalog: bool = True,
):
    """Perform an authenticated GET, retrying once on 401. Returns payload with updated token/cred."""

    if token is None:
        token, cred = await _get_request_token(catalog=catalog, cred=cred)

    client = await get_http_client()
    headers = {"authorization": f"Bearer {token}"}

    try:
        for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
            resp = await client.get(url, headers=headers, params=params)
            _log_response("GET", url, resp)

            if resp.status_code == 401:
                token, cred = await _get_request_token(catalog=catalog, force_refresh=True, cred=cred)
                headers["authorization"] = f"Bearer {token}"
                resp = await client.get(url, headers=headers, params=params)
                _log_response("GET (retry after 401)", url, resp)

            if resp.status_code == 429 and attempt < _RATE_LIMIT_MAX_RETRIES:
                delay = min(_RATE_LIMIT_BASE_DELAY * (2 ** attempt), _RATE_LIMIT_MAX_DELAY)
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = min(delay, max(float(retry_after), 0))
                    except ValueError:
                        pass
                delay = min(delay, _RATE_LIMIT_MAX_DELAY)
                logger.warning("Upstream 429 for %s, retrying in %.1fs (attempt %d/%d)", url, delay, attempt + 1, _RATE_LIMIT_MAX_RETRIES)
                await asyncio.sleep(delay)
                continue

            if resp.status_code == 404:
                fresh_token, fresh_cred = await _get_request_token(catalog=catalog, force_refresh=True, cred=cred)
                if fresh_token != token:
                    headers["authorization"] = f"Bearer {fresh_token}"
                    resp = await client.get(url, headers=headers, params=params)
                    _log_response("GET (retry after 404 token refresh)", url, resp)
                    token, cred = fresh_token, fresh_cred

            break

        resp.raise_for_status()
        return resp.json(), token, cred
    except httpx.HTTPStatusError as e:
        logger.error(
            "Upstream API error %s %s %s",
            e.response.status_code,
            url,
            e.response.text[:1000],
            exc_info=e,
        )
        raise HTTPException(status_code=e.response.status_code, detail="Upstream API error")
    except httpx.RequestError as e:
        if isinstance(e, httpx.TimeoutException):
            raise HTTPException(status_code=429, detail="Upstream timeout")
        raise HTTPException(status_code=503, detail="Connection error to Tidal")


async def make_playback_request_for_cred(cred: dict, url: str, params: Optional[dict] = None):
    token, cred = await get_tidal_token_for_cred(cred=cred)
    return await make_request(url, token=token, params=params, cred=cred, catalog=False)


async def make_playback_request(url: str, params: Optional[dict] = None):
    """Run one complete playback operation in an exclusive account slot."""
    async with _playback_pool.lease() as cred:
        return await make_playback_request_for_cred(cred, url, params=params)


@app.get("/playback/requests/{request_id}")
async def get_playback_request(request_id: str):
    """Poll a playback request that was accepted with HTTP 202."""
    _prune_playback_jobs()
    job = _playback_jobs.get(request_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Playback request not found or expired")

    if job.state in ("pending", "processing"):
        return _pending_playback_response(job)
    if job.state == "completed":
        return job.result
    if job.state == "cancelled":
        return JSONResponse(
            status_code=410,
            content={**_playback_job_payload(job), "detail": "Playback request was cancelled"},
        )
    return JSONResponse(
        status_code=job.error_status,
        content={**_playback_job_payload(job), "detail": job.error or "Playback request failed"},
    )


@app.delete("/playback/requests/{request_id}")
async def cancel_playback_request(request_id: str):
    """Cancel a pending or processing playback request and release its slot."""
    _prune_playback_jobs()
    job = _playback_jobs.get(request_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Playback request not found or expired")
    if job.state in ("pending", "processing") and job.task is not None:
        job.task.cancel()
        job.state = "cancelled"
        job.finished_at = time.time()
    return _playback_job_payload(job)


# --- ArchivePool & Lossless Stream Matcher Integration ---
ARCHIVEPOOL_API_URL = os.getenv("ARCHIVEPOOL_API_URL", "https://archivepool.vercel.app/api/sources")
ARCHIVEPOOL_API_KEY = os.getenv("ARCHIVEPOOL_API_KEY", "atp_43568fcd18cb7cfc3a9b093a4d811b93a4430b1988a5e830")
ARCHIVEPOOL_PREFIX = "enc:1:"
ARCHIVEPOOL_DOMAIN = b"archivepool-client:"
INNERTUBE_API_KEY = "AIzaSyC9XL3ZjWddXya6X74dJoCTL-WEYFDNX30"
CLIENT_VERSION = "1.20260825.00.00"
_last_pool_fetch_time: float = 0.0

def derive_archivepool_client_key(api_key: str) -> bytes:
    return hashlib.sha256(ARCHIVEPOOL_DOMAIN + api_key.encode("utf-8")).digest()

def decrypt_archivepool_value(val: Optional[str], client_key: bytes) -> Optional[str]:
    if not val or not isinstance(val, str) or not val.startswith(ARCHIVEPOOL_PREFIX):
        return val
    try:
        parts = val[len(ARCHIVEPOOL_PREFIX):].split(":")
        if len(parts) != 2:
            return val
        iv = base64.b64decode(parts[0])
        ct = base64.b64decode(parts[1])
        aesgcm = AESGCM(client_key)
        return aesgcm.decrypt(iv, ct, None).decode("utf-8")
    except Exception as e:
        logger.error(f"[ArchivePool] Decryption error: {e}")
        return val

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def clean_string(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"ft\.|feat\.|featuring|official|video|audio|lyrics|lyric|remastered|remaster", "", s)
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def string_similarity(s1: str, s2: str) -> float:
    str1 = clean_string(s1)
    str2 = clean_string(s2)
    if str1 == str2:
        return 1.0
    if str1 in str2 or str2 in str1:
        return 0.85
    set1 = set(str1.split())
    set2 = set(str2.split())
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0.0

def normalize_for_match(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def extract_video_id(input_str: Optional[str]) -> Optional[str]:
    if not input_str:
        return None
    if re.match(r"^[a-zA-Z0-9_-]{11}$", input_str):
        return input_str
    m = re.search(r"(?:youtu\.be/|youtube\.com/(?:embed/|v/|watch\?v=|watch\?.+&v=))([\w-]{11})", input_str)
    return m.group(1) if m else None

def _find_all(node: Any, key: str, results: Optional[list] = None) -> list:
    if results is None:
        results = []
    if not node or not isinstance(node, (dict, list)):
        return results
    if isinstance(node, list):
        for item in node:
            _find_all(item, key, results)
        return results
    if key in node:
        results.append(node[key])
    for k, v in node.items():
        if k != key:
            _find_all(v, key, results)
    return results

def _extract_runs_info(runs: list) -> tuple:
    artists = []
    album = None
    for run in runs or []:
        endpoint = run.get("navigationEndpoint", {}).get("browseEndpoint", {})
        browse_id = endpoint.get("browseId")
        if not browse_id:
            continue
        page_type = (
            endpoint.get("browseEndpointContextSupportedConfigs", {})
            .get("browseEndpointContextMusicConfig", {})
            .get("pageType")
        )
        if page_type == "MUSIC_PAGE_TYPE_ARTIST" or browse_id.startswith("UC"):
            artists.append({"name": run.get("text", ""), "browseId": browse_id})
        elif page_type == "MUSIC_PAGE_TYPE_ALBUM" or browse_id.startswith("MPRE"):
            album = {"name": run.get("text", ""), "browseId": browse_id}
    return artists, album

def _parse_duration_to_seconds(duration_str: str) -> int:
    if not duration_str:
        return 0
    if duration_str.isdigit():
        return int(duration_str)
    parts = duration_str.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return 0
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    elif len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    return 0

async def fetch_youtube_metadata(video_id: str) -> dict:
    client = await get_http_client()
    try:
        body = {
            "videoId": video_id,
            "isAudioOnly": True,
            "tunerSettingValue": "AUTOMIX_SETTING_NORMAL",
            "watchEndpointMusicSupportedConfigs": {
                "watchEndpointMusicConfig": {
                    "musicVideoType": "MUSIC_VIDEO_TYPE_ATV",
                },
            },
            "context": {
                "client": {
                    "clientName": "WEB_REMIX",
                    "clientVersion": CLIENT_VERSION,
                    "hl": "en",
                    "gl": "US",
                    "platform": "DESKTOP",
                },
                "user": {"lockedSafetyMode": False},
                "request": {"useSsl": True},
            },
        }
        res = await client.post(
            f"https://music.youtube.com/youtubei/v1/next?key={INNERTUBE_API_KEY}&prettyPrint=false",
            json=body,
            headers={
                "Content-Type": "application/json",
                "Origin": "https://music.youtube.com",
                "Referer": f"https://music.youtube.com/watch?v={video_id}",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
            timeout=8.0,
        )
        if res.status_code == 200:
            data = res.json()
            renderers = _find_all(data, "playlistPanelVideoRenderer")
            target_idx = next((i for i, r in enumerate(renderers) if r.get("videoId") == video_id), -1)
            if target_idx != -1:
                cur = renderers[target_idx]
                title = cur.get("title", {}).get("runs", [{}])[0].get("text", "")
                runs = cur.get("longBylineText", {}).get("runs") or cur.get("shortBylineText", {}).get("runs") or []
                artists, album = _extract_runs_info(runs)
                dur_text = cur.get("lengthText", {}).get("runs", [{}])[0].get("text", "")
                duration = _parse_duration_to_seconds(dur_text)
                artist_names = [a["name"] for a in artists]
                primary_artist = artist_names[0] if artist_names else (runs[0].get("text", "") if runs else "")
                return {
                    "videoId": video_id,
                    "title": title,
                    "artist": primary_artist,
                    "artists": artist_names,
                    "album": album["name"] if album else None,
                    "duration": duration,
                    "durationText": dur_text,
                    "thumbnails": cur.get("thumbnail", {}).get("thumbnails", []),
                }
    except Exception as e:
        logger.error(f"[Metadata] InnerTube /next error: {e}")

    try:
        oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
        res = await client.get(oembed_url, timeout=8.0)
        if res.status_code == 200:
            data = res.json()
            raw_title = data.get("title", "")
            raw_author = re.sub(r"\s*-\s*Topic$", "", data.get("author_name", ""), flags=re.I).strip()
            artist = raw_author
            title = raw_title
            if " - " in raw_title:
                parts = raw_title.split(" - ", 1)
                artist = parts[0].strip()
                title = parts[1].strip()
            return {
                "videoId": video_id,
                "title": title,
                "artist": artist,
                "artists": [artist],
                "album": None,
                "duration": 0,
            }
    except Exception as e:
        logger.error(f"[Metadata] oEmbed error: {e}")

    raise HTTPException(status_code=404, detail=f"Failed to fetch YouTube Music metadata for video ID: {video_id}")

_tidal_pool_accounts: List[dict] = []

async def fetch_archivepool_sources() -> dict:
    global _last_pool_fetch_time, _tidal_pool_accounts
    try:
        logger.info("[ArchivePool] Fetching Tidal accounts from archivepool...")
        _last_pool_fetch_time = time.time()
        client = await get_http_client()
        res = await client.get(
            ARCHIVEPOOL_API_URL,
            headers={
                "Authorization": f"Bearer {ARCHIVEPOOL_API_KEY}",
                "X-Pool-Client": "v2",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json",
            },
            timeout=8.0,
        )
        if res.status_code != 200:
            logger.error(f"[ArchivePool] API returned HTTP {res.status_code}")
            return {}
        data = res.json()
        client_key = derive_archivepool_client_key(ARCHIVEPOOL_API_KEY)

        # Tidal accounts integration
        tidal_accs = data.get("tidal", {}).get("accounts", [])
        parsed_accs = []
        for acc in tidal_accs:
            if acc.get("status") == "dead":
                continue
            token = decrypt_archivepool_value(acc.get("token"), client_key)
            ref_token = decrypt_archivepool_value(acc.get("refreshToken"), client_key)
            country = decrypt_archivepool_value(acc.get("countryCode"), client_key) or COUNTRY_CODE or "US"
            user_id = decrypt_archivepool_value(acc.get("userId"), client_key) or str(acc.get("id"))
            entry = {
                "id": acc.get("id"),
                "client_id": CLIENT_ID or "zU4XHVVkc2tDPo4t",
                "client_secret": CLIENT_SECRET or "VJKhDFqJPqvsZ2BgVWmdaDilPhuad8xNgTVFlAzfqQc=",
                "refresh_token": ref_token,
                "user_id": user_id,
                "access_token": token,
                "expires_at": time.time() + 86400 if token else 0,
                "country_code": country,
                "source": "archivepool",
            }
            parsed_accs.append(entry)

            if ref_token and not any(c.get("refresh_token") == ref_token for c in _creds):
                _creds.append(entry)
                _playback_pool.add_credential(entry)

        _tidal_pool_accounts = parsed_accs
        logger.info(f"[ArchivePool] Parsed {len(parsed_accs)} candidate Tidal account(s)")
        return {"tidal": parsed_accs}
    except Exception as e:
        logger.error(f"[ArchivePool] Error fetching from archivepool: {e}")
        return {}

async def search_tidal_tracks(query: str, limit: int = 10, country_code: Optional[str] = None) -> list:
    cc = country_code or COUNTRY_CODE or "US"
    search_url = "https://api.tidal.com/v1/search/tracks"
    params = {"query": query, "limit": limit, "offset": 0, "countryCode": cc}
    try:
        res = await make_request(search_url, params=params, catalog=True)
        if isinstance(res, dict) and "data" in res:
            return res["data"].get("items", [])
    except Exception as e:
        logger.warning(f"[TidalSearch] Error searching: {e}")

    try:
        token, _ = await get_tidal_token_for_cred()
        client = await get_http_client()
        r = await client.get(search_url, params=params, headers={"authorization": f"Bearer {token}", **_TIDAL_DEFAULT_HEADERS})
        if r.status_code == 200:
            return r.json().get("items", [])
    except Exception:
        pass
    return []


def find_best_tidal_match(yt_meta: dict, candidates: list) -> Optional[dict]:
    if not candidates:
        return None
    target_title = normalize_for_match(yt_meta.get("title", ""))
    target_artist = normalize_for_match(yt_meta.get("artist", ""))
    yt_dur = yt_meta.get("duration", 0)

    best_match = None
    highest_score = -1.0

    for track in candidates:
        t_title = normalize_for_match(track.get("title", ""))
        art = track.get("artist", {}) or {}
        t_artist = normalize_for_match(art.get("name") or "")
        t_duration = track.get("duration", 0)

        title_sim = string_similarity(target_title, t_title)
        is_title_contained = target_title in t_title or t_title in target_title
        if title_sim < 0.4 and not is_title_contained:
            continue

        artist_sim = 0.0
        if target_artist and t_artist:
            artist_sim = string_similarity(target_artist, t_artist)
            is_artist_contained = target_artist in t_artist or t_artist in target_artist
            if artist_sim < 0.35 and not is_artist_contained:
                continue

        duration_score = 1.0
        if yt_dur > 0 and t_duration > 0:
            diff = abs(yt_dur - t_duration)
            if diff > 4:
                continue
            duration_score = 1.0 if diff == 0 else (1.0 - (diff * 0.05))

        total_score = (title_sim * 0.45) + (artist_sim * 0.35) + (duration_score * 0.20)
        if total_score > highest_score:
            highest_score = total_score
            best_match = track

    return best_match or (candidates[0] if candidates else None)


FFMPEG_BIN = "/opt/homebrew/bin/ffmpeg" if os.path.exists("/opt/homebrew/bin/ffmpeg") else "ffmpeg"


async def get_tidal_hls_manifest_url(track_id: int) -> Optional[str]:
    for acc in _tidal_pool_accounts:
        token = acc.get("access_token")
        if not token:
            continue
        try:
            url = f"https://openapi.tidal.com/v2/trackManifests/{track_id}?adaptive=true&manifestType=HLS&uriScheme=HTTPS&usage=PLAYBACK&formats=FLAC&formats=FLAC_HIRES"
            client = await get_http_client()
            res = await client.get(url, headers={"Authorization": f"Bearer {token}", **_TIDAL_DEFAULT_HEADERS}, timeout=8.0)
            if res.status_code == 200:
                data = res.json()
                uri = data.get("data", {}).get("attributes", {}).get("uri")
                if uri:
                    return uri
        except Exception:
            continue
    return None


async def stream_flac_generator(manifest_url: str, map_stream: str = "0:0") -> AsyncIterator[bytes]:
    ffmpeg_cmd = [
        FFMPEG_BIN,
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", manifest_url,
        "-map", map_stream,
        "-c:a", "copy",
        "-f", "flac",
        "-",
    ]
    proc = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        while True:
            chunk = await proc.stdout.read(65536)
            if not chunk:
                break
            yield chunk
    except (asyncio.CancelledError, GeneratorExit):
        pass
    finally:
        try:
            proc.kill()
            await proc.wait()
        except Exception:
            pass


@app.get("/stream/{track_id}.flac")
@app.get("/stream/{track_id}")
async def stream_flac_audio(track_id: str, quality: str = "LOSSLESS"):
    try:
        clean_id = int(str(track_id).removesuffix(".flac"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid track ID format")
    manifest_url = await get_tidal_hls_manifest_url(clean_id)
    if not manifest_url:
        raise HTTPException(status_code=404, detail="Could not obtain FLAC stream manifest from Tidal")
    map_stream = "0:1" if "HI_RES" in quality.upper() else "0:0"
    return StreamingResponse(
        stream_flac_generator(manifest_url, map_stream),
        media_type="audio/flac",
        headers={
            "Content-Type": "audio/flac",
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=86400",
        },
    )


async def get_tidal_stream_url(track_id: int, quality: str = "HI_RES_LOSSLESS") -> dict:
    track_url = f"https://api.tidal.com/v1/tracks/{track_id}/playbackinfo"
    params = {
        "audioquality": quality,
        "playbackmode": "STREAM",
        "assetpresentation": "FULL",
    }
    try:
        if not _creds:
            await fetch_archivepool_sources()

        async with _playback_pool.lease() as cred:
            token, cred = await get_tidal_token_for_cred(cred=cred)
            client = await get_http_client()
            res = await client.get(
                track_url,
                params=params,
                headers={"authorization": f"Bearer {token}", **_TIDAL_DEFAULT_HEADERS},
                timeout=10.0,
            )
            if res.status_code == 401:
                token, cred = await get_tidal_token_for_cred(force_refresh=True, cred=cred)
                res = await client.get(
                    track_url,
                    params=params,
                    headers={"authorization": f"Bearer {token}", **_TIDAL_DEFAULT_HEADERS},
                    timeout=10.0,
                )
            if res.status_code == 200:
                pb = res.json()
                mime = pb.get("manifestMimeType", "")
                manifest_b64 = pb.get("manifest")
                audio_quality = pb.get("audioQuality", quality)
                if manifest_b64 and "bts" in mime:
                    try:
                        m_data = json.loads(base64.b64decode(manifest_b64).decode("utf-8"))
                        urls = m_data.get("urls", [])
                        if urls:
                            return {
                                "error": False,
                                "quality": audio_quality,
                                "mime_type": m_data.get("mimeType", "audio/mp4"),
                                "codec": m_data.get("codecs", ""),
                                "url": urls[0],
                                "bit_depth": 24 if "HI_RES" in audio_quality else (16 if "LOSSLESS" in audio_quality else None),
                                "sample_rate": 96 if "HI_RES" in audio_quality else (44.1 if "LOSSLESS" in audio_quality else None),
                            }
                    except Exception as e:
                        logger.error(f"[Playback] Manifest decode error: {e}")
    except Exception as e:
        logger.warning(f"[Playback] Lease attempt error: {e}")

    for acc in _tidal_pool_accounts:
        token = acc.get("access_token")
        if not token:
            continue
        try:
            client = await get_http_client()
            res = await client.get(
                track_url,
                params=params,
                headers={"authorization": f"Bearer {token}", **_TIDAL_DEFAULT_HEADERS},
                timeout=10.0,
            )
            if res.status_code == 200:
                pb = res.json()
                mime = pb.get("manifestMimeType", "")
                manifest_b64 = pb.get("manifest")
                audio_quality = pb.get("audioQuality", quality)
                if manifest_b64 and "bts" in mime:
                    m_data = json.loads(base64.b64decode(manifest_b64).decode("utf-8"))
                    urls = m_data.get("urls", [])
                    if urls:
                        return {
                            "error": False,
                            "quality": audio_quality,
                            "mime_type": m_data.get("mimeType", "audio/mp4"),
                            "codec": m_data.get("codecs", ""),
                            "url": urls[0],
                            "bit_depth": 24 if "HI_RES" in audio_quality else (16 if "LOSSLESS" in audio_quality else None),
                            "sample_rate": 96 if "HI_RES" in audio_quality else (44.1 if "LOSSLESS" in audio_quality else None),
                        }
        except Exception:
            continue

    return {
        "error": True,
        "status": 500,
        "message": "Failed to fetch stream URL from Tidal",
    }


async def handle_youtube_stream_match(request: Request, video_input: str) -> JSONResponse:
    if video_input.isdigit():
        track_id = int(video_input)
        base_url = str(request.base_url).rstrip("/")
        hls_manifest_url = await get_tidal_hls_manifest_url(track_id)
        flac_url = f"{base_url}/stream/{track_id}.flac"
        stream_res = await get_tidal_stream_url(track_id, "HI_RES_LOSSLESS")
        return JSONResponse(
            status_code=200,
            content={
                "source": "tidal",
                "trackId": track_id,
                "stream_url": flac_url if hls_manifest_url else stream_res.get("url"),
                "flac_url": flac_url if hls_manifest_url else None,
                "manifest_url": hls_manifest_url,
                "stream": stream_res,
            },
        )

    video_id = extract_video_id(video_input)
    if not video_id:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": "Missing or invalid YouTube video ID/URL. Usage: /?id={videoId} or /?url={youtubeUrl}",
            },
        )

    try:
        yt_meta = await fetch_youtube_metadata(video_id)
        search_query = f"{yt_meta['artist']} {yt_meta['title']}"

        if not _creds or len(_creds) == 0:
            await fetch_archivepool_sources()

        candidates = await search_tidal_tracks(search_query, 10)
        if not candidates:
            candidates = await search_tidal_tracks(yt_meta["title"], 10)

        matched_track = find_best_tidal_match(yt_meta, candidates) if candidates else None

        raw_results = [
            {
                "id": t.get("id"),
                "title": t.get("title"),
                "artist": t.get("artist", {}).get("name") or "Unknown",
                "album": t.get("album", {}).get("title") or "Unknown Album",
                "duration": t.get("duration", 0),
                "audioQuality": t.get("audioQuality"),
            }
            for t in (candidates or [])
        ]

        if not matched_track:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "message": "No matching track found on Tidal catalog.",
                    "ytmusic": yt_meta,
                    "raw_results": raw_results,
                },
            )

        # 1. Fetch HLS FLAC playlist from OpenAPI trackManifests
        hls_manifest_url = await get_tidal_hls_manifest_url(matched_track["id"])
        base_url = str(request.base_url).rstrip("/")
        flac_stream_url = f"{base_url}/stream/{matched_track['id']}.flac"
        flac_hires_stream_url = f"{base_url}/stream/{matched_track['id']}.flac?quality=HI_RES_LOSSLESS"

        streams = {}
        if hls_manifest_url:
            streams["flac"] = {
                "label": "Lossless FLAC (16-Bit / 44.1 kHz CD Quality)",
                "quality": "LOSSLESS",
                "mime_type": "audio/flac",
                "codec": "flac",
                "bit_depth": 16,
                "sample_rate": 44100,
                "url": flac_stream_url,
                "manifest_url": hls_manifest_url,
            }
            if matched_track.get("audioQuality") in ("HI_RES_LOSSLESS", "HI_RES", "LOSSLESS"):
                streams["flac_hires"] = {
                    "label": "Hi-Res Lossless FLAC (24-Bit / up to 96 kHz)",
                    "quality": "HI_RES_LOSSLESS",
                    "mime_type": "audio/flac",
                    "codec": "flac",
                    "bit_depth": 24,
                    "sample_rate": 96000,
                    "url": flac_hires_stream_url,
                    "manifest_url": hls_manifest_url,
                }

        # 2. Also fetch AAC fallback streams
        qualities_to_fetch = [
            {"key": "high", "quality": "HIGH", "label": "High Quality (AAC 320 kbps)"},
            {"key": "low", "quality": "LOW", "label": "Low Quality (AAC 96 kbps)"},
        ]
        results = await asyncio.gather(*[
            get_tidal_stream_url(matched_track["id"], q["quality"]) for q in qualities_to_fetch
        ], return_exceptions=True)

        for q, res in zip(qualities_to_fetch, results):
            if isinstance(res, dict) and not res.get("error"):
                streams[q["key"]] = {"label": q["label"], **res}

        primary_stream = flac_stream_url if hls_manifest_url else None
        if not primary_stream:
            for k in ["high", "low"]:
                if k in streams and streams[k].get("url"):
                    primary_stream = streams[k]["url"]
                    break

        response_payload = {
            "status": "success" if streams else "error",
            "source": "tidal",
            "ytmusic": {
                "videoId": yt_meta.get("videoId"),
                "title": yt_meta.get("title"),
                "artist": yt_meta.get("artist"),
                "duration": yt_meta.get("duration"),
            },
            "matched_track": {
                "id": matched_track.get("id"),
                "title": matched_track.get("title"),
                "artist": matched_track.get("artist", {}).get("name") or "Unknown",
                "album": matched_track.get("album", {}).get("title") or "Unknown Album",
                "duration": matched_track.get("duration"),
                "audioQuality": matched_track.get("audioQuality"),
                "audioModes": matched_track.get("audioModes", []),
                "isrc": matched_track.get("isrc"),
                "url": f"https://tidal.com/browse/track/{matched_track.get('id')}",
            },
            "stream_url": primary_stream,
            "streams": streams,
            "raw_results": raw_results,
        }

        return JSONResponse(
            status_code=200,
            content=response_payload,
            headers={"Cache-Control": "public, max-age=3600"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Processing error: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(e) or "An unexpected error occurred while processing request.",
            },
        )


@app.get("/")
async def index(
    request: Request,
    id: Optional[str] = Query(default=None),
    videoId: Optional[str] = Query(default=None),
    url: Optional[str] = Query(default=None),
):
    video_input = id or videoId or url
    if not video_input:
        return {"version": API_VERSION, "Repo": "https://github.com/binimum/hifi-api"}
    return await handle_youtube_stream_match(request, video_input)


@app.get("/flac")
@app.get("/stream")
async def get_stream_route(
    request: Request,
    id: Optional[str] = Query(default=None),
    videoId: Optional[str] = Query(default=None),
    url: Optional[str] = Query(default=None),
):
    video_input = id or videoId or url
    if not video_input:
        raise HTTPException(status_code=400, detail="Missing id/videoId/url parameter")
    return await handle_youtube_stream_match(request, video_input)

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

@app.get("/info/")
async def get_info(id: int):
    url = f"https://api.tidal.com/v1/tracks/{id}/"
    return await make_request(url, params={"countryCode": COUNTRY_CODE})

@app.get("/track/")
async def get_track(id: int, quality: str = "HI_RES_LOSSLESS", immersiveaudio: bool = False):
    track_url = f"https://api.tidal.com/v1/tracks/{id}/playbackinfo"
    params = {
        "audioquality": quality,
        "playbackmode": "STREAM",
        "assetpresentation": "FULL",
        "immersiveaudio": immersiveaudio
    }
    return await dispatch_playback_request(
        lambda cred: make_playback_request_for_cred(cred, track_url, params=params)
    )


@app.get("/trackManifests/")
async def get_track_manifests(
    id: str,
    request: Request,
    formats: List[str] = Query(default=["HEAACV1", "AACLC", "FLAC", "FLAC_HIRES", "EAC3_JOC"]),
    adaptive: str = Query(default="true"),
    manifestType: str = Query(default="MPEG_DASH"),
    uriScheme: str = Query(default="HTTPS"),
    usage: str = Query(default="PLAYBACK")
):
    url = f"https://openapi.tidal.com/v2/trackManifests/{id}"
    params = [
        ("adaptive", adaptive),
        ("manifestType", manifestType),
        ("uriScheme", uriScheme),
        ("usage", usage),
    ]
    for f in formats:
        params.append(("formats", f))

    async def fetch_manifest(cred: dict):
        res = await make_playback_request_for_cred(cred, url, params=params)
        try:
            drm_data = res["data"]["data"]["attributes"]["drmData"]
            if drm_data:
                proxy_url = str(request.base_url).rstrip("/") + "/widevine"
                drm_data["licenseUrl"] = proxy_url
                drm_data["certificateUrl"] = proxy_url
        except (KeyError, TypeError):
            pass
        return res

    return await dispatch_playback_request(fetch_manifest)

# Not really necessary but I'm including it anyway
@app.api_route("/widevine", methods=["GET", "POST"])
async def widevine_proxy(request: Request):
    client = await get_http_client()
    body = await request.body()
    url = "https://api.tidal.com/v2/widevine"

    async def fetch_license(cred: dict):
        token, cred = await get_tidal_token_for_cred(cred=cred)
        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": request.headers.get("Content-Type", "application/octet-stream")
        }

        try:
            resp = await client.request(request.method, url, headers=headers, content=body)
            _log_response(request.method, url, resp)

            if resp.status_code == 401:
                token, cred = await get_tidal_token_for_cred(force_refresh=True, cred=cred)
                headers["authorization"] = f"Bearer {token}"
                resp = await client.request(request.method, url, headers=headers, content=body)
                _log_response(f"{request.method} (retry)", url, resp)

            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers={"Content-Type": resp.headers.get("Content-Type", "application/json")}
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail="Error communicating with widevine server") from e

    return await dispatch_playback_request(fetch_license)


@app.get("/recommendations/")
async def get_recommendations(id: int):
    recommendations_url = f"https://api.tidal.com/v1/tracks/{id}/recommendations"
    params = {"limit": "20", "countryCode": COUNTRY_CODE}
    return await make_request(recommendations_url, params=params)


@app.api_route("/search/", methods=["GET"])
async def search(
    s: Union[str, None] = Query(default=None),
    a: Union[str, None] = Query(default=None),
    al: Union[str, None] = Query(default=None),
    v: Union[str, None] = Query(default=None),
    p: Union[str, None] = Query(default=None),
    i: Union[str, None] = Query(default=None, description="ISRC query"),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=25, ge=1, le=500),
):
    """Search endpoint supporting track/artist/album/video/playlist queries via distinct params."""
    isrc_query = i.strip() if isinstance(i, str) else None
    if isrc_query:
        return await make_request(
            "https://api.tidal.com/v1/tracks",
            params={
                "isrc": isrc_query,
                "limit": limit,
                "offset": offset,
                "countryCode": COUNTRY_CODE,
            },
        )

    queries = (
        (s, "https://api.tidal.com/v1/search/tracks", {
            "query": s,
            "limit": limit,
            "offset": offset,
            "countryCode": COUNTRY_CODE,
        }),
        (a, "https://api.tidal.com/v1/search/top-hits", {
            "query": a,
            "limit": limit,
            "offset": offset,
            "types": "ARTISTS,TRACKS",
            "countryCode": COUNTRY_CODE,
        }),
        (al, "https://api.tidal.com/v1/search/top-hits", {
            "query": al,
            "limit": limit,
            "offset": offset,
            "types": "ALBUMS",
            "countryCode": COUNTRY_CODE,
        }),
        (v, "https://api.tidal.com/v1/search/top-hits", {
            "query": v,
            "limit": limit,
            "offset": offset,
            "types": "VIDEOS",
            "countryCode": COUNTRY_CODE,
        }),
        (p, "https://api.tidal.com/v1/search/top-hits", {
            "query": p,
            "limit": limit,
            "offset": offset,
            "types": "PLAYLISTS",
            "countryCode": COUNTRY_CODE,
        }),
    )

    for value, url, params in queries:
        if value:
            return await make_request(url, params=params)

    raise HTTPException(status_code=400, detail="Provide one of s, a, al, v, p, or i")

@app.get("/album/")
async def get_album(
    id: int = Query(..., description="Album ID"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    token, cred = await get_catalog_token_for_cred()

    album_url = f"https://api.tidal.com/v1/albums/{id}"
    items_url = f"https://api.tidal.com/v1/albums/{id}/items"

    async def fetch(url: str, params: Optional[dict] = None):
        payload, _, _ = await authed_get_json(
            url,
            params=params,
            token=token,
            cred=cred,
        )
        return payload

    tasks = [fetch(album_url, {"countryCode": COUNTRY_CODE})]

    max_chunk = 100
    current_offset = offset
    remaining_limit = limit

    while remaining_limit > 0:
        chunk_size = min(remaining_limit, max_chunk)
        tasks.append(
            fetch(items_url, {"countryCode": COUNTRY_CODE, "limit": chunk_size, "offset": current_offset})
        )
        current_offset += chunk_size
        remaining_limit -= chunk_size

    results = await asyncio.gather(*tasks)

    album_data = results[0]
    items_pages = results[1:]

    all_items = []
    for page in items_pages:
        page_items = page.get("items", page) if isinstance(page, dict) else page
        if isinstance(page_items, list):
            all_items.extend(page_items)

    album_data["items"] = all_items

    return {
        "version": API_VERSION,
        "data": album_data,
    }


@app.get("/mix/")
async def get_mix(
    id: str = Query(..., description="Mix ID")
):
    """Fetch items from a Tidal mix by its ID."""
    token, cred = await get_catalog_token_for_cred()
    url = "https://api.tidal.com/v1/pages/mix"
    params = {
        "mixId": id,
        "countryCode": COUNTRY_CODE,
        "deviceType": "BROWSER",
    }

    data, _, _ = await authed_get_json(
        url,
        params=params,
        token=token,
        cred=cred,
    )

    header = {}
    items = []

    rows = data.get("rows", [])
    for row in rows:
        modules = row.get("modules", [])
        for module in modules:
            if module.get("type") == "MIX_HEADER":
                header = module.get("mix", {})
            elif module.get("type") == "TRACK_LIST":
                paged_list = module.get("pagedList", {})
                items = paged_list.get("items", [])

    return {
        "version": API_VERSION,
        "mix": header,
        "items": [item.get("item", item) if isinstance(item, dict) else item for item in items],
    }


@app.get("/playlist/")
async def get_playlist(
    id: str = Query(..., min_length=1),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Fetch playlist metadata plus items concurrently, using shared client and single token."""

    token, cred = await get_catalog_token_for_cred()

    playlist_url = f"https://api.tidal.com/v1/playlists/{id}"
    items_url = f"https://api.tidal.com/v1/playlists/{id}/items"

    async def fetch(url: str, params: Optional[dict] = None):
        payload, _, _ = await authed_get_json(
            url,
            params=params,
            token=token,
            cred=cred,
        )
        return payload

    playlist_data, items_data = await asyncio.gather(
        fetch(playlist_url, {"countryCode": COUNTRY_CODE}),
        fetch(items_url, {"countryCode": COUNTRY_CODE, "limit": limit, "offset": offset}),
    )

    return {
        "version": API_VERSION,
        "playlist": playlist_data,
        "items": items_data.get("items", items_data) if isinstance(items_data, dict) else items_data,
    }


def _extract_uuid_from_tidal_url(href: str) -> Optional[str]:
    """Extract and reconstruct a hyphenated UUID from a Tidal resource URL."""
    parts = href.split("/") if href else []
    return "-".join(parts[4:9]) if len(parts) >= 9 else None


@app.get("/artist/similar/")
async def get_similar_artists(
    id: int = Query(..., description="Artist ID"),
    cursor: Union[int, str, None] = None
):
    """Fetch artists similar to another by its ID using V2 API."""
    url = f"https://openapi.tidal.com/v2/artists/{id}/relationships/similarArtists"
    params = {
        "page[cursor]": cursor,
        "countryCode": COUNTRY_CODE,
        "include": "similarArtists,similarArtists.profileArt"
    }

    payload, _, _ = await authed_get_json(url, params=params)
    included = payload.get("included", [])
    artists_map = {i["id"]: i for i in included if i["type"] == "artists"}
    artworks_map = {i["id"]: i for i in included if i["type"] == "artworks"}

    def resolve_artist(entry):
        aid = entry["id"]
        inc = artists_map.get(aid, {})
        attr = inc.get("attributes", {})

        pic_id = None
        if art_data := inc.get("relationships", {}).get("profileArt", {}).get("data"):
            if artwork := artworks_map.get(art_data[0].get("id")):
                if files := artwork.get("attributes", {}).get("files"):
                    pic_id = _extract_uuid_from_tidal_url(files[0].get("href"))

        return {
            **attr,
            "id": int(aid) if str(aid).isdigit() else aid,
            "picture": pic_id or attr.get("selectedAlbumCoverFallback"),
            "url": f"http://www.tidal.com/artist/{aid}",
            "relationType": "SIMILAR_ARTIST"
        }

    return {
        "version": API_VERSION,
        "artists": [resolve_artist(e) for e in payload.get("data", [])]
    }


@app.get("/album/similar/")
async def get_similar_albums(
    id: int = Query(..., description="Album ID"),
    cursor: Union[int, str, None] = None
):
    """Fetch albums similar to another by its ID using V2 API."""
    url = f"https://openapi.tidal.com/v2/albums/{id}/relationships/similarAlbums"
    params = {
        "page[cursor]": cursor,
        "countryCode": COUNTRY_CODE,
        "include": "similarAlbums,similarAlbums.coverArt,similarAlbums.artists"
    }

    payload, _, _ = await authed_get_json(url, params=params)
    included = payload.get("included", [])
    albums_map = {i["id"]: i for i in included if i["type"] == "albums"}
    artworks_map = {i["id"]: i for i in included if i["type"] == "artworks"}
    artists_map = {i["id"]: i for i in included if i["type"] == "artists"}

    def resolve_album(entry):
        aid = entry["id"]
        inc = albums_map.get(aid, {})
        attr = inc.get("attributes", {})

        cover_id = None
        if art_data := inc.get("relationships", {}).get("coverArt", {}).get("data"):
            if artwork := artworks_map.get(art_data[0].get("id")):
                if files := artwork.get("attributes", {}).get("files"):
                    cover_id = _extract_uuid_from_tidal_url(files[0].get("href"))

        artist_list = []
        if art_data := inc.get("relationships", {}).get("artists", {}).get("data"):
             for a_entry in art_data:
                 if a_obj := artists_map.get(a_entry["id"]):
                     a_id = a_obj["id"]
                     artist_list.append({
                         "id": int(a_id) if str(a_id).isdigit() else a_id,
                         "name": a_obj["attributes"]["name"]
                     })

        return {
            **attr,
            "id": int(aid) if str(aid).isdigit() else aid,
            "cover": cover_id,
            "artists": artist_list,
            "url": f"http://www.tidal.com/album/{aid}"
        }

    return {
        "version": API_VERSION,
        "albums": [resolve_album(e) for e in payload.get("data", [])]
    }


@app.get("/artist/")
async def get_artist(
    id: Optional[int] = Query(default=None),
    f: Optional[int] = Query(default=None),
    skip_tracks: bool = Query(default=False),
):
    """Artist detail or album+track aggregation.

    - id: basic artist metadata + cover URLs
    - f: fetch artist albums page and aggregate tracks across albums (capped concurrency)
    - skip_tracks: if true, returns only albums without aggregating tracks (when using 'f')
    """

    if id is None and f is None:
        raise HTTPException(status_code=400, detail="Provide id or f query param")

    token, cred = await get_catalog_token_for_cred()

    if id is not None:
        artist_url = f"https://api.tidal.com/v1/artists/{id}"
        artist_data, token, cred = await authed_get_json(
            artist_url,
            params={"countryCode": COUNTRY_CODE},
            token=token,
            cred=cred,
        )

        picture = artist_data.get("picture")
        fallback = artist_data.get("selectedAlbumCoverFallback")

        if not picture and fallback:
            artist_data["picture"] = fallback
            picture = fallback

        cover = None
        if picture:
            slug = picture.replace("-", "/")
            cover = {
                "id": artist_data.get("id"),
                "name": artist_data.get("name"),
                "750": f"https://resources.tidal.com/images/{slug}/750x750.jpg",
            }

        return {"version": API_VERSION, "artist": artist_data, "cover": cover}

    # Fetch albums and singles/EPs directly in parallel
    albums_url = f"https://api.tidal.com/v1/artists/{f}/albums"
    common_params = {"countryCode": COUNTRY_CODE, "limit": 100}

    tasks = [
        authed_get_json(albums_url, params=common_params, token=token, cred=cred),
        authed_get_json(albums_url, params={**common_params, "filter": "EPSANDSINGLES"}, token=token, cred=cred),
    ]

    if skip_tracks:
        tasks.append(
            authed_get_json(
                f"https://api.tidal.com/v1/artists/{f}/toptracks",
                params={"countryCode": COUNTRY_CODE, "limit": 15},
                token=token,
                cred=cred
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    unique_releases = []
    seen_ids = set()

    # Process albums (first 2 results)
    for res in results[:2]:
        if isinstance(res, tuple) and len(res) > 0:
            data = res[0]
            items = data.get("items", []) if isinstance(data, dict) else data
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and item.get("id") and item["id"] not in seen_ids:
                        unique_releases.append(item)
                        seen_ids.add(item["id"])
        elif isinstance(res, Exception):
            logger.warning("Error fetching artist releases: %s", res)

    album_ids: List[int] = [item["id"] for item in unique_releases]
    page_data = {"items": unique_releases}

    if skip_tracks:
        top_tracks = []
        if len(results) > 2:
            res = results[2]
            if isinstance(res, tuple) and len(res) > 0:
                data = res[0]
                top_tracks = data.get("items", []) if isinstance(data, dict) else data
            elif isinstance(res, Exception):
                logger.warning("Error fetching top tracks: %s", res)

        return {"version": API_VERSION, "albums": page_data, "tracks": top_tracks}

    if not album_ids:
        return {"version": API_VERSION, "albums": page_data, "tracks": []}

    async def fetch_album_tracks(album_id: int):
        async with _album_tracks_sem:
            album_data, _, _ = await authed_get_json(
                "https://api.tidal.com/v1/pages/album",
                params={
                    "albumId": album_id,
                    "countryCode": COUNTRY_CODE,
                    "deviceType": "BROWSER",
                },
                token=token,
                cred=cred,
            )

            rows = album_data.get("rows", [])
            if len(rows) < 2:
                return []
            modules = rows[1].get("modules", [])
            if not modules:
                return []
            paged_list = modules[0].get("pagedList", {})
            items = paged_list.get("items", [])
            tracks = [track.get("item", track) if isinstance(track, dict) else track for track in items]
            return tracks

    results = await asyncio.gather(
        *(fetch_album_tracks(album_id) for album_id in album_ids),
        return_exceptions=True,
    )

    tracks: List[dict] = []
    for res in results:
        if isinstance(res, Exception):
            continue
        tracks.extend(res)

    return {"version": API_VERSION, "albums": page_data, "tracks": tracks}


@app.get("/cover/")
async def get_cover(
    id: Optional[int] = Query(default=None),
    q: Optional[str] = Query(default=None),
):
    """Fetch album cover data for a track id or search query."""

    if id is None and q is None:
        raise HTTPException(status_code=400, detail="Provide id or q query param")

    token, cred = await get_catalog_token_for_cred()

    def build_cover_entry(cover_slug: str, name: Optional[str], track_id: Optional[int]):
        slug = cover_slug.replace("-", "/")
        return {
            "id": track_id,
            "name": name,
            "1280": f"https://resources.tidal.com/images/{slug}/1280x1280.jpg",
            "640": f"https://resources.tidal.com/images/{slug}/640x640.jpg",
            "80": f"https://resources.tidal.com/images/{slug}/80x80.jpg",
        }

    if id is not None:
        track_data, token, cred = await authed_get_json(
            f"https://api.tidal.com/v1/tracks/{id}/",
            params={"countryCode": COUNTRY_CODE},
            token=token,
            cred=cred,
        )

        album = track_data.get("album") or {}
        cover_slug = album.get("cover")
        if not cover_slug:
            raise HTTPException(status_code=404, detail="Cover not found")

        entry = build_cover_entry(
            cover_slug,
            album.get("title") or track_data.get("title"),
            album.get("id") or id,
        )
        return {"version": API_VERSION, "covers": [entry]}

    search_data, token, cred = await authed_get_json(
        "https://api.tidal.com/v1/search/tracks",
        params={"countryCode": COUNTRY_CODE, "query": q, "limit": 10},
        token=token,
        cred=cred,
    )

    items = search_data.get("items", [])[:10]
    if not items:
        raise HTTPException(status_code=404, detail="Cover not found")

    covers = []
    for track in items:
        album = track.get("album") or {}
        cover_slug = album.get("cover")
        if not cover_slug:
            continue
        covers.append(
            build_cover_entry(
                cover_slug,
                track.get("title"),
                track.get("id"),
            )
        )

    if not covers:
        raise HTTPException(status_code=404, detail="Cover not found")

    return {"version": API_VERSION, "covers": covers}


@app.get("/lyrics/")
async def get_lyrics(id: int):
    url = f"https://api.tidal.com/v1/tracks/{id}/lyrics"
    data, token, cred = await authed_get_json(
        url,
        params={"countryCode": COUNTRY_CODE, "locale": "en_US", "deviceType": "BROWSER"},
    )

    if not data:
        raise HTTPException(status_code=404, detail="Lyrics not found")

    return {"version": API_VERSION, "lyrics": data}


@app.get("/topvideos/")
async def get_top_videos(
    countryCode: str = Query(default="US"),
    locale: str = Query(default="en_US"),
    deviceType: str = Query(default="BROWSER"),
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Fetch recommended videos from Tidal."""
    token, cred = await get_catalog_token_for_cred()
    url = "https://api.tidal.com/v1/pages/mymusic_recommended_videos"
    params = {
        "countryCode": countryCode,
        "locale": locale,
        "deviceType": deviceType,
    }

    data, token, cred = await authed_get_json(
        url,
        params=params,
        token=token,
        cred=cred,
    )

    rows = data.get("rows", [])
    all_videos = []
    for row in rows:
        modules = row.get("modules", [])
        for module in modules:
            module_type = module.get("type")
            if module_type in ("VIDEO_PLAYLIST", "VIDEO_ROW", "PAGED_LIST"):
                paged_list = module.get("pagedList", {})
                if paged_list:
                    items = paged_list.get("items", [])
                    for item in items:
                        video = item.get("item", item) if isinstance(item, dict) else item
                        all_videos.append(video)
            elif module_type == "VIDEO" or (module_type and "video" in module_type.lower()):
                item = module.get("item", module)
                if isinstance(item, dict):
                    all_videos.append(item)

    paginated = all_videos[offset:offset + limit]

    response = {
        "version": API_VERSION,
        "videos": paginated,
        "total": len(all_videos),
    }
    return response

@app.get("/video/")
async def get_video(
    id: int = Query(..., description="Video ID"),
    quality: str = Query(default="HIGH", description="Video quality (HIGH, MEDIUM, LOW)"),
    mode: str = Query(default="STREAM", description="Playback mode (STREAM, OFFLINE)"),
    presentation: str = Query(default="FULL", description="Asset presentation (FULL, PREVIEW)"),
):
    """Fetch video playback info from Tidal."""
    url = f"https://api.tidal.com/v1/videos/{id}/playbackinfo"
    params = {
        "videoquality": quality,
        "playbackmode": mode,
        "assetpresentation": presentation,
    }

    async def fetch_video(cred: dict):
        token, cred = await get_tidal_token_for_cred(cred=cred)
        data, token, cred = await authed_get_json(
            url,
            params=params,
            token=token,
            cred=cred,
            catalog=False,
        )
        return {"version": API_VERSION, "video": data}

    return await dispatch_playback_request(fetch_video)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)
