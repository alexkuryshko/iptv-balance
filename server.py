#!/usr/bin/env python3
"""
hls.gd server picker & playlist proxy.

Single-file Python (stdlib only) web service that:
  * periodically downloads the user's playlist from a direct cabinet URL,
  * measures Ping, Jitter, Loss, Download speed, QoS, MOS for all *.hls.gd,
  * accepts browser-side measurements from the home network (POST /api/report),
  * rewrites every channel URL to the best server,
  * serves the resulting playlist over HTTP for the TV/player to consume,
  * provides a modern dashboard at / and a client-side test page at /measure.

No external dependencies. Python 3.8+.
"""

from __future__ import annotations

import json
import math
import os
import re
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

# --------------------------------------------------------------------------- #
# Paths & constants
# --------------------------------------------------------------------------- #
HERE = os.path.dirname(os.path.abspath(__file__))
# Data dir (config, servers list, cookies, logs) — overridable via DATA_DIR env
# so Docker can mount a persistent volume separately from the read-only code.
DATA_DIR = os.environ.get("DATA_DIR") or HERE
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
SERVERS_PATH = os.path.join(DATA_DIR, "servers.json")

HLS_HOST_RE = re.compile(rb"https?://[a-z0-9]+\.hls\.gd/")
# matches  http(s)://<host>.hls.gd/<path>?<query>  -> captures /<path> (drops query)
HLS_PROXY_PLAYLIST_RE = re.compile(rb"https?://[a-z0-9]+\.hls\.gd(/[^\s?#]+)(?:\?[^\s]*)?")
# matches an absolute hls.gd URI anywhere (e.g. inside URI="..." tags)
HLS_PROXY_URI_RE = re.compile(rb"https?://[a-z0-9]+\.hls\.gd(/[^\s?\"#]+)")
# Stable, token-free, CORS-enabled test segment on every hls.gd host (~4.5 MB).
# We fetch a 2 MB range for throughput measurement.
SPEEDTEST_PATH = "/new_stub/stub_domain0.ts"
SPEEDTEST_RANGE = "bytes=0-2097151"  # 2 MB
SPEEDTEST_BYTES = 2097152
PING_ATTEMPTS = 5

# --------------------------------------------------------------------------- #
# Metric helpers
# --------------------------------------------------------------------------- #
def _stddev(xs: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mean = sum(xs) / n
    return math.sqrt(sum((x - mean) ** 2 for x in xs) / (n - 1))


def _compute_qos(ping_ms: float | None, jitter_ms: float | None,
                 loss_pct: float | None, download_mbps: float | None) -> float:
    """Quality of Service score 0..100 (higher = better)."""
    if ping_ms is None:
        return 0.0
    jitter_ms = jitter_ms or 0.0
    loss_pct = loss_pct or 0.0
    q = 100.0
    q -= loss_pct                                   # loss penalty (1:1)
    q -= min(jitter_ms * 0.5, 15.0)                 # jitter penalty
    q -= min(max(0.0, ping_ms - 100.0) * 0.1, 15.0)  # high latency penalty
    if download_mbps is not None:
        if download_mbps < 5.0:
            q -= (5.0 - download_mbps) * 4.0         # slow download penalty
    return round(max(0.0, min(100.0, q)), 1)


def _compute_mos(ping_ms: float | None, jitter_ms: float | None,
                 loss_pct: float | None, download_mbps: float | None) -> float:
    """Mean Opinion Score 1..4.5 (higher = better) for IPTV streaming.

    Calibrated so that low-latency/low-loss/fast servers reach ~4.3-4.4
    (excellent), moderate degradation drops into the 4.0-4.2 band (good),
    and clearly poor servers fall below 4.0. The previous formula divided
    the effective latency by 40, which made the penalty negligible and
    pinned nearly every healthy server at ~4.4 while making 4.5 (3 thumbs)
    mathematically unreachable — so the rating gradation was useless."""
    if ping_ms is None:
        return 1.0
    jitter_ms = jitter_ms or 0.0
    loss_pct = loss_pct or 0.0
    eff_latency = ping_ms + 2.0 * jitter_ms
    # E-model-ish R, but with video-grade penalties: delay, jitter and
    # especially packet loss are far more visible for IPTV than for voice.
    r = 93.2 - eff_latency / 8.0 - loss_pct * 2.5
    if download_mbps is not None and download_mbps < 10.0:
        r -= (10.0 - download_mbps) * 1.5
    r = max(0.0, min(100.0, r))
    mos = 1.0 + 0.035 * r + 0.000007 * r * (r - 60.0) * (100.0 - r)
    return round(max(1.0, min(4.5, mos)), 2)


# --------------------------------------------------------------------------- #
# State (thread-safe)
# --------------------------------------------------------------------------- #
class State:
    def __init__(self, servers: list[dict[str, str]], fallback_host: str,
                 client_ttl: int, rewrite_hosts: bool = False) -> None:
        self.lock = threading.RLock()
        self.servers = servers
        self.fallback_host = fallback_host
        self.client_ttl = client_ttl
        self.rewrite_hosts = rewrite_hosts

        self.playlist_raw: bytes = b""
        self.playlist_updated_at: float = 0.0
        self.playlist_error: str = ""

        # proxy mode: rewrite playlist URLs to /p/<path> and reverse-proxy
        # stream/m3u8 requests through this service so tokens always match the
        # cabinet's current server (no "update playlist" stub on switch).
        self.proxy_mode: bool = False
        self.proxy_map: dict[str, tuple[str, str]] = {}  # channel_id -> (host, token)

        # server-side: {host: {ip, ping_ms, jitter_ms, loss_pct,
        #                       download_mbps, qos, mos, status, ts}}
        self.measurements: dict[str, dict[str, Any]] = {}
        self.measurements_updated_at: float = 0.0

        # client-side: {host: {ping_ms, jitter_ms, loss_pct, download_mbps,
        #                       qos, mos, ts, ip}}
        self.client_measurements: dict[str, dict[str, Any]] = {}

        self.best_host: str = fallback_host
        self.last_selection_source: str = "fallback"

        # measurement progress for UI "Тестируем X/27"
        self.measure_in_progress: bool = False
        self.measure_done: int = 0
        self.measure_total: int = 0

    # --- playlist -------------------------------------------------------- #
    def set_playlist(self, data: bytes) -> None:
        with self.lock:
            self.playlist_raw = data
            self.playlist_updated_at = time.time()
            self.playlist_error = ""
            if self.proxy_mode:
                self._rebuild_proxy_map_locked(data)

    def _rebuild_proxy_map_locked(self, data: bytes) -> None:
        """Parse playlist URL lines into channel_id -> (host, token).

        Channel stream URLs look like:
            http://ru2.hls.gd/ch001/mono.m3u8?token=alexkuryshko.v2_...
        The first path segment is the channel id; the token is per-channel.
        """
        self.proxy_map = {}
        for m in re.finditer(
                rb"https?://([a-z0-9.]+)/([A-Za-z0-9_\-]+)/[^\s?]*\?token=([^\s&]+)",
                data):
            host = m.group(1).decode("ascii", "replace")
            chid = m.group(2).decode("ascii", "replace")
            tok = m.group(3).decode("ascii", "replace")
            self.proxy_map[chid] = (host, tok)

    def proxy_lookup(self, channel_id: str) -> tuple[str, str] | None:
        with self.lock:
            return self.proxy_map.get(channel_id)

    def set_playlist_error(self, err: str) -> None:
        with self.lock:
            self.playlist_error = err

    # --- measurements ---------------------------------------------------- #
    def set_measurements(self, m: dict[str, dict[str, Any]]) -> None:
        with self.lock:
            self.measurements = m
            self.measurements_updated_at = time.time()
            self.measure_in_progress = False
            self._reselect_locked()

    def update_measurements(self, partial: dict[str, dict[str, Any]]) -> None:
        """Merge partial results into existing measurements (re-test of a
        subset of hosts) without clearing the rest."""
        with self.lock:
            self.measurements.update(partial)
            self.measurements_updated_at = time.time()
            self.measure_in_progress = False
            self._reselect_locked()

    def set_measure_progress(self, done: int, total: int,
                             in_progress: bool = True) -> None:
        with self.lock:
            self.measure_done = done
            self.measure_total = total
            self.measure_in_progress = in_progress

    def update_client_measure(self, host: str, data: dict[str, Any],
                              ip: str) -> None:
        with self.lock:
            entry = {"ts": time.time(), "ip": ip}
            entry.update({k: v for k, v in data.items()
                         if k in ("ping_ms", "jitter_ms", "loss_pct",
                                  "download_mbps", "qos", "mos",
                                  "score", "hls_qos", "hls_mos")})
            self.client_measurements[host] = entry
            self._reselect_locked()

    # --- selection ------------------------------------------------------- #
    def _score(self, v: dict[str, Any]) -> tuple:
        """Higher tuple = better. Used with max() in _reselect_locked.
        Prefer the cabinet-style `score` (0..100, higher=better) when present;
        fall back to MOS-based ordering otherwise."""
        score = v.get("score")
        if score is not None and not (isinstance(score, float) and score != score):
            dl = v.get("download_mbps")
            return (float(score), dl if dl is not None else 0.0, 0.0)
        mos = v.get("mos")
        dl = v.get("download_mbps")
        ping = v.get("ping_ms")
        # invalid -> worst
        if mos is None:
            return (0, 0, 1e9)
        return (mos, dl if dl is not None else 0.0,
                -(ping if ping is not None else 1e9))

    def _reselect_locked(self) -> None:
        """Pick best host. Prefer fresh client measurements, else server-side."""
        now = time.time()
        fresh_client = {
            h: v for h, v in self.client_measurements.items()
            if now - v.get("ts", 0) <= self.client_ttl
            and (v.get("mos") is not None or v.get("score") is not None)
        }
        if fresh_client:
            best = max(fresh_client.items(),
                       key=lambda kv: self._score(kv[1]))
            self.best_host = best[0]
            self.last_selection_source = "client"
            return

        ok = {h: v for h, v in self.measurements.items()
              if v.get("status") == "ok" and v.get("mos") is not None}
        if ok:
            best = max(ok.items(), key=lambda kv: self._score(kv[1]))
            self.best_host = best[0]
            self.last_selection_source = "server"
            return

        self.best_host = self.fallback_host
        self.last_selection_source = "fallback"

    def rewrites_playlist(self, base_url: str = "") -> bytes:
        with self.lock:
            raw = self.playlist_raw
            host = self.best_host
            rewrite = self.rewrite_hosts
            proxy = self.proxy_mode
        if not raw:
            return b""
        if proxy:
            # Rewrite hls.gd stream URLs to proxy paths on our service:
            #   http://ru2.hls.gd/ch001/mono.m3u8?token=...  ->  p/ch001/mono.m3u8
            # If base_url is given (e.g. "http://192.168.1.5"), emit absolute
            # URLs (max player compatibility); otherwise relative, which
            # resolve against the playlist's own URL.
            repl = (base_url.encode("ascii", "replace") + b"p\\1") if base_url \
                else rb"p\1"
            return HLS_PROXY_PLAYLIST_RE.sub(repl, raw)
        if not rewrite:
            return raw
        return HLS_HOST_RE.sub(
            lambda m: (b"http://" if m.group().startswith(b"http://") else b"https://")
            + host.encode("ascii") + b"/",
            raw,
        )

    def served_playlist(self, base_url: str = "") -> bytes:
        """Playlist as it should be served to clients."""
        return self.rewrites_playlist(base_url)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            now = time.time()
            client_fresh = {
                h: v for h, v in self.client_measurements.items()
                if now - v.get("ts", 0) <= self.client_ttl
            }
            return {
                "best_host": self.best_host,
                "selection_source": self.last_selection_source,
                "playlist_updated_at": self.playlist_updated_at,
                "playlist_age_sec": int(now - self.playlist_updated_at) if self.playlist_updated_at else None,
                "playlist_size": len(self.playlist_raw),
                "playlist_error": self.playlist_error,
                "measurements_updated_at": self.measurements_updated_at,
                "measurements_age_sec": int(now - self.measurements_updated_at) if self.measurements_updated_at else None,
                "measurements": self.measurements,
                "client_measurements": self.client_measurements,
                "client_measurements_fresh_count": len(client_fresh),
                "measure_in_progress": self.measure_in_progress,
                "measure_done": self.measure_done,
                "measure_total": self.measure_total,
                "servers": self.servers,
            }


# --------------------------------------------------------------------------- #
# Cabinet API client (new.tv.team)
# --------------------------------------------------------------------------- #
import http.cookiejar
import urllib.parse

CABINET_BASE = "https://new.tv.team"
CABINET_COOKIE_FILE = os.path.join(DATA_DIR, ".cabinet_cookies.txt")
UA_CAB = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"


class Cabinet:
    """Thin client for the new.tv.team cabinet v3 API.

    Handles cookie/session persistence, CSRF, slider-captcha login,
    profile GET/PUT (server selection via groupId).
    """

    def __init__(self, base: str = CABINET_BASE,
                 cookie_file: str = CABINET_COOKIE_FILE) -> None:
        self.base = base.rstrip("/")
        self.cookie_file = cookie_file
        self.jar = http.cookiejar.LWPCookieJar(cookie_file)
        if os.path.exists(cookie_file):
            try:
                self.jar.load(ignore_discard=True, ignore_expires=True)
            except Exception:  # noqa: BLE001
                pass
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar))
        self.opener.addheaders = [
            ("User-Agent", UA_CAB),
            ("Accept", "application/json"),
        ]

    # --- low-level ------------------------------------------------------- #
    def _req(self, method: str, path: str, body: Any = None,
             body_type: str = "json", csrf: bool = False,
             extra_headers: dict[str, str] | None = None) -> tuple[int, Any, bytes]:
        url = path if path.startswith("http") else self.base + path
        data: bytes | None = None
        headers: dict[str, str] = dict(extra_headers or {})
        if body is not None and method != "GET":
            if body_type == "form":
                form = {k: str(v) for k, v in body.items()}
                data = urllib.parse.urlencode(form).encode("utf-8")
                headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
            else:
                data = json.dumps(body).encode("utf-8")
                headers["Content-Type"] = "application/json; charset=UTF-8"
        if csrf:
            tok = self.get_csrf()
            if tok:
                headers["X-CSRF-Token"] = tok
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with self.opener.open(req, timeout=25) as r:
                raw = r.read()
                code = r.status if hasattr(r, "status") else r.getcode()
        except urllib.error.HTTPError as e:
            raw = e.read()
            code = e.code
        except Exception as e:  # noqa: BLE001
            return 0, None, str(e).encode("utf-8")
        # auto-refresh on auth failure
        if code in (401, 403) and path.startswith("/v3/") and "/auth/" not in path:
            if self.refresh():
                return self._req(method, path, body, body_type, csrf, extra_headers)
        parsed = None
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception:  # noqa: BLE001
            pass
        if method != "GET":
            try:
                self.jar.save(ignore_discard=True, ignore_expires=True)
            except Exception:  # noqa: BLE001
                pass
        return code, parsed, raw

    def get_csrf(self) -> str:
        code, j, _ = self._req("GET", "/v3/auth/csrf")
        if j and isinstance(j, dict):
            return str(j.get("data", {}).get("csrf", "") or "")
        return ""

    def refresh(self) -> bool:
        code, j, _ = self._req("POST", "/v3/auth/refresh", body={}, body_type="json")
        ok = bool(j and j.get("data", {}).get("authorized") == 1)
        if not ok:
            # some backends return 200 with authorized:1 even without body
            ok = code == 200 and bool(j and j.get("data", {}).get("authorized"))
        return ok

    # --- captcha --------------------------------------------------------- #
    def captcha_generate(self) -> dict[str, Any]:
        code, j, _ = self._req("GET", "/v3/auth/captcha/generate",
                               extra_headers={"Cache-Control": "no-cache"})
        if j and isinstance(j, dict):
            data = j.get("data", {}) or {}
            ch = data.get("challenge", {}) or {}
            return {
                "captchaId": data.get("captchaId", ""),
                "baseImage": ch.get("baseImage", ""),
                "pieceImage": ch.get("pieceImage", ""),
                "width": ch.get("width", 384),
                "height": ch.get("height", 165),
                "knob": ch.get("knob", 44),
            }
        return {}

    def captcha_verify(self, captcha_id: str, offset_x: int,
                       trail: list[dict[str, int]]) -> tuple[bool, str]:
        body = {"captchaId": captcha_id,
                "offsetX": int(offset_x),
                "trail": trail}
        code, j, _ = self._req("POST", "/v3/auth/captcha/verify",
                               body=body, body_type="json")
        data = (j or {}).get("data", {}) or {}
        if data.get("valid"):
            return True, str(data.get("proof", "") or "")
        return False, str(data.get("message", "") or "invalid")

    # --- auth ------------------------------------------------------------ #
    def login(self, user: str, password: str, captcha_id: str,
              proof: str, remember: bool = True) -> tuple[bool, str]:
        body = {"userLogin": user, "userPasswd": password,
                "rememberMe": "1" if remember else "0",
                "captchaId": captcha_id, "captchaSolution": proof}
        code, j, _ = self._req("POST", "/v3/auth/login", body=body, body_type="form")
        data = (j or {}).get("data", {}) or {}
        if data.get("authorized") == 1:
            return True, ""
        return False, str(data.get("message", "") or
                          (j or {}).get("error", {}).get("code", "") or "auth failed")

    def logout(self) -> None:
        try:
            self._req("POST", "/v3/auth/logout", body={}, body_type="json")
        except Exception:  # noqa: BLE001
            pass

    # --- profile --------------------------------------------------------- #
    def get_profile(self) -> dict[str, Any]:
        code, j, _ = self._req("GET", "/v3/profile")
        if not j:
            return {}
        data = j.get("data", {}) or {}
        return {
            "profile": data.get("profile", {}) or {},
            "groups": data.get("groups", []) or [],
            "ok": code == 200,
        }

    def set_server(self, server_id: str) -> tuple[bool, str]:
        prof = self.get_profile()
        p = prof.get("profile", {}) or {}
        if not p:
            return False, "no profile (not logged in?)"
        body = {
            "userLogin": p.get("userLogin", ""),
            "userEmail": p.get("userEmail", ""),
            "groupId": str(server_id),
            "showPorno": p.get("showPorno", 0),
            "fsNumCon": p.get("fsNumCon", 0),
            "stNumCon": p.get("stNumCon", 0),
            "macAddress": p.get("macAddress", ""),
            "pornoPinCode": p.get("pornoPinCode", "0000"),
        }
        code, j, _ = self._req("PUT", "/v3/profile", body=body,
                               body_type="json", csrf=True)
        if code == 200 and (j or {}).get("data", {}).get("message") == "saved":
            return True, ""
        msg = (j or {}).get("error", {}).get("message") or \
              (j or {}).get("data", {}).get("message") or f"HTTP {code}"
        return False, str(msg)

    # --- playlist -------------------------------------------------------- #
    def get_playlist_content(self, play_list_id: str) -> tuple[bool, bytes, str]:
        """Fetch playlist via cabinet API (/v3/playlists/item).

        Returns the `content` field — the M3U8 with tokens freshly bound to
        the server currently selected in the profile (groupId). The direct
        hls.gd/pl/<id>/... URL is CDN-cached and does NOT reflect a recent
        groupId change, so this is the authoritative source after set_server.
        """
        code, j, _ = self._req(
            "GET", f"/v3/playlists/item?playListId={play_list_id}")
        data = (j or {}).get("data", {}) or {}
        content = data.get("content", "") or ""
        if not content:
            msg = (j or {}).get("error", {}).get("message") or f"HTTP {code}"
            return False, b"", str(msg or "no content")
        if isinstance(content, str):
            raw = content.encode("utf-8", "replace")
        else:
            raw = bytes(content)
        if not raw.lstrip().startswith(b"#EXTM3U"):
            return False, b"", "content is not an M3U8 playlist"
        return True, raw, ""

    def get_playlists(self) -> tuple[bool, list[dict], str]:
        """List the user's available playlists from the cabinet
        (/v3/playlists -> data.items). Each item: {id, name, devices, link}."""
        code, j, _ = self._req("GET", "/v3/playlists")
        data = (j or {}).get("data", {}) or {}
        items = data.get("items", []) or []
        if not items:
            msg = (j or {}).get("error", {}).get("message") or f"HTTP {code}"
            return False, [], str(msg or "no playlists")
        return True, items, ""


class CabinetState:
    """Thread-safe holder for cabinet session status."""
    def __init__(self, cabinet: Cabinet, user: str, password: str) -> None:
        self.cabinet = cabinet
        self.user = user
        self.password = password
        self.lock = threading.RLock()
        self.logged_in: bool = False
        self.login_error: str = ""
        self.profile: dict[str, Any] = {}
        self.groups: list[dict[str, str]] = []
        self.current_captcha: dict[str, Any] = {}
        self.last_proof: str = ""
        self.auto_apply: bool = True
        self.last_apply_error: str = ""
        self.last_applied_id: str = ""
        self.last_applied_at: float = 0.0

    def refresh_status(self) -> None:
        """Check session by fetching profile."""
        with self.lock:
            prof = self.cabinet.get_profile()
            if prof.get("ok") and prof.get("profile"):
                self.logged_in = True
                self.profile = prof["profile"]
                self.groups = prof.get("groups", [])
                self.login_error = ""
            else:
                self.logged_in = False

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            cur_id = str(self.profile.get("groupId", "") or "")
            cur_host = ""
            for g in self.groups:
                if str(g.get("value", "")) == cur_id:
                    cur_host = g.get("label", "")
                    break
            return {
                "logged_in": self.logged_in,
                "login_error": self.login_error,
                "user": self.profile.get("userLogin", "") or self.user,
                "current_server_id": cur_id,
                "current_server_host": cur_host,
                "groups": self.groups,
                "auto_apply": self.auto_apply,
                "last_apply_error": self.last_apply_error,
                "last_applied_id": self.last_applied_id,
                "last_applied_at": self.last_applied_at,
                "has_credentials": bool(self.user and self.password),
                "cabinet_user": self.user or "",
            }


# --------------------------------------------------------------------------- #
# Background workers
# --------------------------------------------------------------------------- #
def worker_playlist(state: State, playlist_url: str, interval_min: int,
                    cab: "CabinetState | None" = None) -> None:
    def fetch_once() -> None:
        try:
            data = fetch_playlist_smart(playlist_url, cab)
            state.set_playlist(data)
            print(f"[playlist] downloaded {len(data)} bytes", flush=True)
        except Exception as e:  # noqa: BLE001
            state.set_playlist_error(f"{type(e).__name__}: {e}")
            print(f"[playlist] error: {e}", flush=True)

    fetch_once()
    while True:
        time.sleep(max(1, interval_min) * 60)
        fetch_once()


def _measure_one(host: str, timeout: float, with_download: bool = True
                 ) -> dict[str, Any]:
    """Full per-host measurement: ping/jitter/loss + download + QoS + MOS."""
    out: dict[str, Any] = {
        "host": host, "ip": None,
        "ping_ms": None, "jitter_ms": None, "loss_pct": None,
        "download_mbps": None, "qos": None, "mos": None,
        "status": "fail",
    }
    # resolve
    try:
        infos = socket.getaddrinfo(host, 80, type=socket.SOCK_STREAM)
        out["ip"] = infos[0][4][0]
    except Exception:  # noqa: BLE001
        out["status"] = "dns_fail"
        return out

    # --- ping: PING_ATTEMPTS TCP connects ------------------------------- #
    lats: list[float] = []
    fail = 0
    for _ in range(PING_ATTEMPTS):
        t0 = time.perf_counter()
        try:
            with socket.create_connection((host, 80), timeout=timeout) as s:
                s.settimeout(timeout)
            lats.append((time.perf_counter() - t0) * 1000.0)
        except Exception:  # noqa: BLE001
            fail += 1
        time.sleep(0.05)
    if not lats:
        out["status"] = "unreachable"
        return out
    out["ping_ms"] = round(min(lats), 2)
    out["jitter_ms"] = round(_stddev(lats), 2)
    out["loss_pct"] = round(fail / PING_ATTEMPTS * 100.0, 1)

    # --- download throughput: 2 MB range of stub .ts --------------------- #
    if with_download:
        t0 = time.perf_counter()
        try:
            req = urllib.request.Request(
                f"http://{host}{SPEEDTEST_PATH}",
                headers={"Range": SPEEDTEST_RANGE,
                         "User-Agent": "tv-plst-picker/1.0"},
            )
            with urllib.request.urlopen(req, timeout=timeout + 10) as r:
                data = r.read()
            dt = time.perf_counter() - t0
            if dt > 0 and len(data) > 10000:
                out["download_mbps"] = round(len(data) * 8 / dt / 1e6, 2)
        except Exception:  # noqa: BLE001
            out["download_mbps"] = None

    # --- composite scores ------------------------------------------------ #
    out["qos"] = _compute_qos(out["ping_ms"], out["jitter_ms"],
                              out["loss_pct"], out["download_mbps"])
    out["mos"] = _compute_mos(out["ping_ms"], out["jitter_ms"],
                              out["loss_pct"], out["download_mbps"])
    out["status"] = "ok"
    return out


def _run_measure(state: State, timeout: float, with_download: bool = True,
                 hosts: list[str] | None = None, merge: bool = False
                 ) -> None:
    """Run a measurement cycle, updating progress as it goes.

    If hosts is None, measure all configured servers and replace the stored
    measurements. If hosts is given, measure only those and (when merge=True)
    update them in place without wiping the rest — useful for a quick re-test
    of just the active cabinet server.
    """
    all_hosts = [s["host"] for s in state.servers]
    targets = hosts if hosts is not None else all_hosts
    state.set_measure_progress(0, len(targets), True)
    results: dict[str, dict[str, Any]] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=min(8, len(targets) or 1)) as ex:
        futs = {ex.submit(_measure_one, h, timeout, with_download): h
                for h in targets}
        for fut in as_completed(futs):
            try:
                r = fut.result()
            except Exception as e:  # noqa: BLE001
                r = {"host": futs[fut], "status": f"error: {e}"}
            results[r["host"]] = r
            done += 1
            state.set_measure_progress(done, len(targets), True)
    if merge:
        state.update_measurements(results)
    else:
        state.set_measurements(results)
    with state.lock:
        print(f"[measure] done ({len(targets)} hosts, merge={merge}), "
              f"best={state.best_host} ({state.last_selection_source})",
              flush=True)


def worker_measure(state: State, timeout: float, interval_min: int) -> None:
    while True:
        _run_measure(state, timeout, with_download=True)
        time.sleep(max(1, interval_min) * 60)


def _run_measure_then_apply(state: State, cab: CabinetState,
                            playlist_url: str, timeout: float,
                            with_download: bool, hosts: list[str] | None,
                            merge: bool) -> None:
    """Run a measurement cycle, then (for a full sweep, when auto-apply is on
    and the cabinet is logged in) apply the best server via the cabinet.

    Hysteresis is respected (force=False): a manual 'Тест' never forces a
    switch away from a good current server — it only acts if the current one
    is degraded/failing or another is meaningfully better. Use the explicit
    'Apply best' button for a forced switch."""
    _run_measure(state, timeout, with_download=with_download, hosts=hosts, merge=merge)
    if merge:
        return  # only the current server was re-tested — nothing to choose from
    with cab.lock:
        on = cab.logged_in and cab.auto_apply
    if on:
        try:
            _apply_best_to_cabinet(state, cab, playlist_url, force=False)
        except Exception as e:  # noqa: BLE001
            print(f"[measure] auto-apply after manual test error: {e}", flush=True)


def fetch_playlist(url: str) -> bytes:
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (tv-plst picker)"})
    with urllib.request.urlopen(req, timeout=25) as r:
        data = r.read()
    if not data.startswith(b"#EXTM3U"):
        raise ValueError("not an M3U8 playlist")
    return data


def _playlist_id_from_url(url: str) -> str:
    """Extract playListId from a hls.gd playlist URL.

    Matches  http(s)://hls.gd/pl/<id>/<token>/playlist.m3u8
    or any URL containing /pl/<digits>/ .
    """
    m = re.search(r"/pl/(\d+)(?:/|\?|$)", url or "")
    return m.group(1) if m else ""


def fetch_playlist_smart(url: str, cab: "CabinetState | None" = None
                         ) -> bytes:
    """Fetch the playlist, preferring the cabinet API when logged in.

    The cabinet `/v3/playlists/item` endpoint returns M3U8 content with tokens
    bound to the server currently set in the profile (groupId). The plain
    hls.gd URL is CDN-cached and lags behind a groupId change, which produces
    "server changed, update playlist" stub errors in the player. So whenever
    we have an active cabinet session we pull content through the API;
    otherwise (or on failure) we fall back to the direct URL.
    """
    pid = _playlist_id_from_url(url)
    if pid and cab is not None:
        with cab.lock:
            logged_in = cab.logged_in
        if logged_in:
            ok, raw, err = cab.cabinet.get_playlist_content(pid)
            if ok and raw:
                print(f"[playlist] via cabinet API ({pid}): "
                      f"{len(raw)} bytes", flush=True)
                return raw
            print(f"[playlist] cabinet API failed ({err}); "
                  f"falling back to direct URL", flush=True)
    return fetch_playlist(url)


def _host_to_server_id(cab: CabinetState, host: str) -> str:
    """Find cabinet groupId for a given hls.gd host."""
    with cab.lock:
        for g in cab.groups:
            if g.get("label", "") == host:
                return str(g.get("value", ""))
    return ""


# --- stream proxy helpers --------------------------------------------------- #
PROXY_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"


def _rewrite_proxy_m3u8(body: bytes, channel_id: str) -> bytes:
    """Rewrite URLs inside a proxied m3u8 so they point back at our /p/ proxy.

    channel_id is the first path segment of the proxy request (e.g. "ch001");
    relative segment/variant URIs are resolved under /p/<channel_id>/...
    Absolute hls.gd URIs are rewritten to /p/<path>. Tokens are stripped — the
    proxy re-attaches the current cabinet token at request time, so a server
    switch is seamless even mid-stream.
    """
    cid = channel_id.encode("ascii", "replace")
    out: list[bytes] = []
    for raw_line in body.split(b"\n"):
        line = raw_line.rstrip(b"\r")
        stripped = line.strip()
        if not stripped:
            out.append(raw_line)
            continue
        if stripped.startswith(b"#"):
            # rewrite URI="..." attributes inside tags (e.g. #EXT-X-KEY)
            def _uri_abs(m: re.Match) -> bytes:
                return b'URI="/p' + m.group(1) + b'"'
            line = HLS_PROXY_URI_RE.sub(_uri_abs, line)
            out.append(line)
            continue
        # a URI line (segment / variant playlist)
        seg = stripped.split(b"?", 1)[0]  # drop query (token)
        if seg.startswith(b"http://") or seg.startswith(b"https://"):
            m = HLS_PROXY_URI_RE.match(seg)
            if m:
                out.append(b"/p" + m.group(1))
            else:
                out.append(line)  # non-hls.gd absolute — leave as-is
        else:
            # relative -> resolve under /p/<channel_id>/
            rel = seg[1:] if seg.startswith(b"/") else seg
            out.append(b"/p/" + cid + b"/" + rel)
    return b"\n".join(out)


def _proxy_fetch(upstream_url: str, headers: dict[str, str] | None = None,
                 timeout: float = 60.0) -> tuple[int, dict[str, str], Any]:
    """Open an upstream URL for the proxy, returning (status, headers, resp).

    The caller is responsible for reading/closing `resp`. Headers are
    lowercased.
    """
    hs = {"User-Agent": PROXY_UA}
    if headers:
        hs.update(headers)
    req = urllib.request.Request(upstream_url, headers=hs)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return (resp.status if hasattr(resp, "status") else resp.getcode(),
                {k.lower(): v for k, v in resp.headers.items()}, resp)
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e
    except Exception:  # noqa: BLE001
        return 0, {}, None


def _playlist_host(raw: bytes) -> str:
    m = re.search(rb"https?://([a-z0-9]+\.hls\.gd)/", raw)
    return m.group(1).decode("ascii", "replace") if m else ""


def detect_local_ip() -> str:
    """Best-effort primary LAN IP of this machine (for the default connect addr)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        finally:
            s.close()
    except Exception:  # noqa: BLE001
        return "127.0.0.1"


def _base_from_authority(auth: str) -> str:
    """Build an http base URL ('http://host[:port]/') from a free-form authority
    string the user may type: 'tv.cybr.com', 'tv.cybr.com:8080',
    'http://tv.cybr.com', etc. Empty -> '' (caller falls back to Host header)."""
    auth = (auth or "").strip()
    if not auth:
        return ""
    for sch in ("http://", "https://"):
        if auth.lower().startswith(sch):
            auth = auth[len(sch):]
            break
    auth = auth.rstrip("/")
    if not auth:
        return ""
    return f"http://{auth}/"


def _upstream_serves_real(host: str, channel_path: str, token: str,
                          timeout: float = 15.0) -> bool:
    """Fetch a channel master m3u8 from <host> and return True if it returns
    real segments (not the provider's "server changed" stub)."""
    url = f"http://{host}{channel_path}?token={urllib.parse.quote(token)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": PROXY_UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
    except Exception:  # noqa: BLE001
        return False
    for line in data.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            return not line.lower().startswith("stub")
    return False


def warm_apply_playlist(state: State, cab: "CabinetState | None",
                        playlist_url: str, max_wait: float = 120.0,
                        step: float = 5.0) -> None:
    """After a cabinet server switch, repeatedly re-fetch the playlist through
    the cabinet API and probe the new server until it serves real segments,
    then swap the proxy map.

    Re-fetching each cycle is essential: the token issued by the cabinet API in
    the first seconds after set_server is frequently NOT yet honored by the new
    server (provider-side propagation). A single-shot probe with that stale
    token would fail for the whole window and then swap in a dead token,
    leaving the proxy on a stub until the next periodic reload. By re-fetching
    a fresh token every cycle, we swap as soon as the cabinet API + server have
    converged — typically ~30s — with a valid token, so playback recovers
    immediately.
    """
    if not playlist_url:
        return
    with state.lock:
        old_host = _playlist_host(state.playlist_raw) if state.playlist_raw else ""
    deadline = time.time() + max_wait
    attempt = 0
    last_content: bytes | None = None
    new_host = ""
    print(f"[proxy] warm-apply after switch (old={old_host})", flush=True)
    while time.time() < deadline:
        attempt += 1
        try:
            content = fetch_playlist_smart(playlist_url, cab)
        except Exception as e:  # noqa: BLE001
            print(f"[proxy] warm fetch error: {e}", flush=True)
            time.sleep(step)
            continue
        last_content = content
        new_host = _playlist_host(content)
        if not new_host or new_host == old_host or not state.proxy_mode:
            state.set_playlist(content)
            print(f"[proxy] warm-apply: host={new_host or '?'} "
                  f"(same/none/proxy off); swapped", flush=True)
            return
        m = re.search(
            rb"https?://[a-z0-9.]+(/[A-Za-z0-9_\-]+/[^\s?]*\.m3u8)\?token=([^\s&]+)",
            content)
        if not m:
            state.set_playlist(content)
            return
        ch_path = m.group(1).decode("ascii", "replace")
        token = m.group(2).decode("ascii", "replace")
        if _upstream_serves_real(new_host, ch_path, token):
            state.set_playlist(content)
            print(f"[proxy] {new_host} ready after {attempt} probe(s) "
                  f"(fresh token); swapped", flush=True)
            return
        time.sleep(step)
    if last_content:
        state.set_playlist(last_content)
        print(f"[proxy] {new_host or '?'} not ready after {max_wait}s; "
              f"swapped with last fetched content", flush=True)


def _apply_best_to_cabinet(state: State, cab: CabinetState,
                           playlist_url: str, force: bool = False) -> None:
    """If logged in & auto_apply, set cabinet server to measured best host,
    then re-download the playlist (tokens get re-bound by the provider).

    When force=True (manual "apply best" button, post-login) the hysteresis
    guard is skipped — the user explicitly asked for the best server. In the
    automatic worker path (force=False) we keep the current server unless the
    best one is meaningfully better, to avoid invalidating the player's cached
    playlist on every measurement cycle.
    """
    if not cab.user or not cab.password:
        return
    with state.lock:
        best_host = state.best_host
        src = state.last_selection_source
    if not best_host or src == "fallback":
        print(f"[cabinet] auto-apply skip (no best/force={force})", flush=True)
        return
    if not force:
        with cab.lock:
            if not cab.logged_in or not cab.auto_apply:
                return
    else:
        with cab.lock:
            if not cab.logged_in:
                return
    sid = _host_to_server_id(cab, best_host)
    if not sid:
        with cab.lock:
            cab.last_apply_error = f"сервер {best_host} не найден в списке кабинета"
        return

    # --- hysteresis: avoid flip-flopping the cabinet server on jitter ----- #
    # Each cabinet server change invalidates tokens in the player's cached
    # playlist ("server changed, update playlist" stub). So only switch when
    # the best server is meaningfully better than the one already set in the
    # cabinet, or the current one is failing/unmeasured. Skipped when force.
    MOS_HYSTERESIS = 0.3
    with cab.lock:
        cur_sid = str(cab.profile.get("groupId", "") or "")
    cur_host = ""
    with cab.lock:
        for g in cab.groups:
            if str(g.get("value", "")) == cur_sid:
                cur_host = g.get("label", "")
                break
    if cur_sid and cur_sid == sid:
        # already on the best server — nothing to do, reset error
        with cab.lock:
            cab.last_apply_error = ""
            cab.last_applied_id = sid
            cab.last_applied_at = time.time()
        print(f"[cabinet] already on best {best_host}; no switch needed"
              f"{' (manual test)' if not force else ''}", flush=True)
        return
    if not force:
        MOS_HYSTERESIS = 0.3
        MOS_GOOD = 4.05  # current server at/above this is "good enough" (2 thumbs)
        with state.lock:
            best_mos = (state.measurements.get(best_host, {}) or {}).get("mos")
            cur_mos = (state.measurements.get(cur_host, {}) or {}).get("mos") \
                if cur_host else None
            cur_status = (state.measurements.get(cur_host, {}) or {}).get("status") \
                if cur_host else None
        # Each switch costs a ~30s stub window (provider token propagation),
        # so never auto-switch away from a server that is already good. Only
        # switch when the current one is degraded/failing.
        if cur_mos is not None and cur_status == "ok" and cur_mos >= MOS_GOOD:
            print(f"[cabinet] keep {cur_host} (MOS {cur_mos:.2f} >= {MOS_GOOD}) "
                  f"— current is good, no auto-switch", flush=True)
            with cab.lock:
                cab.last_apply_error = ""
            return
        should_switch = True
        if cur_mos is not None and best_mos is not None \
                and cur_status == "ok" \
                and (best_mos - cur_mos) < MOS_HYSTERESIS:
            # current is degraded but best isn't meaningfully better — keep it
            should_switch = False
            reason = (f"keep {cur_host} (MOS {cur_mos:.2f}) vs best {best_host} "
                      f"(MOS {best_mos:.2f}) — разница < {MOS_HYSTERESIS}")
            print(f"[cabinet] {reason}", flush=True)
            with cab.lock:
                cab.last_apply_error = ""
        if not should_switch:
            return
        with cab.lock:
            if sid == cab.last_applied_id and cur_sid == sid \
                    and (time.time() - cab.last_applied_at) < 600:
                return  # already applied recently
    ok, err = cab.cabinet.set_server(sid)
    with cab.lock:
        if ok:
            cab.last_applied_id = sid
            cab.last_applied_at = time.time()
        cab.last_apply_error = "" if ok else err
    if not ok:
        print(f"[cabinet] set_server({sid}/{best_host}) failed: {err}", flush=True)
        return
    print(f"[cabinet] set groupId={sid} ({best_host}), re-fetching playlist",
          flush=True)
    cab.refresh_status()
    if not playlist_url:
        return
    # warm-apply: re-fetch the cabinet playlist each probe cycle (fresh token)
    # and swap once the new server serves real segments. Handles the ~30s
    # provider-side propagation delay without leaving the proxy on a dead token.
    try:
        warm_apply_playlist(state, cab, playlist_url)
    except Exception as e:  # noqa: BLE001
        state.set_playlist_error(f"cabinet warm-apply: {type(e).__name__}: {e}")
        print(f"[cabinet] warm-apply error: {e}", flush=True)


def worker_cabinet(state: State, cab: CabinetState, playlist_url: str,
                   interval_min: int) -> None:
    """Periodically refresh cabinet status and auto-apply best server."""
    cab.refresh_status()
    while True:
        try:
            cab.refresh_status()
            _apply_best_to_cabinet(state, cab, playlist_url)
        except Exception as e:  # noqa: BLE001
            print(f"[cabinet] worker error: {e}", flush=True)
        time.sleep(max(1, interval_min) * 60)


# --------------------------------------------------------------------------- #
# HTTP handlers
# --------------------------------------------------------------------------- #
def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")


# --- Icons (inline SVG strings) -------------------------------------------- #
ICONS = {
    "bolt": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 14a1 1 0 0 1-.78-1.63l9.9-10.2a.5.5 0 0 1 .86.46l-1.92 6.02A1 1 0 0 0 13 10h7a1 1 0 0 1 .78 1.63l-9.9 10.2a.5.5 0 0 1-.86-.46l1.92-6.02A1 1 0 0 0 11 14z"/></svg>',
    "wave": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 12h2l2-6 3 14 3-10 2 6 2-4h6"/></svg>',
    "down": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14"/><path d="m19 12-7 7-7-7"/></svg>',
    "medal": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M7.21 15 2.66 7.14a.5.5 0 0 1 .43-.74h3.48a2 2 0 0 1 1.78 1.08L11.79 11"/><path d="m16.79 15 4.55-7.86a.5.5 0 0 0-.43-.74h-3.48a2 2 0 0 0-1.78 1.08L12.21 11"/><circle cx="12" cy="17" r="5"/></svg>',
    "trophy": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6"/><path d="M18 9h1.5a2.5 2.5 0 0 0 0-5H18"/><path d="M4 22h16"/><path d="M10 14.66V17c0 .55-.47.98-.97 1.21C7.85 18.75 7 20.24 7 22"/><path d="M14 14.66V17c0 .55.47.98.97 1.21C16.15 18.75 17 20.24 17 22"/><path d="M18 2H6v7a6 6 0 0 0 12 0V2Z"/></svg>',
    "check": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>',
    "thumbs": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M7 10v12"/><path d="M15 5.88 14 10h5.83a2 2 0 0 1 1.92 2.56l-2.33 8A2 2 0 0 1 17.5 22H4a2 2 0 0 1-2-2v-8a2 2 0 0 1 2-2h2.76a2 2 0 0 0 1.79-1.11L12 2a3.13 3.13 0 0 1 3 3.88Z"/></svg>',
    "ping": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h2"/><path d="M9 12h2"/><path d="M13 12h2"/><path d="M17 12h2"/><path d="m19 9 3 3-3 3"/></svg>',
    "loss": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/></svg>',
    "x": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>',
    "play": '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z"/></svg>',
    "spinner": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round"><path d="M21 12a9 9 0 1 1-6.219-8.56" /></svg>',
    "grid": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="7" height="7" x="3" y="3" rx="1"/><rect width="7" height="7" x="14" y="3" rx="1"/><rect width="7" height="7" x="14" y="14" rx="1"/><rect width="7" height="7" x="3" y="14" rx="1"/></svg>',
    "list": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="8" x2="21" y1="6" y2="6"/><line x1="8" x2="21" y1="12" y2="12"/><line x1="8" x2="21" y1="18" y2="18"/><line x1="3" x2="3.01" y1="6" y2="6"/><line x1="3" x2="3.01" y1="12" y2="12"/><line x1="3" x2="3.01" y1="18" y2="18"/></svg>',
}


# --- Modern HTML dashboard -------------------------------------------------- #
DASHBOARD_HTML = """<!doctype html>
<html lang="ru" class="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/png" href="/logo.png">
<link rel="apple-touch-icon" href="/logo.png">
<title>IPTV Balance</title>
<style>
  :root{
    --bg:#0a0e1a; --bg2:#111726; --card:#161d2f; --card2:#1c2542;
    --border:#243056; --text:#e6ecff; --muted:#8b96b8; --dim:#5b6688;
    --primary:#6366f1; --primary2:#818cf8; --accent:#22d3ee;
    --ok:#22c55e; --ok2:#4ade80; --warn:#f59e0b; --err:#ef4444; --best:#facc15;
    --blue:#60a5fa; --purple:#a78bfa; --orange:#fb923c; --green:#34d399;
    --grad: linear-gradient(135deg,#6366f1 0%,#22d3ee 100%);
    --grad-best: linear-gradient(135deg,#facc15 0%,#f59e0b 100%);
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,Helvetica,Arial,sans-serif;
       background:radial-gradient(1200px 600px at 10% -10%,#1e2a55 0%,transparent 60%),
                  radial-gradient(1000px 500px at 100% 0%,#0e2a3a 0%,transparent 55%),
                  var(--bg);
       color:var(--text);min-height:100vh;line-height:1.5;-webkit-font-smoothing:antialiased}
  a{color:var(--primary2);text-decoration:none}
  a:hover{text-decoration:underline}
  .svg{display:inline-block;vertical-align:middle;width:1em;height:1em;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
  .wrap{max-width:1320px;margin:0 auto;padding:28px 20px 60px}
  header{display:flex;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:22px}
  .logo{width:50px;height:50px;border-radius:12px;background:linear-gradient(135deg,#22c55e,#16a34a);
        display:grid;place-items:center;color:white;box-shadow:0 10px 30px rgba(34,197,94,.35)}
  .logo .svg{width:26px;height:26px}
  .titles h1{font-size:22px;font-weight:800;letter-spacing:-.4px}
  .titles .sub{color:var(--muted);font-size:13px;margin-top:2px}
  .nav{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
  .nav a{padding:8px 12px;border:1px solid var(--border);border-radius:9px;
         background:var(--card);color:var(--text);font-size:12px;font-weight:600;transition:.18s}
  .nav a:hover{border-color:var(--primary);background:var(--card2);text-decoration:none}

  /* control panel like the cabinet modal */
  .panel{background:var(--card);border:1px solid var(--border);border-radius:16px;
         padding:18px;margin-bottom:18px;box-shadow:0 12px 40px rgba(0,0,0,.25)}
  .panel-row{display:flex;gap:14px;flex-wrap:wrap;align-items:center}
  .field{display:flex;flex-direction:column;gap:5px}
  .field label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
  select{background:var(--bg2);border:1px solid var(--border);color:var(--text);
         padding:9px 12px;border-radius:9px;font-size:13px;font-family:inherit;min-width:160px}
  .checks{display:flex;gap:16px;flex-wrap:wrap}
  .check{display:flex;align-items:center;gap:7px;cursor:pointer;font-size:13px}
  .check input{width:16px;height:16px;accent-color:var(--ok);cursor:pointer}
  .btn-run{flex:1;min-width:220px;cursor:pointer;border:none;color:white;
           background:linear-gradient(135deg,#16a34a,#22c55e);
           padding:12px 18px;border-radius:11px;font-size:14px;font-weight:700;
           font-family:inherit;display:flex;align-items:center;justify-content:center;gap:9px;
           box-shadow:0 8px 22px rgba(34,197,94,.35);transition:.18s}
  .btn-run:hover{transform:translateY(-1px);box-shadow:0 10px 28px rgba(34,197,94,.5)}
  .btn-run:disabled{opacity:.6;cursor:not-allowed;transform:none}
  .btn-run .svg{width:16px;height:16px}
  .spin{animation:sp .8s linear infinite}
  @keyframes sp{to{transform:rotate(360deg)}}

  /* best server highlight */
  .best-card{background:linear-gradient(180deg,rgba(250,204,21,.10),var(--card));
             border:1.5px solid var(--best);border-radius:16px;padding:16px 18px;
             margin-bottom:14px;box-shadow:0 0 0 1px rgba(250,204,21,.2),0 14px 40px rgba(250,204,21,.12);
             display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  .best-card .badge-best{display:inline-flex;align-items:center;gap:6px;
         background:var(--grad-best);color:#1a1206;font-weight:800;font-size:11px;
         padding:4px 10px;border-radius:999px;text-transform:uppercase;letter-spacing:.5px}
  .best-card .badge-best .svg{width:13px;height:13px}
  .best-card .host{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
                   font-size:20px;font-weight:800;letter-spacing:-.3px}
  .best-card .thumbs{color:var(--best);font-size:18px;letter-spacing:2px}
  .best-card .summary{color:var(--muted);font-size:13px;margin-left:auto;font-family:ui-monospace,monospace}
  .best-card .summary b{color:var(--text)}

  /* server cards */
  .cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:14px;
        padding:14px 15px;transition:.18s}
  .card:hover{border-color:var(--primary);transform:translateY(-2px)}
  .card.is-best{border-color:var(--best);box-shadow:0 0 0 1px var(--best),0 10px 30px rgba(250,204,21,.12)}
  .card.fail{opacity:.55}
  .card-head{display:flex;align-items:center;gap:8px;margin-bottom:11px}
  .card-head .ok-mark{width:20px;height:20px;border-radius:50%;background:rgba(34,197,94,.18);
         color:var(--ok2);display:grid;place-items:center}
  .card-head .ok-mark .svg{width:13px;height:13px}
  .card-head .ok-mark.fail{background:rgba(239,68,68,.18);color:#fca5a5}
  .card-head .host{font-family:ui-monospace,monospace;font-weight:700;font-size:14px}
  .card-head .thumbs{color:var(--best);margin-left:auto;font-size:14px;letter-spacing:1px;opacity:.85}
  .card-head .ip{color:var(--dim);font-size:11px;font-family:ui-monospace,monospace}
  .grid4{display:grid;grid-template-columns:1fr 1fr;gap:10px}
  .metric{background:var(--bg2);border:1px solid rgba(36,48,86,.5);border-radius:10px;
          padding:9px 11px;display:flex;align-items:center;gap:9px}
  .metric .ic{width:28px;height:28px;border-radius:8px;display:grid;place-items:center;flex-shrink:0}
  .metric .ic .svg{width:16px;height:16px}
  .m-jitter .ic{background:rgba(96,165,250,.15);color:var(--blue)}
  .m-qos .ic{background:rgba(167,139,250,.15);color:var(--purple)}
  .m-down .ic{background:rgba(52,211,153,.15);color:var(--green)}
  .m-mos .ic{background:rgba(251,146,60,.15);color:var(--orange)}
  .metric .body{min-width:0}
  .metric .k{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.4px;font-weight:600}
  .metric .v{font-size:15px;font-weight:800;letter-spacing:-.3px;font-variant-numeric:tabular-nums}
  .metric .v .u{font-size:10px;color:var(--dim);font-weight:600;margin-left:2px}
  .card-foot{display:flex;gap:14px;margin-top:10px;padding-top:9px;border-top:1px solid rgba(36,48,86,.5);font-size:11px;color:var(--muted);font-family:ui-monospace,monospace}

  /* captcha slider */
  .cap-wrap{max-width:420px}
  .cap-img{position:relative;width:384px;max-width:100%;height:165px;border-radius:10px;
           overflow:hidden;background:var(--bg2);border:1px solid var(--border);user-select:none}
  .cap-img img{position:absolute;top:0;left:0;height:100%;display:block;pointer-events:none}
  .cap-piece{filter:drop-shadow(0 2px 4px rgba(0,0,0,.5));will-change:transform}
  .cap-overlay{position:absolute;inset:0;display:grid;place-items:center;color:var(--muted);
               font-size:13px;background:rgba(10,14,26,.6);backdrop-filter:blur(2px)}
  .cap-track{position:relative;width:384px;max-width:100%;height:42px;margin-top:10px;
             background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden}
  .cap-knob{position:absolute;top:0;left:0;width:42px;height:42px;background:linear-gradient(135deg,#6366f1,#22d3ee);
            display:grid;place-items:center;color:#fff;cursor:grab;border-radius:9px;touch-action:none}
  .cap-knob:active{cursor:grabbing}
  .cap-knob .svg{width:18px;height:18px;transform:rotate(-90deg)}
  .cap-hint{position:absolute;left:54px;top:50%;transform:translateY(-50%);color:var(--dim);font-size:12px;pointer-events:none}

  /* view toggle */
  .view-bar{display:flex;align-items:center;gap:10px;margin:6px 0 12px}
  .view-bar .title{font-size:13px;font-weight:700;color:var(--text)}
  .view-bar .spacer{flex:1}
  .view-toggle{display:inline-flex;background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:3px}
  .view-toggle button{display:inline-flex;align-items:center;gap:6px;background:transparent;border:none;
         color:var(--muted);font-size:12px;font-weight:600;padding:6px 11px;border-radius:7px;cursor:pointer;transition:.15s}
  .view-toggle button .svg{width:15px;height:15px}
  .view-toggle button.active{background:var(--card2);color:var(--text);box-shadow:0 0 0 1px var(--border)}
  .view-toggle button:hover:not(.active){color:var(--text)}

  /* rows view */
  .rows{display:flex;flex-direction:column;gap:6px}
  .row-item{display:grid;grid-template-columns:24px 150px repeat(6,1fr) 70px;gap:12px;align-items:center;
         background:var(--card);border:1px solid var(--border);border-radius:10px;padding:10px 14px;
         font-size:12px;transition:.15s}
  .row-item:hover{border-color:var(--primary)}
  .row-item.is-best{border-color:var(--best);box-shadow:0 0 0 1px var(--best),0 6px 20px rgba(250,204,21,.10)}
  .row-item.fail{opacity:.55}
  .row-item .rmark{width:18px;height:18px;border-radius:50%;display:grid;place-items:center;
         background:rgba(34,197,94,.18);color:var(--ok2);font-size:11px}
  .row-item .rmark.fail{background:rgba(239,68,68,.18);color:#fca5a5}
  .row-item .rhost{font-family:ui-monospace,monospace;font-weight:700;font-size:13px;color:var(--text)}
  .row-item .rcell{font-family:ui-monospace,monospace;color:var(--text)}
  .row-item .rcell .k{display:block;font-size:10px;color:var(--muted);font-family:inherit;font-weight:400}
  .row-item .rcell .v{font-size:13px;font-weight:600}
  .row-item .rbadge{font-size:11px;font-weight:700;color:var(--best);text-align:right}
  .row-head{display:grid;grid-template-columns:24px 150px repeat(6,1fr) 70px;gap:12px;
         padding:6px 14px;font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em;font-weight:600}
  @media(max-width:760px){.row-item,.row-head{grid-template-columns:24px 110px repeat(2,1fr)} .row-item .rcell.h,.row-head .h{display:none}}

  .footer-status{text-align:center;color:var(--dim);font-size:12px;margin-top:18px;min-height:18px}
  .url-box{margin-top:16px;padding:13px 15px;border:1px dashed var(--border);
           border-radius:11px;background:var(--bg2);font-family:ui-monospace,monospace;
           font-size:13px;word-break:break-all;color:var(--accent);
           display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .url-box b{color:var(--text);white-space:nowrap}
  .url-box #url{flex:1;min-width:0}
  .copy-btn{flex:none;cursor:pointer;border:1px solid var(--border);background:var(--bg);
            color:var(--text);padding:5px 10px;border-radius:8px;font-size:15px;line-height:1}
  .copy-btn:hover{background:var(--bg2);border-color:var(--accent)}
  .row-info{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px;color:var(--muted);font-size:12px}
  .pill{display:inline-flex;align-items:center;gap:6px;padding:3px 9px;border-radius:999px;
        font-size:11px;font-weight:700;border:1px solid var(--border);background:var(--bg2)}
  .dot{width:7px;height:7px;border-radius:50%}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="logo" style="width:46px;height:46px;border-radius:12px;overflow:hidden;background:#000;flex:none"><img src="/logo.png" alt="logo" style="width:100%;height:100%;object-fit:cover;display:block"></div>
    <div class="titles">
      <h1>IPTV Balance</h1>
      <div class="sub">Автоподбор лучшего сервера и плейлист new.tv.team</div>
    </div>
  </header>

  <div class="panel" id="cfgPanel">
    <div class="panel-row" style="align-items:flex-end">
      <div class="field" style="flex:1;min-width:240px">
        <label>Плейлист (из кабинета new.tv.team) <a href="#" id="plCustomToggle" onclick="plToggleCustom();return false" style="font-size:11px;color:var(--accent);text-decoration:none"> · свой URL</a></label>
        <select id="plSel" style="width:100%;background:var(--bg2);border:1px solid var(--border);color:var(--text);padding:9px 12px;border-radius:9px;font-size:13px;font-family:ui-monospace,monospace">
          <option value="">— войдите в кабинет, чтобы выбрать плейлист —</option>
        </select>
        <input type="text" id="plUrl" placeholder="http://hls.gd/pl/.../playlist.m3u8" style="display:none;width:100%;background:var(--bg2);border:1px solid var(--border);color:var(--text);padding:9px 12px;border-radius:9px;font-size:13px;font-family:ui-monospace,monospace">
      </div>
      <button class="btn-run" id="saveBtn" onclick="saveConfig()"><span id="saveLabel">Сохранить</span></button>
      <button class="btn-run" id="dlBtn" onclick="downloadPlaylistFile()" style="background:linear-gradient(135deg,#6366f1,#22d3ee);box-shadow:0 8px 22px rgba(99,102,241,.35)"><span class="svg">__IC_DOWN__</span><span id="dlLabel">Скачать</span></button>
    </div>
    <div class="panel-row" style="align-items:flex-end;margin-top:10px">
      <div class="field" style="flex:1;min-width:240px">
        <label>Адрес подключения (как плеер обращается к сервису) — попадает в ссылки плейлиста</label>
        <div style="display:flex;gap:8px;align-items:stretch">
          <input type="text" id="connAddr" placeholder="cybrp.com  или  tv.cybrp.com  или  tv.cybrp.com:80"
                 style="flex:1;background:var(--bg2);border:1px solid var(--border);color:var(--text);padding:9px 12px;border-radius:9px;font-size:13px;font-family:ui-monospace,monospace">
          <button class="copy-btn" id="applyConn" onclick="applyConnAddr()" title="Применить адрес" style="padding:9px 14px;font-size:13px">✅ Применить</button>
        </div>
      </div>
    </div>
    <div class="panel-row" style="align-items:flex-end;margin-top:10px">
      <div class="field" style="flex:1;min-width:240px">
        <label>Плейлист для плеера</label>
        <div style="display:flex;gap:8px;align-items:stretch">
          <input type="text" id="url" readonly placeholder="http://cybrp.com/plst.m3u8"
                 style="flex:1;background:var(--bg2);border:1px solid var(--border);color:var(--accent);padding:9px 12px;border-radius:9px;font-size:13px;font-family:ui-monospace,monospace">
          <button class="copy-btn" id="copyUrl" onclick="copyUrl()" title="Скопировать в буфер" style="padding:9px 12px">📋</button>
        </div>
      </div>
    </div>
    <div id="plInfo" style="margin-top:10px;font-size:12px;color:var(--muted);font-family:ui-monospace,monospace"></div>
  </div>

  <div class="panel" id="cabPanel">
    <div class="panel-row" style="margin-bottom:12px">
      <div class="logo" style="width:38px;height:38px;border-radius:10px;background:linear-gradient(135deg,#6366f1,#22d3ee);box-shadow:0 6px 18px rgba(99,102,241,.35)"><span class="svg">__IC_BOLT__</span></div>
      <div style="flex:1">
        <div style="font-weight:800;font-size:15px">Кабинет new.tv.team</div>
        <div class="sub" style="font-size:12px;color:var(--muted)">Смена сервера через API → токены перевыпускаются под новый хост</div>
      </div>
      <span id="cabStatus" class="pill" style="background:var(--bg2)">—</span>
    </div>

    <!-- not logged in: login + password, then captcha -->
    <div id="cabLogin">
      <div style="font-size:13px;color:var(--muted);margin-bottom:10px">Войдите в кабинет new.tv.team, чтобы сервис мог применять лучший сервер. Введите логин и пароль, затем пройдите капчу.</div>
      <div class="panel-row" style="margin-bottom:12px">
        <div class="field" style="flex:1;min-width:180px">
          <label>Логин (email)</label>
          <input type="text" id="cabUser" placeholder="alexkuryshko" autocomplete="username"
                 style="width:100%;background:var(--bg2);border:1px solid var(--border);color:var(--text);padding:9px 12px;border-radius:9px;font-size:13px;font-family:ui-monospace,monospace">
        </div>
        <div class="field" style="flex:1;min-width:160px">
          <label>Пароль</label>
          <input type="password" id="cabPass" placeholder="••••••••" autocomplete="current-password"
                 style="width:100%;background:var(--bg2);border:1px solid var(--border);color:var(--text);padding:9px 12px;border-radius:9px;font-size:13px;font-family:ui-monospace,monospace">
        </div>
      </div>
      <div class="cap-wrap" id="capWrap">
        <div class="cap-img" id="capImgBox">
          <img id="capBase" alt="" style="display:none">
          <img id="capPiece" alt="" class="cap-piece" style="display:none">
          <div class="cap-overlay" id="capOverlay">Введите логин/пароль и нажмите «Получить captcha»</div>
        </div>
        <div class="cap-track" id="capTrack">
          <div class="cap-knob" id="capKnob"><span class="svg">__IC_PLAY__</span></div>
          <span class="cap-hint" id="capHint">← потяните</span>
        </div>
      </div>
      <div class="panel-row" style="margin-top:12px">
        <button class="btn-run" id="capGenBtn" onclick="capGenerate()" style="background:linear-gradient(135deg,#6366f1,#22d3ee);box-shadow:0 8px 22px rgba(99,102,241,.35);flex:0 0 auto;min-width:180px"><span id="capGenLabel">Получить captcha</span></button>
        <button class="btn-run" id="capLoginBtn" onclick="capLogin()" disabled style="flex:0 0 auto;min-width:160px"><span id="capLoginLabel">Войти</span></button>
        <span id="capInfo" style="font-size:12px;color:var(--muted);font-family:ui-monospace,monospace"></span>
      </div>
    </div>

    <!-- logged in: server control -->
    <div id="cabControl" style="display:none">
      <div class="panel-row">
        <div class="field" style="flex:1;min-width:220px">
          <label>Сервер в кабинете (groupId)</label>
          <select id="cabServerSel" style="min-width:240px"></select>
        </div>
        <button class="btn-run" id="cabApplyBtn" onclick="cabSelectServer()" style="flex:0 0 auto;min-width:180px"><span id="cabApplyLabel">Применить сервер</span></button>
      </div>
      <div class="panel-row" style="margin-top:12px">
        <button class="btn-run" id="cabBestBtn" onclick="cabApplyBest()" style="background:linear-gradient(135deg,#f59e0b,#facc15);color:#1a1206;box-shadow:0 8px 22px rgba(250,204,21,.3);flex:0 0 auto;min-width:200px"><span class="svg">__IC_TROPHY__</span><span id="cabBestLabel">Применить лучший</span></button>
        <button class="btn-run" id="cabLogoutBtn" onclick="cabLogout()" style="background:var(--card2);box-shadow:none;flex:0 0 auto;min-width:120px"><span>Выйти</span></button>
        <span id="cabInfo" style="font-size:12px;color:var(--muted);font-family:ui-monospace,monospace;flex:1"></span>
      </div>
    </div>
  </div>

  <div class="panel">
    <div class="panel-row">
      <div class="field">
        <label>Выбор сервера</label>
        <select id="sel"><option value="all">Все серверы</option><option value="current">Текущий сервер</option></select>
      </div>
      <div class="checks">
        <label class="check"><input type="checkbox" id="full" checked> Полный тест (Ping + Скорость)</label>
        <label class="check"><input type="checkbox" id="cabAuto" checked onchange="cabSetAuto(this.checked)"> Авто-применять лучший сервер</label>
      </div>
      <button class="btn-run" id="runBtn" onclick="runTest()"><span class="svg">__IC_PLAY__</span><span id="runLabel">Запустить тест</span></button>
    </div>
  </div>

  <div id="bestHolder"></div>
  <div class="view-bar">
    <span class="title">Серверы</span>
    <span class="spacer"></span>
    <span class="view-toggle">
      <button id="vGrid" class="active" onclick="setView('grid')"><span class="svg">__IC_GRID__</span>Карточки</button>
      <button id="vList" onclick="setView('list')"><span class="svg">__IC_LIST__</span>Строки</button>
    </span>
  </div>
  <div class="cards" id="cards"></div>
  <div class="rows" id="rows" style="display:none"></div>
  <div class="footer-status" id="footStat">Загрузка…</div>

  <div class="row-info">
    <span class="pill"><span class="dot" style="background:var(--best)"></span> Лучший сервер подставляется в плейлист автоматически</span>
    <span>Откройте <a href="/measure">/measure</a> из домашней сети — замеры от вашего IP будут использоваться приоритетно.</span>
  </div>
</div>

<script>
const $ = s => document.querySelector(s);
const IC = {bolt:'__IC_BOLT__',wave:'__IC_WAVE__',down:'__IC_DOWN__',medal:'__IC_MEDAL__',trophy:'__IC_TROPHY__',check:'__IC_CHECK__',thumbs:'__IC_THUMBS__',ping:'__IC_PING__',loss:'__IC_LOSS__',play:'__IC_PLAY__',spinner:'__IC_SPINNER__',grid:'__IC_GRID__',list:'__IC_LIST__'};
const SVG = (name,cls='') => '<span class="svg '+(cls||'')+'">'+IC[name]+'</span>';
const fmt = v => v==null||v===undefined ? '—' : v;
// Quality gauge: 👍 count reflects actual MOS, so the emoji carries info
// instead of being 3 identical thumbs for every good server.
function thumbs(m){
  const sc = m && m.score;
  if(sc!=null) return sc>=85?'👍👍👍':sc>=70?'👍👍':sc>=50?'👍':'❌';
  const mos = m && m.mos;
  if(mos==null) return '';
  if(mos >= 4.20) return '👍👍👍';
  if(mos >= 4.05) return '👍👍';
  if(mos >= 3.80) return '👍';
  return '⚠️';
}
const fmtAge = s => s==null ? '—' : s<60 ? s+'с назад' : s<3600 ? Math.floor(s/60)+'м назад' : Math.floor(s/3600)+'ч '+Math.floor((s%3600)/60)+'м назад';
let autoRefresh = null;
let viewMode = localStorage.tvView || 'grid';
let cabTestRunning = false;

function metricCell(cls, ic, key, val, unit){
  return '<div class="metric '+cls+'"><div class="ic">'+SVG(ic)+'</div>'+
    '<div class="body"><div class="k">'+key+'</div>'+
    '<div class="v">'+val+(unit?'<span class="u">'+unit+'</span>':'')+'</div></div></div>';
}
function bestCard(b, m){
  if(!b) return '';
  return '<div class="best-card">'+
    '<span class="badge-best">'+SVG('trophy')+' Лучший сервер</span>'+
    '<span class="host">'+b+'</span>'+
    '<span class="thumbs">'+thumbs(m)+'</span>'+
    '<span class="summary">Джиттер: <b>'+fmt(m&&m.jitter_ms)+'ms</b> | <b>'+fmt(m&&m.download_mbps)+' Мбит/с</b> | MOS: <b>'+fmt(m&&m.mos)+'</b></span>'+
  '</div>';
}
function serverCard(s, m, isBest){
  const ok = m && m.status==='ok';
  const cls = 'card'+(isBest?' is-best':'')+(ok?'':' fail');
  const mark = ok ? '<div class="ok-mark">'+SVG('check')+'</div>'
                  : '<div class="ok-mark fail">✕</div>';
  const dl = (m&&m.download_mbps!=null)?m.download_mbps+'<span class="u">Мбит/с</span>':'—';
  const jit = (m&&m.jitter_ms!=null)?m.jitter_ms+'<span class="u">ms</span>':'—';
  const qos = (m&&m.qos!=null)?m.qos:'—';
  const mos = (m&&m.mos!=null)?m.mos:'—';
  const score = (m&&m.score!=null)?m.score:'—';
  const scoreOrMos = (m&&m.score!=null)
    ? metricCell('m-mos','medal','Score',score,'')
    : metricCell('m-mos','medal','MOS',mos,'');
  return '<div class="'+cls+'">'+
    '<div class="card-head">'+mark+
      '<span class="host">'+s.host+'</span>'+
      (ok?'<span class="thumbs">'+thumbs(m)+'</span>':'')+
    '</div>'+
    '<div class="card-head" style="margin:-6px 0 11px"><span class="ip">'+(m&&m.ip||'')+'</span></div>'+
    '<div class="grid4">'+
      metricCell('m-jitter','wave','Джиттер',jit,'')+
      metricCell('m-qos','bolt','QoS',qos,'')+
      metricCell('m-down','down','Загрузка',dl,'')+
      scoreOrMos+
    '</div>'+
    '<div class="card-foot"><span>'+SVG('ping')+' Ping: '+fmt(m&&m.ping_ms)+'ms</span><span>'+SVG('loss')+' Loss: '+fmt(m&&m.loss_pct)+'%</span></div>'+
  '</div>';
}
function serverRow(s, m, isBest){
  const ok = m && m.status==='ok';
  const cls = 'row-item'+(isBest?' is-best':'')+(ok?'':' fail');
  const mark = ok ? '<div class="rmark">'+SVG('check')+'</div>' : '<div class="rmark fail">✕</div>';
  const cell = (label,val,extraCls='') => '<div class="rcell '+extraCls+'"><span class="k">'+label+'</span><span class="v">'+fmt(val)+'</span></div>';
  const badge = isBest ? '<div class="rbadge">★ лучший</div>' : '<div class="rbadge"></div>';
  return '<div class="'+cls+'">'+mark+
    '<div class="rhost">'+s.host+' <span class="thumbs" style="font-size:11px">'+thumbs(m)+'</span></div>'+
    cell('Пинг', m&&m.ping_ms!=null?m.ping_ms+' ms':null,'h')+
    cell('Джиттер', m&&m.jitter_ms!=null?m.jitter_ms+' ms':null)+
    cell('QoS', m&&m.qos)+
    cell(m&&m.score!=null?'Score':'MOS', m&&m.score!=null?m.score:m&&m.mos)+
    cell('Загрузка', m&&m.download_mbps!=null?m.download_mbps+' Мбит/с':null)+
    cell('Loss', m&&m.loss_pct!=null?m.loss_pct+'%':null,'h')+
    badge+
  '</div>';
}
function rowsHead(){
  const h = (t,extra='') => '<span class="'+extra+'">'+t+'</span>';
  return '<div class="row-head">'+
    '<span></span>'+h('Сервер')+h('Пинг','h')+h('Джиттер')+h('QoS')+h('Score/MOS')+h('Загрузка')+h('Loss','h')+h('')+
  '</div>';
}
function setView(mode){
  viewMode = mode; localStorage.tvView = mode;
  $('#vGrid').classList.toggle('active', mode==='grid');
  $('#vList').classList.toggle('active', mode==='list');
  $('#cards').style.display = mode==='grid'?'grid':'none';
  const rowsEl = $('#rows');
  rowsEl.style.display = mode==='list'?'flex':'none';
}
async function load(){
  try{
    const r = await fetch('/api/status?_='+Date.now());
    const d = await r.json();
    const all = d.measurements||{};
    const cli = d.client_measurements||{};
    // effective metric per host: prefer fresh client
    const eff = (host) => {
      const c = cli[host];
      const now = Date.now()/1000;
      if(c && (now - (c.ts||0)) <= 3600 && (c.mos!=null || c.score!=null)) return Object.assign({status:'ok'}, c);
      const s = all[host];
      if(s) return s;
      return {status:'pending'};
    };
    const bestHost = d.best_host;
    const bestM = eff(bestHost);
    $('#bestHolder').innerHTML = bestCard(bestHost, bestM);
    // sort: best first, then by score (cabinet) or mos desc
    const rank = m => (m.score!=null? m.score : (m.mos||0));
    const list = (d.servers||[]).slice().sort((a,b)=>{
      if(a.host===bestHost) return -1;
      if(b.host===bestHost) return 1;
      const ma=eff(a.host), mb=eff(b.host);
      return rank(mb)-rank(ma);
    });
    $('#cards').innerHTML = list.map(s=>serverCard(s, eff(s.host), s.host===bestHost)).join('');
    $('#rows').innerHTML = rowsHead()+list.map(s=>serverRow(s, eff(s.host), s.host===bestHost)).join('');
    const urlEl=$('#url'); if(urlEl) urlEl.value = playlistOrigin()+'/plst.m3u8';
    // progress / footer — don't clobber the cabinet-style browser test UI
    if(cabTestRunning){ return; }
    if(d.measure_in_progress){
      $('#footStat').innerHTML = '⏳ Тестируем '+(d.measure_done||0)+'/'+(d.measure_total||d.servers.length)+'… Дождитесь завершения…';
      $('#runBtn').disabled = true;
      $('#runLabel').textContent = 'Тестируем '+(d.measure_done||0)+'/'+(d.measure_total||'…');
      if(!autoRefresh) autoRefresh = setInterval(load, 1500);
    } else {
      $('#footStat').innerHTML = 'Готово. Замеры обновлены '+fmtAge(d.measurements_age_sec)+' · источник выбора: <b style="color:var(--accent)">'+({client:'браузер',server:'сервер',fallback:'fallback'}[d.selection_source]||d.selection_source)+'</b>'+(d.playlist_error?' · <span style="color:#fca5a5">плейлист: '+d.playlist_error+'</span>':'');
      $('#runBtn').disabled = false;
      $('#runLabel').textContent = 'Запустить тест';
      if(autoRefresh){ clearInterval(autoRefresh); autoRefresh=null; }
      loadCabinet();  // refresh cabinet status after measurement cycle (auto-apply may have run)
    }
  }catch(e){ $('#footStat').textContent = 'Ошибка: '+e; }
}
/* ===== Cabinet-style browser speedtest (mirrors new.tv.team exactly) ===== */
/* ping = median of N RTTs to /speedtest/v1/ping; jitter = mean |ΔRTT|;
   download = streamed Mbit/s from /speedtest/v1/download?seconds=N;
   hls = 5 segments -> qos/mos; score = ping15%+jitter15%+dl50%+hls20% (or /0.8). */
const CAB = {
  origin: u => { try { return new URL(u).origin } catch { return CAB.origin('https://'+u.replace(/^https?:\/\//,'')) } },
  fetch: async (url, opt={}, ms=5000) => {
    const ac = new AbortController(); const t = setTimeout(()=>ac.abort(), ms);
    try { return await fetch(url, {...opt, signal:ac.signal, mode:'cors', cache:'no-store'}) }
    finally { clearTimeout(t) }
  },
  ping: async (url, N=10) => {
    const base = CAB.origin(url);
    try { await CAB.fetch(`${base}/speedtest/v1/ping`, {method:'GET'}, 3000) } catch(e){} // warmup
    const rtts=[];
    for(let i=0;i<N;i++){
      const t0=performance.now();
      await CAB.fetch(`${base}/speedtest/v1/ping`, {method:'GET'}, 3000);
      rtts.push(performance.now()-t0);
      await new Promise(r=>setTimeout(r,100));
    }
    const sorted=[...rtts].sort((a,b)=>a-b);
    const median = sorted.length%2===0 ? (sorted[sorted.length/2-1]+sorted[sorted.length/2])/2 : sorted[Math.floor(sorted.length/2)];
    let j=0; for(let i=1;i<rtts.length;i++) j+=Math.abs(rtts[i]-rtts[i-1]);
    const jitter = rtts.length>1 ? j/(rtts.length-1) : 0;
    return { ping: Math.round(median*10)/10, jitter: Math.round(jitter*10)/10 };
  },
  download: async (url, seconds=10) => {
    const base = CAB.origin(url);
    const ac = new AbortController(); const t = setTimeout(()=>ac.abort(), (seconds+10)*1000);
    try {
      const t0 = performance.now(); let bytes=0;
      const r = await fetch(`${base}/speedtest/v1/download?seconds=${seconds}`, {mode:'cors', cache:'no-store', signal:ac.signal});
      if(!r.ok) throw new Error('HTTP '+r.status);
      const rd = r.body.getReader();
      for(;;){ const {done,value}=await rd.read(); if(done) break; if(value) bytes+=value.length }
      const el=(performance.now()-t0)/1000;
      if(!bytes||!el) throw new Error('no data');
      return Math.round(bytes*8/el/1e6*10)/10; // Mbit/s, 1 decimal
    } finally { clearTimeout(t) }
  },
  hls: async (url) => {
    const base = CAB.origin(url);
    await CAB.fetch(`${base}/speedtest/hls/master.m3u8?br=3000`, {}, 5000);
    const lat=[];
    for(let i=0;i<5;i++){
      const t0=performance.now();
      const r = await CAB.fetch(`${base}/speedtest/hls/segment?bytes=188000&jitter=10`, {}, 10000);
      await r.arrayBuffer();
      lat.push(performance.now()-t0);
    }
    const avg = lat.reduce((a,b)=>a+b,0)/lat.length;
    const variance = lat.reduce((a,b)=>a+Math.pow(b-avg,2),0)/lat.length;
    const sd = Math.sqrt(variance);
    const ce = 2*1000/avg;
    let Y=0;
    if(ce>=2) Y+=60; else if(ce>=1) Y+=60*(ce-1);
    const cons = Math.max(0, 100 - sd/avg*100);
    Y += cons/100*40;
    Y = Math.max(0, Math.min(100, Y));
    let mos;
    if(Y>=90) mos=4.5+(Y-90)/20;
    else if(Y>=75) mos=4+(Y-75)/30;
    else if(Y>=60) mos=3.5+(Y-60)/30;
    else if(Y>=40) mos=3+(Y-40)/40;
    else if(Y>=20) mos=2+(Y-20)/20;
    else mos=1+Y/20;
    return { qos: Math.round(Y*10)/10, mos: Math.round(mos*10)/10 };
  },
  // cabinet score (Z): ping15% + jitter15% + download50% + hls20% (or /0.8 without hls)
  score: (r, withHls) => {
    let s=0;
    if(r.ping!=null && !isNaN(r.ping)) s += Math.max(0, 100-r.ping)*0.15;
    if(r.jitter!=null && !isNaN(r.jitter)) s += Math.max(0, 100-r.jitter*10)*0.15;
    if(r.download_mbps!=null && !isNaN(r.download_mbps) && r.download_mbps>0){
      const dl=r.download_mbps; let z;
      if(dl>=200) z=100; else if(dl>=100) z=85+(dl-100)/100*15;
      else if(dl>=50) z=70+(dl-50)/50*15; else if(dl>=25) z=50+(dl-25)/25*20;
      else z=dl/25*50;
      s += z*0.5;
    }
    if(withHls && r.hls_qos!=null && !isNaN(r.hls_qos)) s += r.hls_qos*0.2;
    else s = s/0.8;
    return Math.round(s*10)/10;
  },
  // cabinet quality gate (X): a server may be chosen as "best" only if this passes
  qualify: (r, withHls) => {
    const dl=r.download_mbps, j=r.jitter, mos=r.hls_mos, qos=r.hls_qos;
    if(!dl||isNaN(dl)||!j||isNaN(j)) return false;
    if(withHls && mos!=null && !isNaN(mos) && qos!=null && !isNaN(qos)){
      let b=0;
      if(mos>=4.5) b+=30; else if(mos>=4) b+=25; else if(mos>=3.5) b+=20; else if(mos>=3) b+=15; else b+=(mos-1)/4*15;
      b += qos/100*25;
      b += j<2?25:j<5?20:j<10?15:j<20?10:5;
      b += dl>=100?20:dl>=50?17:dl>=30?14:dl>=15?10:dl/15*10;
      return b>=50;
    }
    return j<10 && dl>30;
  }
};
async function cabTestServer(s, withHls){
  const r = { host:s.host, status:'testing', stage:'ping', ping_ms:null, jitter_ms:null, download_mbps:null, hls_qos:null, hls_mos:null, score:null };
  const deadline = withHls?60000:40000;
  try {
    await Promise.race([(async()=>{
      const p = await CAB.ping(s.url); r.ping_ms=p.ping; r.jitter_ms=p.jitter; r.stage='download';
      r.download_mbps = await CAB.download(s.url, 10);
      if(withHls){ r.stage='hls'; const h=await CAB.hls(s.url); r.hls_qos=h.qos; r.hls_mos=h.mos; }
    })(), new Promise((_,rej)=>setTimeout(()=>rej(new Error('timeout')), deadline))]);
    r.stage='complete'; r.score = CAB.score(r, withHls); r.status='completed';
  } catch(e){ r.status='error'; r.error = (e&&e.message)||'error'; r.score=null; }
  return r;
}
async function runTest(){
  const mode = $('#sel') ? $('#sel').value : 'all';
  const withHls = $('#full') ? $('#full').checked : true;
  $('#runBtn').disabled = true;
  cabTestRunning = true;
  $('#runLabel').innerHTML = SVG('spinner','spin')+' Запускаю…';
  try{
    const r = await fetch('/api/status?_='+Date.now());
    const d = await r.json();
    let servers = (d.servers||[]).map(s=>({host:s.host, url:'https://'+s.host}));
    if(mode==='current'){
      const cab = await fetch('/api/cabinet/state').then(x=>x.json()).catch(()=>({}));
      const cur = cab.current_server_host;
      if(cur) servers = servers.filter(s=>s.host===cur);
    } else if(mode!=='all'){
      servers = servers.filter(s=>s.host===mode);
    }
    if(!servers.length){ $('#footStat').textContent='Нет серверов для теста'; $('#runBtn').disabled=false; $('#runLabel').textContent='Запустить тест'; return; }
    const results=[];
    for(let i=0;i<servers.length;i++){
      const s=servers[i];
      $('#runLabel').innerHTML = SVG('spinner','spin')+' '+(i+1)+'/'+servers.length+' '+s.host;
      $('#footStat').innerHTML = '⏳ '+(i+1)+'/'+servers.length+' · '+s.host+' (ping→download'+(withHls?'→hls':'')+', ~'+(withHls?15:11)+'с/сервер)';
      const res = await cabTestServer(s, withHls);
      results.push(res);
      // report to backend so selection + cards use cabinet metrics
      fetch('/api/report',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({measurements:[{host:s.host, ping_ms:res.ping_ms, jitter_ms:res.jitter_ms,
          download_mbps:res.download_mbps, qos:res.hls_qos, mos:res.hls_mos,
          score:res.score, hls_qos:res.hls_qos, hls_mos:res.hls_mos}]})});
      await load();
    }
    $('#footStat').innerHTML = 'Готово. Протестировано '+results.length+' серверов (cabinet-style).';
    // auto-apply best (highest score among qualified) — uses backend best_host (score-based)
    if($('#cabAuto') && $('#cabAuto').checked){
      const q = results.filter(r=>r.status==='completed' && CAB.qualify(r, withHls));
      if(q.length){
        q.sort((a,b)=>(b.score||0)-(a.score||0));
        $('#footStat').innerHTML += ' Применяю лучший: <b>'+q[0].host+'</b> (score '+q[0].score+')…';
        await fetch('/api/cabinet/apply-best',{method:'POST'});
        loadCabinet();
      } else {
        $('#footStat').innerHTML += ' Нет серверов, прошедших фильтр качества.';
      }
    }
  }catch(e){ $('#footStat').textContent='Ошибка: '+e; }
  cabTestRunning = false;
  $('#runLabel').textContent='Запустить тест'; $('#runBtn').disabled=false;
  loadCabinet();
}
async function loadConfig(){
  try{
    const r = await fetch('/api/config?_='+Date.now());
    const d = await r.json();
    $('#plUrl').value = d.playlist_url || '';
    $('#connAddr').value = d.connect_addr || '';
    cfgConnectAddr = d.connect_addr || '';
    updatePlInfo(d);
    updatePlaylistUrl();
  }catch(e){ $('#plInfo').textContent = 'Ошибка загрузки настроек: '+e; }
}
function updatePlInfo(d){
  const parts = [];
  parts.push('Плейлист: '+(d.has_playlist?'<span style="color:var(--ok2)">✓ загружен</span>':'<span style="color:#fca5a5">не загружен</span>'));
  if(d.playlist_size) parts.push('размер: '+(d.playlist_size/1024).toFixed(1)+' КБ');
  if(d.playlist_age_sec!=null) parts.push('обновлён '+fmtAge(d.playlist_age_sec));
  if(d.playlist_error) parts.push('<span style="color:#fca5a5">ошибка: '+d.playlist_error+'</span>');
  parts.push('авто-замер: каждые '+(d.refresh_measure_minutes||30)+' мин');
  parts.push('авто-плейлист: каждые '+(d.refresh_playlist_minutes||360)+' мин');
  $('#plInfo').innerHTML = parts.join(' · ');
}
async function saveConfig(){
  const url = $('#plUrl').value.trim();
  const conn = $('#connAddr').value.trim();
  if(!url){ $('#plInfo').innerHTML = '<span style="color:#fca5a5">Введите ссылку</span>'; return; }
  $('#saveBtn').disabled = true;
  $('#saveLabel').innerHTML = SVG('spinner','spin')+' Сохраняю…';
  try{
    const r = await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},
      body: JSON.stringify({playlist_url: url, connect_addr: conn})});
    const d = await r.json();
    if(d.ok){
      cfgConnectAddr = d.connect_addr || conn;
      $('#saveLabel').textContent = '✓ Сохранено';
      await load();
      await loadConfig();
      setTimeout(()=>{ $('#saveLabel').textContent='Сохранить'; $('#saveBtn').disabled=false; }, 1800);
    } else {
      $('#plInfo').innerHTML = '<span style="color:#fca5a5">Ошибка: '+(d.error||'неизвестно')+'</span>';
      $('#saveLabel').textContent = 'Сохранить'; $('#saveBtn').disabled=false;
    }
  }catch(e){
    $('#plInfo').innerHTML = '<span style="color:#fca5a5">Ошибка: '+e+'</span>';
    $('#saveLabel').textContent = 'Сохранить'; $('#saveBtn').disabled=false;
  }
}
async function downloadPlaylistFile(){
  $('#dlBtn').disabled = true;
  $('#dlLabel').innerHTML = SVG('spinner','spin')+' Готовлю…';
  try{
    const resp = await fetch('/plst.m3u8?_='+Date.now());
    if(!resp.ok){ $('#plInfo').innerHTML = '<span style="color:#fca5a5">Плейлист ещё не готов (HTTP '+resp.status+'). Сначала сохраните ссылку.</span>'; $('#dlLabel').textContent='Скачать'; $('#dlBtn').disabled=false; return; }
    const blob = await resp.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'playlist.m3u8';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
    $('#dlLabel').textContent = '✓ Скачан';
    setTimeout(()=>{ $('#dlLabel').textContent='Скачать'; $('#dlBtn').disabled=false; }, 1800);
  }catch(e){
    $('#plInfo').innerHTML = '<span style="color:#fca5a5">Ошибка скачивания: '+e+'</span>';
    $('#dlLabel').textContent = 'Скачать'; $('#dlBtn').disabled=false;
  }
}
setView(viewMode);
let cfgConnectAddr = '';
let playlistsLoaded = false;
// extract playListId from a hls.gd/pl/<id>/... URL (for matching the dropdown)
function plIdFromUrl(u){ const m = (u||'').match(/\/pl\/(\d+)\//); return m ? m[1] : ''; }
async function loadPlaylists(){
  if(playlistsLoaded) return;
  const sel = $('#plSel'); if(!sel) return;
  try{
    const r = await fetch('/api/cabinet/playlists?_='+Date.now());
    const d = await r.json();
    if(!d.ok || !d.items || !d.items.length){ return; }
    playlistsLoaded = true;
    const curId = plIdFromUrl($('#plUrl').value);
    // default to TiviMate if current url doesn't match any item
    let defItem = d.items.find(it=>String(it.id)===String(curId))
              || d.items.find(it=>/tivi/i.test(it.name))
              || d.items[0];
    sel.innerHTML = d.items.map(it=>{
      const sel = String(it.id)===String(defItem.id) ? 'selected' : '';
      const dev = (it.devices||'').replace(/<[^>]+>/g,'').slice(0,40);
      return `<option value="${it.link}" ${sel}>${it.name}${dev?(' — '+dev):''}</option>`;
    }).join('');
    // keep #plUrl in sync with the selected playlist link
    if(!$('#plUrl').value || plIdFromUrl($('#plUrl').value)!==String(defItem.id)){
      $('#plUrl').value = defItem.link;
    }
  }catch(e){ /* keep the placeholder option */ }
}
function plToggleCustom(){
  const sel = $('#plSel'), inp = $('#plUrl');
  if(inp.style.display==='none'){ inp.style.display=''; sel.style.display='none'; }
  else { inp.style.display='none'; sel.style.display=''; if(sel.value) inp.value=sel.value; }
}
// sync #plUrl when a playlist is chosen from the dropdown
(function(){
  const sel = $('#plSel');
  if(sel) sel.addEventListener('change',()=>{ if(sel.value) $('#plUrl').value = sel.value; });
})();
function playlistOrigin(){
  const a = (cfgConnectAddr||'').trim();
  if(!a) return location.origin;
  let s = a.replace(/^https?:\/\//i,'').replace(/\/+$/,'');
  return 'http://'+s;
}
function updatePlaylistUrl(){
  const el = $('#url'); if(el) el.value = playlistOrigin()+'/plst.m3u8';
}
async function applyConnAddr(){
  const conn = ($('#connAddr').value||'').trim();
  const btn = $('#applyConn');
  btn.disabled = true; const old=btn.textContent; btn.textContent='⏳ Применяю…';
  try{
    const r = await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},
      body: JSON.stringify({connect_addr: conn})});
    const d = await r.json();
    if('connect_addr' in d){
      const eff = (d.connect_addr||'').trim();
      cfgConnectAddr = eff || conn;
      if(eff) $('#connAddr').value = eff;   // show resolved IP when empty was applied
      updatePlaylistUrl();
      btn.textContent='✓'; setTimeout(()=>btn.textContent=old,1200);
    } else {
      btn.textContent=old; $('#plInfo').innerHTML='<span style="color:#fca5a5">Ошибка: '+(d.error||'')+'</span>';
    }
  }catch(e){ btn.textContent=old; $('#plInfo').innerHTML='<span style="color:#fca5a5">Ошибка: '+e+'</span>'; }
  btn.disabled=false;
}
function copyUrl(){
  const txt = ($('#url').value||'').trim();
  if(!txt) return;
  const btn = $('#copyUrl');
  const done = () => { const o=btn.textContent; btn.textContent='✓'; setTimeout(()=>btn.textContent=o,1200); };
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(txt).then(done).catch(()=>fallbackCopy(txt,done));
  } else { fallbackCopy(txt,done); }
}
function fallbackCopy(txt,cb){
  const ta=document.createElement('textarea'); ta.value=txt; ta.style.position='fixed'; ta.style.opacity='0';
  document.body.appendChild(ta); ta.select(); try{document.execCommand('copy'); cb();}catch(e){}
  document.body.removeChild(ta);
}
loadConfig();
load();
loadCabinet();

/* ---------------- Cabinet ---------------- */
let capData = null;       // {captchaId, baseImage, pieceImage, width, height, knob}
let capDragging = false, capStartX = 0, capOffsetX = 0, capTrail = [];
let capLoginTimer = null;

async function loadCabinet(){
  try{
    const r = await fetch('/api/cabinet/state?_='+Date.now());
    const d = await r.json();
    renderCabinet(d);
  }catch(e){ console.error('cabinet state', e); }
}
function renderCabinet(d){
  const st = $('#cabStatus');
  // pre-fill login from config/profile (password is left empty for the user)
  const uEl = $('#cabUser');
  if(uEl && !uEl.value){ uEl.value = d.cabinet_user || d.user || ''; }
  if(d.logged_in){
    st.innerHTML = '<span class="dot" style="background:var(--ok)"></span> в кабинете';
    $('#cabLogin').style.display = 'none';
    $('#cabControl').style.display = '';
    // fill server select
    const sel = $('#cabServerSel');
    sel.innerHTML = (d.groups||[]).map(g=>`<option value="${g.value}" ${String(g.value)===String(d.current_server_id)?'selected':''}>${g.label}</option>`).join('');
    $('#cabAuto').checked = d.auto_apply;
    let cur = d.current_server_host || (d.current_server_id ? ('id '+d.current_server_id) : '—');
    let info = 'Текущий: '+cur;
    if(d.last_apply_error) info += ' · <span style="color:#fca5a5">ошибка: '+d.last_apply_error+'</span>';
    if(d.last_applied_id) info += ' · последний: id '+d.last_applied_id;
    $('#cabInfo').innerHTML = info;
    // populate the playlist dropdown from the cabinet
    loadPlaylists();
  }else{
    st.innerHTML = '<span class="dot" style="background:var(--warn)"></span> не авторизован';
    $('#cabLogin').style.display = '';
    $('#cabControl').style.display = 'none';
    if(d.login_error) $('#capInfo').innerHTML = '<span style="color:#fca5a5">'+d.login_error+'</span>';
  }
}
async function capGenerate(){
  const user = $('#cabUser').value.trim();
  const pass = $('#cabPass').value;
  if(!user || !pass){ $('#capInfo').innerHTML = '<span style="color:#fca5a5">Введите логин и пароль</span>'; return; }
  $('#capGenBtn').disabled = true;
  $('#capGenLabel').innerHTML = SVG('spinner','spin')+' Получаю…';
  try{
    const r = await fetch('/api/cabinet/captcha/generate',{method:'POST'});
    const d = await r.json();
    if(d.ok && d.captcha){
      capData = d.captcha;
      const base = $('#capBase'), piece = $('#capPiece');
      base.src = capData.baseImage; base.style.display = 'block';
      piece.src = capData.pieceImage; piece.style.display = 'block';
      piece.style.transform = 'translateX(0px)';
      $('#capOverlay').style.display = 'none';
      $('#capLoginBtn').disabled = false;
      $('#capInfo').textContent = 'Потяните ползунок, чтобы совместить кусок с вырезом';
      capOffsetX = 0; capTrail = [];
      $('#capKnob').style.left = '0px';
    } else $('#capInfo').innerHTML = '<span style="color:#fca5a5">Не удалось получить captcha</span>';
  }catch(e){ $('#capInfo').innerHTML = '<span style="color:#fca5a5">Ошибка: '+e+'</span>'; }
  $('#capGenLabel').textContent = 'Получить captcha'; $('#capGenBtn').disabled = false;
}
// slider drag
(function(){
  const knob = $('#capKnob'), track = $('#capTrack'), piece = $('#capPiece');
  const W = ()=>track.clientWidth - knob.offsetWidth;
  function down(e){
    if(!capData) return;
    capDragging = true; capTrail = [];
    capStartX = (e.touches?e.touches[0].clientX:e.clientX) - knob.offsetLeft;
    knob.setPointerCapture && e.pointerId!=null && knob.setPointerCapture(e.pointerId);
    e.preventDefault();
  }
  function move(e){
    if(!capDragging) return;
    let x = (e.touches?e.touches[0].clientX:e.clientX) - capStartX;
    x = Math.max(0, Math.min(x, W()));
    capOffsetX = x;
    knob.style.left = x+'px';
    piece.style.transform = 'translateX('+x+'px)';
    capTrail.push({x: Math.round(x), t: Math.round(performance.now())});
  }
  function up(){
    if(!capDragging) return;
    capDragging = false;
    capVerify();
  }
  knob.addEventListener('mousedown', down);
  knob.addEventListener('touchstart', down, {passive:false});
  document.addEventListener('mousemove', move);
  document.addEventListener('touchmove', move, {passive:false});
  document.addEventListener('mouseup', up);
  document.addEventListener('touchend', up);
})();
async function capVerify(){
  if(!capData) return;
  $('#capInfo').innerHTML = SVG('spinner','spin')+' Проверка…';
  try{
    const r = await fetch('/api/cabinet/captcha/verify',{method:'POST',headers:{'Content-Type':'application/json'},
      body: JSON.stringify({captchaId: capData.captchaId, offsetX: Math.round(capOffsetX), trail: capTrail})});
    const d = await r.json();
    if(d.valid){
      $('#capInfo').innerHTML = '<span style="color:var(--ok2)">✓ Капча пройдена — вхожу…</span>';
      await capLogin(d.proof);
    }else{
      $('#capInfo').innerHTML = '<span style="color:#fca5a5">Неверное смещение: '+(d.error||'')+'</span>. Получите новую captcha.';
      capData = null; $('#capLoginBtn').disabled = true;
    }
  }catch(e){ $('#capInfo').innerHTML = '<span style="color:#fca5a5">Ошибка: '+e+'</span>'; }
}
async function capLogin(proof){
  const user = $('#cabUser').value.trim();
  const pass = $('#cabPass').value;
  if(!user || !pass){ $('#capInfo').innerHTML = '<span style="color:#fca5a5">Введите логин и пароль</span>'; $('#capLoginLabel').textContent = 'Войти'; $('#capLoginBtn').disabled = false; return; }
  $('#capLoginBtn').disabled = true;
  $('#capLoginLabel').innerHTML = SVG('spinner','spin')+' Вход…';
  try{
    const r = await fetch('/api/cabinet/login',{method:'POST',headers:{'Content-Type':'application/json'},
      body: JSON.stringify({user: user, password: pass, captchaId: capData?capData.captchaId:'', proof: proof})});
    const d = await r.json();
    if(d.ok){
      $('#capInfo').innerHTML = '<span style="color:var(--ok2)">✓ Вход выполнен</span>';
      renderCabinet(d.state);
    }else{
      $('#capInfo').innerHTML = '<span style="color:#fca5a5">Вход не удался: '+(d.error||'')+'</span>';
      renderCabinet(d.state||{});
    }
  }catch(e){ $('#capInfo').innerHTML = '<span style="color:#fca5a5">Ошибка: '+e+'</span>'; }
  $('#capLoginLabel').textContent = 'Войти'; $('#capLoginBtn').disabled = false;
}
async function cabSelectServer(){
  const sid = $('#cabServerSel').value;
  $('#cabApplyBtn').disabled = true;
  $('#cabApplyLabel').innerHTML = SVG('spinner','spin')+' Применяю…';
  try{
    const r = await fetch('/api/cabinet/select',{method:'POST',headers:{'Content-Type':'application/json'},
      body: JSON.stringify({server_id: sid})});
    const d = await r.json();
    if(d.ok){
      $('#cabInfo').innerHTML = '<span style="color:var(--ok2)">✓ Сервер применён, плейлист прогревается…</span>';
      renderCabinet(d.state);
      let n=0;
      const poll = setInterval(()=>{ loadCabinet(); load(); if(++n>=6){ clearInterval(poll); } }, 3000);
    }
    else $('#cabInfo').innerHTML = '<span style="color:#fca5a5">Ошибка: '+(d.error||'')+'</span>';
  }catch(e){ $('#cabInfo').innerHTML = '<span style="color:#fca5a5">Ошибка: '+e+'</span>'; }
  $('#cabApplyLabel').textContent = 'Применить сервер'; $('#cabApplyBtn').disabled = false;
}
async function cabApplyBest(){
  $('#cabBestBtn').disabled = true;
  $('#cabBestLabel').innerHTML = SVG('spinner','spin')+' Применяю…';
  try{
    await fetch('/api/cabinet/apply-best',{method:'POST'});
    $('#cabInfo').innerHTML = '<span style="color:var(--ok2)">✓ Запущено — лучший сервер применяется в кабинете, плейлист прогревается…</span>';
    // warm-swap runs in background; poll cabinet + cards a few times
    let n=0;
    const poll = setInterval(()=>{ loadCabinet(); load(); if(++n>=6){ clearInterval(poll); } }, 3000);
  }catch(e){ $('#cabInfo').innerHTML = '<span style="color:#fca5a5">Ошибка: '+e+'</span>'; }
  $('#cabBestLabel').textContent = 'Применить лучший'; $('#cabBestBtn').disabled = false;
}
async function cabSetAuto(on){
  await fetch('/api/cabinet/auto',{method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({enabled: on})});
  $('#cabInfo').innerHTML = 'Авто-применение '+(on?'включено':'выключено');
}
async function cabLogout(){
  await fetch('/api/cabinet/logout',{method:'POST'});
  loadCabinet();
}
setInterval(()=>{ if(!autoRefresh) load(); }, 8000);
</script>
</body>
</html>
"""


MEASURE_HTML = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Тест из браузера · hls.gd</title>
<style>
  :root{--bg:#0a0e1a;--bg2:#111726;--card:#161d2f;--border:#243056;--text:#e6ecff;
        --muted:#8b96b8;--dim:#5b6688;--primary:#6366f1;--accent:#22d3ee;--ok:#22c55e;
        --err:#ef4444;--best:#facc15;--blue:#60a5fa;--purple:#a78bfa;--orange:#fb923c;--green:#34d399;
        --grad:linear-gradient(135deg,#16a34a,#22c55e)}
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,sans-serif;
       background:radial-gradient(900px 500px at 50% -10%,#1e2a55 0%,transparent 60%),var(--bg);
       color:var(--text);min-height:100vh;padding:26px 16px 60px}
  .wrap{max-width:1280px;margin:0 auto}
  header{display:flex;align-items:center;gap:14px;margin-bottom:18px}
  .logo{width:46px;height:46px;border-radius:12px;background:var(--grad);display:grid;place-items:center;color:white}
  .logo .svg{width:24px;height:24px}
  h1{font-size:20px;font-weight:800}
  .sub{color:var(--muted);font-size:13px;margin-top:2px}
  .note{background:rgba(34,211,238,.08);border:1px solid rgba(34,211,238,.25);
        border-radius:12px;padding:13px 15px;color:#b6e7ee;font-size:13px;margin-bottom:16px;line-height:1.6}
  .note b{color:var(--accent)}
  .controls{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px;align-items:center}
  button{cursor:pointer;border:none;background:var(--grad);color:white;
         padding:12px 20px;border-radius:11px;font-size:14px;font-weight:700;font-family:inherit;
         box-shadow:0 8px 22px rgba(34,197,94,.35);transition:.18s;display:flex;align-items:center;gap:9px}
  button:hover{transform:translateY(-1px)}
  button.ghost{background:var(--card);border:1px solid var(--border);color:var(--text);box-shadow:none}
  button:disabled{opacity:.55;cursor:not-allowed;transform:none}
  button .svg{width:16px;height:16px}
  .svg{display:inline-block;vertical-align:middle;width:1em;height:1em;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
  .spin{animation:sp .8s linear infinite}
  @keyframes sp{to{transform:rotate(360deg)}}
  #status{font-size:13px;color:var(--muted);margin:8px 0 14px;min-height:20px;font-family:ui-monospace,monospace}
  .cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(255px,1fr));gap:11px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:13px;padding:13px}
  .card.is-best{border-color:var(--best);box-shadow:0 0 0 1px var(--best),0 10px 28px rgba(250,204,21,.12)}
  .card.pending{opacity:.7}
  .ch{display:flex;align-items:center;gap:8px;margin-bottom:9px}
  .ch .host{font-family:ui-monospace,monospace;font-weight:700;font-size:13px}
  .ch .badge{margin-left:auto;font-size:10px;font-weight:700;padding:2px 7px;border-radius:6px;background:rgba(34,197,94,.15);color:#4ade80}
  .ch .badge.best{background:var(--grad);color:#1a1206}
  .grid4{display:grid;grid-template-columns:1fr 1fr;gap:8px}
  .m{background:var(--bg2);border:1px solid rgba(36,48,86,.5);border-radius:9px;padding:8px 10px;display:flex;align-items:center;gap:8px}
  .m .ic{width:24px;height:24px;border-radius:7px;display:grid;place-items:center;flex-shrink:0}
  .m .ic .svg{width:14px;height:14px}
  .mj .ic{background:rgba(96,165,250,.15);color:var(--blue)}
  .mq .ic{background:rgba(167,139,250,.15);color:var(--purple)}
  .md .ic{background:rgba(52,211,153,.15);color:var(--green)}
  .mm .ic{background:rgba(251,146,60,.15);color:var(--orange)}
  .m .v{font-size:14px;font-weight:800;font-variant-numeric:tabular-nums}
  .m .k{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.4px;font-weight:600}
  .foot{margin-top:10px;padding-top:8px;border-top:1px solid rgba(36,48,86,.5);font-size:11px;color:var(--muted);font-family:ui-monospace,monospace;display:flex;gap:12px}
  .result{margin-top:18px;padding:14px;border-radius:13px;border:1px dashed var(--border);background:rgba(99,102,241,.06);font-family:ui-monospace,monospace;font-size:13px;line-height:1.7}
  .result b{color:var(--best)}
  .data-note{margin-top:10px;color:var(--dim);font-size:11px}
  footer{margin-top:24px;color:var(--dim);font-size:12px;text-align:center}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="logo"><span class="svg">__IC_BOLT__</span></div>
    <div><h1>Тест скорости из браузера</h1><div class="sub">Ping · Джиттер · Загрузка · QoS · MOS — от вашего IP</div></div>
  </header>
  <div class="note">
    ⚠️ <b>Откройте эту страницу из домашней сети</b> (где стоит ТВ/плеер) — например, с телефона в той же Wi-Fi.
    Замеры выполняются <b>браузером</b> и отправляются на сервер; они используются приоритетно при выборе лучшего хоста.
    VPS не может сделать честный HLS-тест (токен привязан к IP), поэтому этот тест — основной способ получить
    релевантные для вашего ТВ цифры.
  </div>
  <div class="controls">
    <button id="run" onclick="runTest()"><span class="svg">__IC_PLAY__</span> Запустить тест</button>
    <button class="ghost" onclick="location.href='/'">← Назад</button>
  </div>
  <div id="status"></div>
  <div class="cards" id="cards"></div>
  <div class="data-note">Объём данных: ~27 × 2 МБ = <b>~54 МБ</b> (скачивание 2 МБ с каждого сервера для замера скорости) + 5×27 tiny ping.</div>
  <div class="result" id="result" style="display:none"></div>
  <footer>Браузер: 5 ping (TCP через fetch) → Ping/Джиттер/Loss; 1 загрузка 2 МБ → Мбит/с; QoS и MOS вычисляются</footer>
</div>
<script>
const HOSTS = __HOSTS__;
const SPEEDTEST_PATH = "__SPEEDTEST_PATH__";
// Match the page's scheme so cross-origin requests to hls.gd are not blocked
// as mixed content (e.g. when this page is served via https://tv.cybr.com,
// fetching http://hls.gd/... would be blocked). hls.gd supports both schemes
// and returns Access-Control-Allow-Origin: * on both.
const PROTO = location.protocol === 'https:' ? 'https:' : 'http:';
const $ = s => document.querySelector(s);
// Host-keyed element IDs contain dots (e.g. "d-14.hls.gd"), which querySelector
// would misparse (dots = class selector). getElementById treats the string
// literally, so use it for all host-keyed lookups.
const E = id => document.getElementById(id);
const IC = {bolt:'__IC_BOLT__',wave:'__IC_WAVE__',down:'__IC_DOWN__',medal:'__IC_MEDAL__',play:'__IC_PLAY__',spinner:'__IC_SPINNER__',ping:'__IC_PING__',loss:'__IC_LOSS__'};
const SVG = (n,cls='') => '<span class="svg '+(cls||'')+'">'+IC[n]+'</span>';
function setStatus(t){ $('#status').textContent = t; }
function stddev(xs){const n=xs.length;if(n<2)return 0;const m=xs.reduce((a,b)=>a+b,0)/n;return Math.sqrt(xs.reduce((a,b)=>a+(b-m)*(b-m),0)/(n-1));}
function qos(p,j,l,dl){let q=100;q-=l;q-=Math.min(j*0.5,15);q-=Math.min(Math.max(0,p-100)*0.1,15);if(dl!=null&&dl<5)q-=(5-dl)*4;return Math.max(0,Math.min(100,Math.round(q*10)/10));}
function mos(p,j,l,dl){let e=p+2*j+10*l;let r=93.2-(e/40);if(dl!=null&&dl<10)r-=(10-dl)*1.2;r=Math.max(0,Math.min(100,r));let m=1+0.035*r+0.000007*r*(r-60)*(100-r);return Math.max(1,Math.min(4.5,Math.round(m*100)/100));}

function buildCards(){
  $('#cards').innerHTML = HOSTS.map(h=>
    '<div class="card pending" id="c-'+h+'">'+
      '<div class="ch"><span class="host">'+h+'</span><span class="badge">ожидание</span></div>'+
      '<div class="grid4">'+
        '<div class="m mj"><div class="ic">'+SVG('wave')+'</div><div><div class="k">Джиттер</div><div class="v" id="j-'+h+'">—</div></div></div>'+
        '<div class="m mq"><div class="ic">'+SVG('bolt')+'</div><div><div class="k">QoS</div><div class="v" id="q-'+h+'">—</div></div></div>'+
        '<div class="m md"><div class="ic">'+SVG('down')+'</div><div><div class="k">Загрузка</div><div class="v" id="d-'+h+'">—</div></div></div>'+
        '<div class="m mm"><div class="ic">'+SVG('medal')+'</div><div><div class="k">MOS</div><div class="v" id="o-'+h+'">—</div></div></div>'+
      '</div>'+
      '<div class="foot"><span id="p-'+h+'">Ping —</span><span id="l-'+h+'">Loss —</span></div>'+
    '</div>').join('');
}
async function pingHost(host){
  const lats=[]; let fail=0;
  const pEl=E('p-'+host);
  for(let i=0;i<5;i++){
    const t0=performance.now();
    const ctrl=new AbortController(); const to=setTimeout(()=>ctrl.abort(),5000);
    let ok=true;
    try{ await fetch(PROTO+'//'+host+'/', {mode:'no-cors', cache:'no-store', redirect:'manual', signal:ctrl.signal}); lats.push(performance.now()-t0); }
    catch(e){ fail++; lats.push(performance.now()-t0); ok=false; } // even failed attempts give a latency proxy
    clearTimeout(to);
    if(pEl){ const last=Math.round(lats[lats.length-1]*10)/10; pEl.textContent='Ping '+(i+1)+'/5: '+last+'ms'+(ok?'':' ✕'); }
    await new Promise(r=>setTimeout(r,60));
  }
  lats.sort((a,b)=>a-b);
  const ping = Math.round(lats[0]*10)/10;
  const jitter = Math.round(stddev(lats)*10)/10;
  const loss = Math.round(fail/5*100*10)/10;
  return {ping, jitter, loss};
}
async function downloadHost(host){
  // No Range header: Range triggers a CORS preflight (OPTIONS) which hls.gd
  // does not answer, so the browser would block the request. A plain GET is a
  // "simple" CORS request (no preflight) and hls.gd returns
  // Access-Control-Allow-Origin: *, so we can read the body. We stream-read
  // ~2 MB and cancel, to keep the data volume bounded per host. The live
  // Mbit/s is shown in the card as bytes arrive.
  const t0=performance.now();
  const TARGET=2*1024*1024;
  const ctrl=new AbortController(); const to=setTimeout(()=>ctrl.abort(),8000);
  const dEl=E('d-'+host);
  let lastTick=0;
  try{
    const r = await fetch(PROTO+'//'+host+SPEEDTEST_PATH, {mode:'cors', cache:'no-store', signal:ctrl.signal});
    if(r.body && r.body.getReader){
      const reader=r.body.getReader(); let received=0;
      while(received<TARGET){
        const {done,value}=await reader.read();
        if(done) break;
        if(value){
          received+=value.length;
          if(dEl && received-lastTick>200000){
            lastTick=received;
            const dt=(performance.now()-t0)/1000;
            if(dt>0) dEl.textContent=(Math.round(received*8/dt/1e6*100)/100)+' Мбит/с …';
          }
        }
      }
      try{ reader.cancel(); }catch(e){}
      clearTimeout(to);
      const dt=(performance.now()-t0)/1000;
      if(received>100000 && dt>0) return Math.round(received*8/dt/1e6*100)/100;
    } else {
      const buf=await r.arrayBuffer();
      clearTimeout(to);
      const dt=(performance.now()-t0)/1000;
      if(buf.byteLength>10000 && dt>0) return Math.round(buf.byteLength*8/dt/1e6*100)/100;
    }
  }catch(e){ clearTimeout(to); }
  return null;
}
function setCard(h, m){
  const c=E('c-'+h); if(!c) return;
  c.classList.remove('pending');
  // tolerate both naming styles: success uses jitter_ms/ping_ms/loss_pct,
  // the catch branch uses jitter/ping/loss.
  const j = m.jitter_ms!=null?m.jitter_ms:m.jitter;
  const p = m.ping_ms!=null?m.ping_ms:m.ping;
  const l = m.loss_pct!=null?m.loss_pct:m.loss;
  E('j-'+h).textContent = j!=null?j+'ms':'—';
  E('q-'+h).textContent = m.qos!=null?m.qos:'—';
  E('d-'+h).textContent = m.download_mbps!=null?m.download_mbps+' Мбит/с':'—';
  E('o-'+h).textContent = m.mos!=null?m.mos:'—';
  E('p-'+h).textContent = 'Ping '+(p!=null?p+'ms':'—');
  E('l-'+h).textContent = 'Loss '+(l!=null?l+'%':'—');
  c.querySelector('.badge').textContent = m.mos!=null?('OK · '+m.mos):'fail';
}
async function runTest(){
  $('#run').disabled=true;
  setStatus('Тестирую '+HOSTS.length+' серверов (5 ping + 2 МБ загрузка на каждый)…');
  buildCards();
  const results=[];
  const CONC=4; let idx=0;
  async function worker(){
    while(idx<HOSTS.length){
      const i=idx++; const h=HOSTS[i];
      const bc=E('c-'+h); if(bc){ bc.classList.remove('pending'); bc.querySelector('.badge').textContent='ping…'; }
      setStatus('Тестируем '+results.length+'/'+HOSTS.length+' · '+h+' (ping…)');
      try{
        const p = await pingHost(h);
        if(bc){ bc.querySelector('.badge').textContent='загрузка…'; }
        setStatus('Тестируем '+results.length+'/'+HOSTS.length+' · '+h+' (загрузка…)');
        const dl = await downloadHost(h);
        const q = qos(p.ping,p.jitter,p.loss,dl);
        const mo = mos(p.ping,p.jitter,p.loss,dl);
        const m = {host:h, ping_ms:p.ping, jitter_ms:p.jitter, loss_pct:p.loss, download_mbps:dl, qos:q, mos:mo};
        results.push(m); setCard(h,m);
      }catch(e){
        results.push({host:h, ping_ms:null, jitter_ms:null, loss_pct:100, download_mbps:null, qos:0, mos:1});
        setCard(h,{ping:null,jitter:null,loss:100,download_mbps:null,qos:0,mos:null});
      }
      setStatus('Тестируем '+results.length+'/'+HOSTS.length+'…');
    }
  }
  await Promise.all(Array.from({length:CONC}, worker));
  // mark best
  results.sort((a,b)=>(b.mos||0)-(a.mos||0));
  const best = results.find(r=>r.mos!=null);
  if(best){ const bc=E('c-'+best.host); if(bc){ bc.classList.add('is-best'); bc.querySelector('.badge').classList.add('best'); bc.querySelector('.badge').textContent='ЛУЧШИЙ'; } }
  setStatus('Готово. Лучший: '+(best?best.host+' (MOS '+best.mos+', '+best.download_mbps+' Мбит/с)':'—')+'. Отправляю на сервер…');
  try{
    const r=await fetch('/api/report',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({measurements:results})});
    const d=await r.json();
    $('#result').style.display='block';
    const origin = mOrigin();
    $('#result').innerHTML='✅ Отправлено. Сервер выбрал: <b>'+d.best_host+'</b> (источник: '+d.selection_source+')<br>Плейлист: <a href="/plst.m3u8">'+origin+'/plst.m3u8</a>';
    setStatus('Готово. Данные сохранены и применены к плейлисту.');
  }catch(e){ setStatus('Ошибка отправки: '+e); }
  $('#run').disabled=false;
}
let mConnectAddr='';
function mOrigin(){
  const a=(mConnectAddr||'').trim();
  if(!a) return location.origin;
  return 'http://'+a.replace(/^https?:\/\//i,'').replace(/\/+$/,'');
}
(async function(){ try{ const r=await fetch('/api/config?_='+Date.now()); const d=await r.json(); mConnectAddr=d.connect_addr||''; }catch(e){} })();
buildCards();
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "tv-plst/1.0"

    @property
    def state(self) -> State:
        return self.server.state  # type: ignore[attr-defined]

    # silence default noisy logging
    def log_message(self, fmt: str, *args: Any) -> None:
        pass

    # --- helpers --------------------------------------------------------- #
    def _send(self, code: int, body: bytes, ctype: str,
              extra: dict[str, str] | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass

    def _send_json(self, obj: Any, code: int = 200) -> None:
        self._send(code, _json_bytes(obj), "application/json; charset=utf-8")

    def _client_ip(self) -> str:
        xff = self.headers.get("X-Forwarded-For")
        if xff:
            return xff.split(",")[0].strip()
        return self.client_address[0] if self.client_address else "?"

    # --- routes ---------------------------------------------------------- #
    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        st = self.state

        if path == "/" or path == "/index.html":
            html = self._render_dashboard()
            self._send(200, html.encode("utf-8"), "text/html; charset=utf-8")
            return

        if path == "/logo.png" or path == "/favicon.png":
            try:
                with open(os.path.join(HERE, "logo.png"), "rb") as f:
                    data = f.read()
                self._send(200, data, "image/png",
                           {"Cache-Control": "public, max-age=3600"})
            except Exception:  # noqa: BLE001
                self._send(404, b"logo not found\n", "text/plain")
            return

        if path == "/measure":
            # The standalone browser-test page was removed; the test lives on
            # the main dashboard now. Redirect old links/bookmarks home.
            self._send(302, b"", "text/plain", {"Location": "/"})
            return

        if path == "/playlist.m3u8" or path == "/plst.m3u8":
            # Build an absolute base for the proxied channel URLs. Prefer the
            # configured "connect address" (the host[:port] the player uses to
            # reach us, e.g. "tv.cybr.com") so the playlist is valid regardless
            # of how this request arrived. Fall back to the request Host header.
            cfg = self.server.config  # type: ignore[attr-defined]
            base = _base_from_authority(cfg.get("connect_addr", "") or "")
            if not base:
                host_hdr = self.headers.get("Host", "")
                if host_hdr:
                    hh, _, port = host_hdr.partition(":")
                    base = f"http://{hh}" + (f":{port}" if port and port != "80" else "")
                    base = base + "/"
            data = st.served_playlist(base)
            if not data:
                self._send(503, b"playlist not ready yet\n", "text/plain; charset=utf-8")
                return
            self._send(200, data, "application/vnd.apple.mpegurl; charset=utf-8",
                       {"Cache-Control": "no-cache, no-store, must-revalidate"})
            return

        if path.startswith("/p/"):
            self._handle_proxy(path)
            return

        if path == "/api/status":
            self._send_json(st.snapshot())
            return

        if path == "/api/config":
            cfg = self.server.config  # type: ignore[attr-defined]
            with st.lock:
                self._send_json({
                    "playlist_url": cfg.get("playlist_url", ""),
                    "connect_addr": cfg.get("connect_addr", "") or "",
                    "has_playlist": bool(st.playlist_raw),
                    "playlist_size": len(st.playlist_raw),
                    "playlist_age_sec": int(time.time() - st.playlist_updated_at) if st.playlist_updated_at else None,
                    "playlist_error": st.playlist_error,
                    "refresh_playlist_minutes": cfg.get("refresh_playlist_minutes", 360),
                    "refresh_measure_minutes": cfg.get("refresh_measure_minutes", 30),
                    "listen_port": cfg.get("listen_port", 8080),
                    "rewrite_hosts": bool(cfg.get("rewrite_hosts", False)),
                    "proxy_streams": bool(cfg.get("proxy_streams", False)),
                })
            return

        if path == "/api/cabinet/state":
            self._send_json(self.server.cabinet.snapshot())  # type: ignore[attr-defined]
            return

        if path == "/api/cabinet/playlists":
            cabst = self.server.cabinet  # type: ignore[attr-defined]
            ok, items, err = cabst.cabinet.get_playlists()
            self._send_json({"ok": ok, "items": items, "error": err})
            return

        if path == "/favicon.ico":
            # serve the PNG logo as the icon (browsers accept image/png for .ico)
            try:
                with open(os.path.join(HERE, "logo.png"), "rb") as f:
                    data = f.read()
                self._send(200, data, "image/x-icon",
                           {"Cache-Control": "public, max-age=3600"})
            except Exception:  # noqa: BLE001
                self._send(204, b"", "image/x-icon")
            return

        self._send(404, b"not found\n", "text/plain; charset=utf-8")

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0]
        st = self.state
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""

        if path == "/api/report":
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception as e:  # noqa: BLE001
                self._send_json({"error": f"bad json: {e}"}, 400)
                return
            items = payload.get("measurements") or []
            ip = self._client_ip()
            ok = 0
            for it in items:
                host = it.get("host")
                if host and any(s["host"] == host for s in st.servers):
                    st.update_client_measure(host, it, ip)
                    ok += 1
            with st.lock:
                best = st.best_host
                src = st.last_selection_source
            self._send_json({"accepted": ok, "best_host": best,
                             "selection_source": src})
            return

        if path == "/api/measure":
            config = self.server.config  # type: ignore[attr-defined]
            timeout = float(config.get("measure_timeout_seconds", 3))
            mode = "all"
            full = True
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
                mode = str(payload.get("mode", "all")).strip() or "all"
                full = payload.get("full", True)
                if not isinstance(full, bool):
                    full = bool(full)
            except Exception:  # noqa: BLE001
                pass
            hosts: list[str] | None = None
            merge = False
            if mode == "current":
                cabcur = self.server.cabinet  # type: ignore[attr-defined]
                with cabcur.lock:
                    cur_id = str(cabcur.profile.get("groupId", "") or "")
                    cur_host = ""
                    for g in cabcur.groups:
                        if str(g.get("value", "")) == cur_id:
                            cur_host = g.get("label", "")
                            break
                if cur_host and any(s["host"] == cur_host for s in st.servers):
                    hosts = [cur_host]
                    merge = True
                else:
                    mode = "all"  # no current server known -> full sweep
            cfg = self.server.config  # type: ignore[attr-defined]
            cab = self.server.cabinet  # type: ignore[attr-defined]
            threading.Thread(
                target=_run_measure_then_apply,
                args=(st, cab, cfg.get("playlist_url", ""), timeout,
                      full, hosts, merge),
                daemon=True,
            ).start()
            with st.lock:
                best = st.best_host
                src = st.last_selection_source
            self._send_json({"started": True, "mode": mode, "full": full,
                             "best_host": best, "selection_source": src})
            return

        if path == "/api/reload":
            threading.Thread(target=self._trigger_reload, daemon=True).start()
            with st.lock:
                size = len(st.playlist_raw)
                err = st.playlist_error
            self._send_json({"started": True, "size": size, "error": err,
                             "ok": bool(size)})
            return

        if path == "/api/config":
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception as e:  # noqa: BLE001
                self._send_json({"ok": False, "error": f"bad json: {e}"}, 400)
                return
            url = (payload.get("playlist_url") or "").strip()
            conn = (payload.get("connect_addr") or "").strip()
            cfg = self.server.config  # type: ignore[attr-defined]
            # playlist_url is required overall (must already be set); reject only
            # if the user is actively clearing it.
            if "playlist_url" in payload and not url:
                self._send_json({"ok": False, "error": "playlist_url пустой"}, 400)
                return
            try:
                changed_url = False
                if url and url != cfg.get("playlist_url"):
                    cfg["playlist_url"] = url
                    changed_url = True
                # connect_addr: empty string means "use the IP I'm browsing to"
                # — resolve it from the request Host header so the field shows
                # the user's actual IP and the playlist URLs are absolute.
                if "connect_addr" in payload:
                    if not conn:
                        host_hdr = self.headers.get("Host", "")
                        if host_hdr:
                            hh, _, port = host_hdr.partition(":")
                            conn = hh + (f":{port}" if port and port != "80" else "")
                    cfg["connect_addr"] = conn
                with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, indent=2, ensure_ascii=False)
            except Exception as e:  # noqa: BLE001
                self._send_json({"ok": False, "error": f"save failed: {e}"}, 500)
                return
            # reload the playlist only if the URL actually changed
            if changed_url:
                self._trigger_reload()
                with st.lock:
                    size = len(st.playlist_raw)
                    err = st.playlist_error
            else:
                with st.lock:
                    size = len(st.playlist_raw)
                    err = st.playlist_error
            self._send_json({
                "ok": bool(size), "size": size, "error": err,
                "playlist_url": cfg.get("playlist_url", ""),
                "connect_addr": cfg.get("connect_addr", "") or "",
            })
            return

        # --- Cabinet API ------------------------------------------------- #
        cab = self.server.cabinet  # type: ignore[attr-defined]

        if path == "/api/cabinet/captcha/generate":
            cap = cab.cabinet.captcha_generate()
            with cab.lock:
                cab.current_captcha = cap
                cab.last_proof = ""
            self._send_json({"ok": bool(cap), "captcha": cap})
            return

        if path == "/api/cabinet/captcha/verify":
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception as e:  # noqa: BLE001
                self._send_json({"ok": False, "error": f"bad json: {e}"}, 400)
                return
            with cab.lock:
                cap = cab.current_captcha
            captcha_id = payload.get("captchaId") or cap.get("captchaId", "")
            offset_x = int(payload.get("offsetX", 0))
            trail = payload.get("trail", []) or []
            valid, proof = cab.cabinet.captcha_verify(captcha_id, offset_x, trail)
            with cab.lock:
                if valid:
                    cab.last_proof = proof
            self._send_json({"valid": valid, "proof": proof if valid else "",
                             "error": "" if valid else proof})
            return

        if path == "/api/cabinet/login":
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception as e:  # noqa: BLE001
                self._send_json({"ok": False, "error": f"bad json: {e}"}, 400)
                return
            with cab.lock:
                cap = cab.current_captcha
                proof = payload.get("proof") or cab.last_proof
                user = payload.get("user") or cab.user
                password = payload.get("password") or cab.password
            captcha_id = payload.get("captchaId") or cap.get("captchaId", "")
            if not user or not password:
                self._send_json({"ok": False, "error": "нет логина/пароля (задайте в config.json)"}, 400)
                return
            if not captcha_id or not proof:
                self._send_json({"ok": False, "error": "сначала решите captcha"}, 400)
                return
            ok, err = cab.cabinet.login(user, password, captcha_id, proof)
            with cab.lock:
                cab.logged_in = ok
                cab.login_error = "" if ok else err
                if ok:
                    cab.user = user
                    cab.password = password
            if ok:
                # persist credentials to config.json so background workers and
                # restarts keep using them (user entered them via the UI)
                try:
                    cfg = self.server.config  # type: ignore[attr-defined]
                    cfg["cabinet_user"] = user
                    cfg["cabinet_password"] = password
                    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                        json.dump(cfg, f, indent=2, ensure_ascii=False)
                except Exception:  # noqa: BLE001
                    pass
                cab.refresh_status()
                # after login, apply best server immediately (forced)
                cfg = self.server.config  # type: ignore[attr-defined]
                threading.Thread(
                    target=_apply_best_to_cabinet,
                    args=(st, cab, cfg.get("playlist_url", ""), True),
                    daemon=True).start()
            self._send_json({"ok": ok, "error": err,
                             "state": cab.snapshot()})
            return

        if path == "/api/cabinet/logout":
            cab.cabinet.logout()
            with cab.lock:
                cab.logged_in = False
                cab.profile = {}
                cab.groups = []
            self._send_json({"ok": True})
            return

        if path == "/api/cabinet/select":
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception as e:  # noqa: BLE001
                self._send_json({"ok": False, "error": f"bad json: {e}"}, 400)
                return
            sid = str(payload.get("server_id", "")).strip()
            if not sid:
                self._send_json({"ok": False, "error": "server_id пустой"}, 400)
                return
            ok, err = cab.cabinet.set_server(sid)
            with cab.lock:
                if ok:
                    cab.last_applied_id = sid
                    cab.last_applied_at = time.time()
                cab.last_apply_error = "" if ok else err
            if ok:
                cab.refresh_status()
                # warm reload in background: re-fetch the cabinet playlist each
                # cycle (fresh token) and swap once the new server serves real
                # segments, so the proxy doesn't land on a dead/stale token.
                cfg = self.server.config  # type: ignore[attr-defined]
                url = cfg.get("playlist_url", "")
                def _bg():
                    try:
                        warm_apply_playlist(st, cab, url)
                    except Exception as e:  # noqa: BLE001
                        st.set_playlist_error(f"{type(e).__name__}: {e}")
                        print(f"[api/cabinet/select] warm-apply error: {e}",
                              flush=True)
                threading.Thread(target=_bg, daemon=True).start()
            self._send_json({"ok": ok, "error": err, "state": cab.snapshot()})
            return

        if path == "/api/cabinet/auto":
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception as e:  # noqa: BLE001
                self._send_json({"ok": False, "error": f"bad json: {e}"}, 400)
                return
            with cab.lock:
                cab.auto_apply = bool(payload.get("enabled", True))
            self._send_json({"ok": True, "auto_apply": cab.auto_apply})
            return

        if path == "/api/cabinet/apply-best":
            cfg = self.server.config  # type: ignore[attr-defined]
            threading.Thread(
                target=_apply_best_to_cabinet,
                args=(st, cab, cfg.get("playlist_url", ""), True),
                daemon=True).start()
            self._send_json({"ok": True, "best_host": st.best_host})
            return

        self._send(404, b"not found\n", "text/plain; charset=utf-8")

    # --- helpers --------------------------------------------------------- #
    def _inject_icons(self, html: str) -> str:
        for name, svg in ICONS.items():
            html = html.replace(f"__IC_{name.upper()}__", svg)
        return html

    def _render_dashboard(self) -> str:
        return self._inject_icons(DASHBOARD_HTML)

    def _handle_proxy(self, path: str) -> None:
        """Reverse-proxy /p/<channel_id>/<file>[?...] -> hls.gd with fresh token.

        Resolves the channel's current (host, token) from the proxy map (which
        is rebuilt every time the playlist is refreshed from the cabinet API),
        so a server switch is transparent: the next m3u8/segment request uses
        the new host+token automatically.
        """
        st = self.state
        # sub-path after "/p/"
        sub = path[3:]
        if not sub:
            self._send(404, b"no proxy path\n", "text/plain; charset=utf-8")
            return
        parts = sub.split("/", 1)
        channel_id = parts[0]
        upstream_path = "/" + sub  # e.g. /ch001/mono.m3u8 or /ch001/898784.ts
        lookup = st.proxy_lookup(channel_id)
        if not lookup:
            self._send(502, f"channel {channel_id} not in current playlist\n"
                       .encode("utf-8"), "text/plain; charset=utf-8")
            return
        host, token = lookup
        upstream = f"http://{host}{upstream_path}?token={urllib.parse.quote(token)}"
        # forward Range header for segment requests
        fwd: dict[str, str] = {}
        rng = self.headers.get("Range")
        if rng:
            fwd["Range"] = rng
        code, hdrs, resp = _proxy_fetch(upstream, fwd, timeout=60.0)
        if resp is None or code == 0:
            self._send(502, b"upstream unreachable\n", "text/plain; charset=utf-8")
            return
        try:
            ctype = hdrs.get("content-type", "application/octet-stream")
            is_m3u8 = ("mpegurl" in ctype) or (upstream_path.endswith(".m3u8"))
            if is_m3u8:
                # rewrite internal URLs to /p/... and return
                raw = resp.read()
                # accept 200 only for m3u8 (no range)
                rewritten = _rewrite_proxy_m3u8(raw, channel_id)
                self.send_response(200)
                self.send_header("Content-Type",
                                 "application/vnd.apple.mpegurl; charset=utf-8")
                self.send_header("Content-Length", str(len(rewritten)))
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.end_headers()
                try:
                    self.wfile.write(rewritten)
                except BrokenPipeError:
                    pass
            else:
                # stream segment bytes (possibly partial for Range requests)
                self.send_response(code)
                for h in ("Content-Type", "Content-Length", "Content-Range",
                          "Accept-Ranges"):
                    if h.lower() in hdrs:
                        self.send_header(h, hdrs[h.lower()])
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                try:
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    pass
        finally:
            try:
                resp.close()
            except Exception:  # noqa: BLE001
                pass

    def _trigger_reload(self) -> None:
        url = self.server.config.get("playlist_url", "")  # type: ignore[attr-defined]
        cab = self.server.cabinet  # type: ignore[attr-defined]
        try:
            data = fetch_playlist_smart(url, cab)
            self.state.set_playlist(data)
            print(f"[api/reload] downloaded {len(data)} bytes", flush=True)
        except Exception as e:  # noqa: BLE001
            self.state.set_playlist_error(f"{type(e).__name__}: {e}")
            print(f"[api/reload] error: {e}", flush=True)


class Server(ThreadingHTTPServer):
    state: State
    config: dict[str, Any]
    cabinet: CabinetState


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_servers() -> list[dict[str, str]]:
    with open(SERVERS_PATH, encoding="utf-8") as f:
        return json.load(f)


def _seed_data_dir() -> None:
    """If DATA_DIR is separate from the code dir and the data files are missing
    (typical Docker first run with an empty volume), seed them from the bundled
    defaults so the service can start without manual setup."""
    if os.path.abspath(DATA_DIR) == os.path.abspath(HERE):
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    for name in ("servers.json", "config.json"):
        dst = os.path.join(DATA_DIR, name)
        src = os.path.join(HERE, name)
        if not os.path.exists(dst) and os.path.exists(src):
            try:
                import shutil
                shutil.copy2(src, dst)
            except Exception as e:  # noqa: BLE001
                print(f"[seed] copy {name}: {e}", file=sys.stderr)


def main() -> int:
    _seed_data_dir()
    config = load_config()
    servers = load_servers()
    # Default connection address = this machine's primary LAN IP, so the
    # playlist URLs point where the player can actually reach us. The user can
    # override it (e.g. "tv.cybr.com") via the dashboard.
    if not config.get("connect_addr"):
        config["connect_addr"] = detect_local_ip()
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
        except Exception:  # noqa: BLE001
            pass
    fallback = config.get("fallback_host", "ru3.hls.gd")
    client_ttl = int(config.get("client_measure_ttl_minutes", 60)) * 60
    rewrite_hosts = bool(config.get("rewrite_hosts", False))
    proxy_streams = bool(config.get("proxy_streams", False))
    state = State(servers, fallback, client_ttl, rewrite_hosts)
    state.proxy_mode = proxy_streams

    playlist_url = config.get("playlist_url", "")
    if not playlist_url or playlist_url.startswith("ВСТАВЬ"):
        print("WARNING: playlist_url не задан в config.json — плейлист не будет скачан.",
              file=sys.stderr)

    # Cabinet integration
    cab_user = config.get("cabinet_user", "")
    cab_pass = config.get("cabinet_password", "")
    cabinet = Cabinet(config.get("cabinet_api_base", CABINET_BASE))
    cab_state = CabinetState(cabinet, cab_user, cab_pass)
    # Resolve session from persisted cookies before the first playlist fetch
    # so the initial download can go through the cabinet API (fresh tokens).
    if cab_user and cab_pass:
        try:
            cab_state.refresh_status()
        except Exception as e:  # noqa: BLE001
            print(f"[cabinet] initial status check failed: {e}", flush=True)

    threading.Thread(
        target=worker_playlist,
        args=(state, playlist_url, int(config.get("refresh_playlist_minutes", 360)),
              cab_state),
        daemon=True,
    ).start()
    threading.Thread(
        target=worker_measure,
        args=(state, float(config.get("measure_timeout_seconds", 3)),
              int(config.get("refresh_measure_minutes", 30))),
        daemon=True,
    ).start()
    if cab_user and cab_pass:
        threading.Thread(
            target=worker_cabinet,
            args=(state, cab_state, playlist_url,
                  int(config.get("refresh_cabinet_minutes", 10))),
            daemon=True,
        ).start()

    host = os.environ.get("HOST") or config.get("listen_host", "0.0.0.0")
    port = int(os.environ.get("PORT") or config.get("listen_port", 8080))
    httpd = Server((host, port), Handler)
    httpd.state = state
    httpd.config = config
    httpd.cabinet = cab_state  # type: ignore[attr-defined]

    print(f"tv-plst server picker listening on http://{host}:{port}", flush=True)
    print(f"  dashboard:  http://<ip>:{port}/", flush=True)
    print(f"  measure:    http://<ip>:{port}/measure", flush=True)
    print(f"  playlist:   http://<ip>:{port}/plst.m3u8", flush=True)
    print(f"  servers:    {len(servers)} hosts configured", flush=True)
    print(f"  cabinet:    {'configured' if cab_user else 'not configured'} "
          f"(rewrite_hosts={rewrite_hosts}, proxy_streams={proxy_streams})",
          flush=True)
    if proxy_streams:
        print(f"  proxy:      /p/<channel>/<file> -> hls.gd (tokens via cabinet)",
              flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down…", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
