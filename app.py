import os
import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

# Optional dotenv for local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "state.json")

app = FastAPI()

# -----------------------------
# State storage: Postgres or file
# -----------------------------
_pg_conn = None
_last_storage = "none"
_last_db_error: Optional[str] = None


def _require_ors() -> str:
    ors = (os.getenv("ORS_API_KEY") or "").strip()
    if not ors:
        raise HTTPException(
            status_code=500,
            detail="ORS_API_KEY mangler. Sæt env var ORS_API_KEY før du starter serveren.",
        )
    return ors


def _db_url_with_ssl(url: str) -> str:
    """
    Render Postgres kræver ofte SSL. Hvis sslmode ikke findes, tilføj sslmode=require.
    """
    if not url:
        return url
    try:
        parsed = urlparse(url)
        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if "sslmode" not in q:
            q["sslmode"] = "require"
            parsed = parsed._replace(query=urlencode(q))
            return urlunparse(parsed)
        return url
    except Exception:
        return url


def _get_pg_conn():
    """
    Returns a cached PostgreSQL connection if DATABASE_URL is set.
    Creates the app_state table if needed.
    Uses psycopg (v3) which supports Python 3.13.
    """
    global _pg_conn, _last_storage, _last_db_error

    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        _last_storage = "file"
        _last_db_error = "DATABASE_URL not set"
        return None

    db_url = _db_url_with_ssl(db_url)

    try:
        import psycopg
        from psycopg import sql

        if _pg_conn is None or _pg_conn.closed:
            _pg_conn = psycopg.connect(db_url, connect_timeout=10)
            _pg_conn.autocommit = True

            with _pg_conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_state (
                        id TEXT PRIMARY KEY,
                        data JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )

        _last_storage = "postgres"
        _last_db_error = None
        return _pg_conn

    except Exception as e:
        _last_storage = "file"
        _last_db_error = repr(e)
        return None


def _ensure_dict(x: Any) -> Dict[str, Any]:
    """
    DB can return JSON as dict, but older rows may be stored as JSON string.
    Ensure we always return a dict.
    """
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            v = json.loads(x)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def load_state() -> Dict[str, Any]:
    conn = _get_pg_conn()
    if conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM app_state WHERE id=%s", ("default",))
            row = cur.fetchone()
            return _ensure_dict(row[0]) if row else {}

    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def save_state(state: Dict[str, Any]) -> None:
    global _last_storage, _last_db_error

    if not isinstance(state, dict):
        state = {}

    conn = _get_pg_conn()
    if conn:
        try:
            import psycopg
            from psycopg.types.json import Jsonb

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app_state (id, data)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO UPDATE
                      SET data = EXCLUDED.data,
                          updated_at = NOW();
                    """,
                    ("default", Jsonb(state)),
                )
            _last_storage = "postgres"
            _last_db_error = None
            return
        except Exception as e:
            _last_storage = "file"
            _last_db_error = repr(e)

    # fallback (local only)
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        _last_storage = "file"
    except Exception as e:
        _last_storage = "none"
        _last_db_error = repr(e)


def clear_state() -> None:
    conn = _get_pg_conn()
    if conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_state WHERE id=%s", ("default",))
        return

    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
        except Exception:
            pass


# -----------------------------
# ORS helpers
# -----------------------------
def ors_headers():
    ors = _require_ors()
    # Accept sættes pr. endpoint (isochrones vs matrix)
    return {
        "Authorization": ors,
        "Content-Type": "application/json",
    }


def geocode_one(address: str) -> Dict[str, Any]:
    ors = _require_ors()
    url = "https://api.openrouteservice.org/geocode/search"
    params = {"api_key": ors, "text": address, "size": 1}
    r = requests.get(url, params=params, timeout=20)
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"ORS geocode fejl: {r.status_code} - {r.text}")
    data = r.json()
    feats = data.get("features") or []
    if not feats:
        raise HTTPException(status_code=400, detail=f"Kunne ikke finde adressen: {address}")
    f = feats[0]
    lon, lat = f["geometry"]["coordinates"]
    label = f["properties"].get("label") or address
    return {"lonlat": [lon, lat], "label": label}


def ors_autocomplete(q: str) -> List[Dict[str, Any]]:
    ors = _require_ors()
    url = "https://api.openrouteservice.org/geocode/autocomplete"
    params = {"api_key": ors, "text": q, "size": 10}
    r = requests.get(url, params=params, timeout=20)
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"ORS autocomplete fejl: {r.status_code} - {r.text}")
    data = r.json()
    feats = data.get("features") or []
    return [{"label": f["properties"].get("label"), "lonlat": f["geometry"]["coordinates"]} for f in feats]


def ors_isochrone(lonlat: List[float], minutes: int, profile: str) -> Dict[str, Any]:
    secs = int(minutes) * 60
    url = f"https://api.openrouteservice.org/v2/isochrones/{profile}"
    body = {"locations": [lonlat], "range": [secs], "range_type": "time", "smoothing": 0.6}
    r = requests.post(
        url,
        headers={**ors_headers(), "Accept": "application/geo+json;charset=UTF-8"},
        json=body,
        timeout=40,
    )
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"ORS isochrones fejl: HTTP {r.status_code} - {r.text}")
    return r.json()


def ors_matrix(from_lonlat: List[float], to_lonlats: List[List[float]], profile: str) -> List[Optional[int]]:
    url = f"https://api.openrouteservice.org/v2/matrix/{profile}"
    locations = [from_lonlat] + to_lonlats
    body = {
        "locations": locations,
        "sources": [0],
        "destinations": list(range(1, len(locations))),
        "metrics": ["duration"],
        "resolve_locations": False,
        "units": "m",
    }
    r = requests.post(
        url,
        headers={**ors_headers(), "Accept": "application/json;charset=UTF-8"},
        json=body,
        timeout=40,
    )
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"ORS matrix fejl: HTTP {r.status_code} - {r.text}")
    data = r.json()
    durations = (data.get("durations") or [[]])[0]
    return [None if d is None else int(round(d)) for d in durations]


# -----------------------------
# Routes
# -----------------------------
@app.get("/", response_class=HTMLResponse)
def index():
    path = os.path.join(BASE_DIR, "index.html")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/state")
def api_get_state():
    data = load_state()
    data["_debug"] = {
        "storage": _last_storage,
        "db_error": _last_db_error,
        "has_DATABASE_URL": bool((os.getenv("DATABASE_URL") or "").strip()),
    }
    return data


@app.post("/api/state")
def api_post_state(payload: Dict[str, Any]):
    save_state(payload or {})
    return {
        "ok": True,
        "_debug": {
            "storage": _last_storage,
            "db_error": _last_db_error,
            "has_DATABASE_URL": bool((os.getenv("DATABASE_URL") or "").strip()),
        },
    }


@app.delete("/api/state")
def api_delete_state():
    clear_state()
    return {"ok": True}


@app.get("/api/autocomplete")
def api_autocomplete(q: str = ""):
    q = (q or "").strip()
    if len(q) < 1:
        return []
    return [{"label": x["label"]} for x in ors_autocomplete(q)]


@app.post("/api/overlap")
def api_overlap(payload: Dict[str, Any]):
    address1 = (payload.get("address1") or "").strip()
    address2 = (payload.get("address2") or "").strip()
    minutes = int(payload.get("minutes") or 60)
    profile = (payload.get("profile") or "driving-car").strip()

    if not address1 or not address2:
        raise HTTPException(status_code=400, detail="Begge adresser skal udfyldes.")

    p1 = geocode_one(address1)
    p2 = geocode_one(address2)

    iso1 = ors_isochrone(p1["lonlat"], minutes, profile)
    iso2 = ors_isochrone(p2["lonlat"], minutes, profile)

    features = []
    if iso1.get("features"):
        f = iso1["features"][0]
        f.setdefault("properties", {})
        f["properties"]["color"] = "#2563eb"  # A
        f["properties"]["fillOpacity"] = 0.10
        features.append(f)

    if iso2.get("features"):
        f = iso2["features"][0]
        f.setdefault("properties", {})
        f["properties"]["color"] = "#0a7a2f"  # B
        f["properties"]["fillOpacity"] = 0.10
        features.append(f)

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "a": {"lonlat": p1["lonlat"], "label": p1["label"]},
            "b": {"lonlat": p2["lonlat"], "label": p2["label"]},
            "minutes": minutes,
            "profile": profile,
        },
    }


@app.post("/api/houses")
def api_houses(payload: Dict[str, Any]):
    houses = payload.get("houses") or []
    a_lonlat = payload.get("a_lonlat")
    b_lonlat = payload.get("b_lonlat")
    profile = (payload.get("profile") or "driving-car").strip()

    if not isinstance(houses, list) or not houses:
        return {"houses": [], "profile_name": profile}

    if not (isinstance(a_lonlat, list) and isinstance(b_lonlat, list)):
        raise HTTPException(status_code=400, detail="Mangler a_lonlat/b_lonlat.")

    resolved = []
    to_lonlats = []
    for h in houses:
        hid = h.get("id")
        addr = (h.get("address") or "").strip()
        if not hid or not addr:
            continue
        g = geocode_one(addr)
        resolved.append(
            {"id": hid, "address_input": addr, "address_found": g["label"], "lonlat": g["lonlat"]}
        )
        to_lonlats.append(g["lonlat"])

    if not resolved:
        return {"houses": [], "profile_name": profile}

    durA = ors_matrix(a_lonlat, to_lonlats, profile)
    durB = ors_matrix(b_lonlat, to_lonlats, profile)

    out = []
    for i, h in enumerate(resolved):
        a_sec = durA[i] if i < len(durA) else None
        b_sec = durB[i] if i < len(durB) else None
        out.append(
            {
                **h,
                "fromA_minutes": None if a_sec is None else int(round(a_sec / 60)),
                "fromB_minutes": None if b_sec is None else int(round(b_sec / 60)),
            }
        )

    return {"houses": out, "profile_name": profile}
