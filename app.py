import os
import json
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

# Optional dotenv for local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

ORS_API_KEY = os.getenv("ORS_API_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "state.json")

app = FastAPI()

# -----------------------------
# State storage: Postgres or file
# -----------------------------
_pg_conn = None


def _get_pg_conn():
    """
    Returns a cached PostgreSQL connection if DATABASE_URL is set.
    Creates the app_state table if needed.
    """
    global _pg_conn
    if not DATABASE_URL:
        return None

    if _pg_conn is None or getattr(_pg_conn, "closed", 1) != 0:
        import psycopg2

        _pg_conn = psycopg2.connect(DATABASE_URL)
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
    return _pg_conn


def load_state() -> Dict[str, Any]:
    """
    Load state from Postgres (preferred) or fallback to a local JSON file.
    """
    conn = _get_pg_conn()
    if conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM app_state WHERE id=%s", ("default",))
            row = cur.fetchone()
            return row[0] if row else {}

    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            return {}
    return {}


def save_state(state: Dict[str, Any]) -> None:
    """
    Save state to Postgres (preferred) or fallback to a local JSON file.
    """
    conn = _get_pg_conn()
    if conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_state (id, data)
                VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE
                  SET data = EXCLUDED.data,
                      updated_at = NOW();
                """,
                ("default", json.dumps(state)),
            )
        return

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def clear_state() -> None:
    """
    Clear state in Postgres (preferred) or delete local JSON file.
    """
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
def _require_ors():
    if not ORS_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="ORS_API_KEY mangler. Sæt env var ORS_API_KEY før du starter serveren.",
        )


def ors_headers():
    """
    Base headers shared across ORS endpoints.
    NOTE: 'Accept' differs per endpoint, so it is set per request.
    """
    return {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json",
    }


def geocode_one(address: str) -> Dict[str, Any]:
    _require_ors()
    url = "https://api.openrouteservice.org/geocode/search"
    params = {
        "api_key": ORS_API_KEY,
        "text": address,
        "size": 1,
    }
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
    _require_ors()
    url = "https://api.openrouteservice.org/geocode/autocomplete"
    params = {
        "api_key": ORS_API_KEY,
        "text": q,
        "size": 10,
    }
    r = requests.get(url, params=params, timeout=20)
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"ORS autocomplete fejl: {r.status_code} - {r.text}")
    data = r.json()
    feats = data.get("features") or []
    out = []
    for f in feats:
        out.append(
            {
                "label": f["properties"].get("label"),
                "lonlat": f["geometry"]["coordinates"],
            }
        )
    return out


def ors_isochrone(lonlat: List[float], minutes: int, profile: str) -> Dict[str, Any]:
    _require_ors()
    secs = int(minutes) * 60
    url = f"https://api.openrouteservice.org/v2/isochrones/{profile}"
    body = {
        "locations": [lonlat],
        "range": [secs],
        "range_type": "time",
        "smoothing": 0.6,
    }
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
    _require_ors()
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
    out: List[Optional[int]] = []
    for d in durations:
        out.append(None if d is None else int(round(d)))
    return out


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
    return load_state()


@app.post("/api/state")
def api_post_state(payload: Dict[str, Any]):
    save_state(payload or {})
    return {"ok": True}


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
        f["properties"]["color"] = "#2563eb"  # blue outline for A
        f["properties"]["fillOpacity"] = 0.10
        features.append(f)

    if iso2.get("features"):
        f = iso2["features"][0]
        f.setdefault("properties", {})
        f["properties"]["color"] = "#0a7a2f"  # green outline for B
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
            {
                "id": hid,
                "address_input": addr,
                "address_found": g["label"],
                "lonlat": g["lonlat"],
            }
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
