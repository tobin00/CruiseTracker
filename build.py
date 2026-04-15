#!/usr/bin/env python3
"""
Disney Cruise Tracker — Unified Build Pipeline
===============================================
Run from the project root directory.

Steps:
  ingest   — parse CSV -> assignments.json + cruises_master.json + cruise stubs
  geocode  — scan cruise legs -> geocode missing ports -> ports.json
  route    — compute water-safe routes + build itinerary positions -> SQLite + site/data/
  export   — write site/data/<ship>.json.gz + site/data/manifest.json

Usage:
  python build.py --all --csv data/SCRAPE.csv --coast data/coastlines/ne_110m_land.shp
  python build.py --step ingest  --csv data/SCRAPE.csv
  python build.py --step geocode
  python build.py --step route   --coast data/coastlines/ne_110m_land.shp
  python build.py --step export
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT       = Path(__file__).resolve().parent
DATA       = ROOT / "data"
SITE_DATA  = ROOT / "site" / "data"
DB_PATH    = ROOT / "cruise_tracker.db"
CRUISES_DIR       = DATA / "cruises"
PORTS_PATH        = DATA / "ports.json"
MASTER_PATH       = DATA / "cruises_master.json"
ASSIGNMENTS_PATH  = DATA / "assignments.json"
ROUTES_CACHE_PATH = DATA / "routes_cache.json"

STEP_MINUTES = 10        # position sampling cadence
PSEUDO_PORTS = {"at sea"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def slugify(title: str) -> str:
    t = title.strip().lower()
    t = t.replace("&", " and ").replace("'", "").replace("\u2019", "")
    t = re.sub(r"[^a-z0-9]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-")
    return t[:120]

def parse_date(d: str) -> Optional[str]:
    if not d:
        return None
    d = d.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%b-%Y", "%b %d, %Y"):
        try:
            return datetime.strptime(d, fmt).date().isoformat()
        except Exception:
            continue
    try:
        return datetime.fromisoformat(d).date().isoformat()
    except Exception:
        return None

def norm_lon(lon: float) -> float:
    x = (lon + 180.0) % 360.0
    if x < 0:
        x += 360.0
    return x - 180.0

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def setup_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS assignments (
            id           INTEGER PRIMARY KEY,
            ship         TEXT NOT NULL,
            start_date   TEXT NOT NULL,
            cruise_title TEXT NOT NULL,
            UNIQUE(ship, start_date, cruise_title)
        );

        CREATE TABLE IF NOT EXISTS routes (
            route_key   TEXT PRIMARY KEY,
            points_json TEXT NOT NULL,
            coast_tag   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS itinerary (
            id            INTEGER PRIMARY KEY,
            ship          TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL,
            lat           REAL NOT NULL,
            lon           REAL NOT NULL,
            status        TEXT NOT NULL,
            port_name     TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_itin_ship_time ON itinerary(ship, timestamp_utc);
        CREATE INDEX IF NOT EXISTS idx_itin_time      ON itinerary(timestamp_utc);

        CREATE TABLE IF NOT EXISTS itinerary_archive (
            id            INTEGER PRIMARY KEY,
            ship          TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL,
            lat           REAL NOT NULL,
            lon           REAL NOT NULL,
            status        TEXT NOT NULL,
            port_name     TEXT NOT NULL DEFAULT ''
        );
    """)
    conn.commit()

def migrate_routes_cache(conn: sqlite3.Connection, verbose: bool = False) -> None:
    """One-time: import routes_cache.json into the routes table."""
    if not ROUTES_CACHE_PATH.exists():
        return
    existing = conn.execute("SELECT COUNT(*) FROM routes").fetchone()[0]
    if existing > 0:
        return  # already migrated
    try:
        cache = load_json(ROUTES_CACHE_PATH)
    except Exception:
        return
    tag = cache.get("_meta", {}).get("tag", "unknown")
    rows = []
    for key, val in cache.items():
        if key == "_meta" or not isinstance(val, list):
            continue
        rows.append((key, json.dumps(val), tag))
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO routes(route_key, points_json, coast_tag) VALUES (?,?,?)",
            rows,
        )
        conn.commit()
        if verbose:
            print(f"  Migrated {len(rows)} routes from routes_cache.json -> SQLite")

def archive_old_positions(conn: sqlite3.Connection, verbose: bool = False) -> None:
    """Move itinerary rows older than today into the archive table."""
    cutoff = datetime.now(timezone.utc).date().isoformat()
    rows = conn.execute(
        "SELECT ship, timestamp_utc, lat, lon, status, port_name "
        "FROM itinerary WHERE timestamp_utc < ?",
        (cutoff,),
    ).fetchall()
    if not rows:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO itinerary_archive(ship,timestamp_utc,lat,lon,status,port_name) "
        "VALUES (?,?,?,?,?,?)",
        [(r["ship"], r["timestamp_utc"], r["lat"], r["lon"], r["status"], r["port_name"]) for r in rows],
    )
    conn.execute("DELETE FROM itinerary WHERE timestamp_utc < ?", (cutoff,))
    conn.commit()
    if verbose:
        print(f"  Archived {len(rows)} positions older than {cutoff}")

# ---------------------------------------------------------------------------
# Step 1 — Ingest
# ---------------------------------------------------------------------------
def step_ingest(csv_path: Path, verbose: bool = False) -> None:
    print("\n-- Step 1: Ingest ------------------------------------------")
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    required = {"Ship Name", "Cruise title", "Start date", "Days"}
    rows: List[Dict] = []
    seen: Set[Tuple] = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = set(reader.fieldnames or [])
        missing = required - headers
        if missing:
            print(f"ERROR: CSV missing columns: {sorted(missing)}", file=sys.stderr)
            sys.exit(1)
        for r in reader:
            ship  = (r.get("Ship Name") or "").strip()
            title = (r.get("Cruise title") or "").strip()
            start = parse_date((r.get("Start date") or "").strip())
            days_s = (r.get("Days") or "").strip()
            if not ship or not title or not start:
                continue
            key = (ship, start, title)
            if key in seen:
                continue
            seen.add(key)
            days = None
            try:
                days = int(float(days_s)) if days_s else None
            except Exception:
                pass
            rows.append({"ship": ship, "cruise_title": title, "start_date": start, "days": days})
    rows.sort(key=lambda r: (r["start_date"], r["ship"], r["cruise_title"]))
    print(f"  Parsed {len(rows)} unique sailings from CSV")

    conn = open_db()
    setup_db(conn)
    new_assignments = 0
    for r in rows:
        cur = conn.execute(
            "INSERT OR IGNORE INTO assignments(ship,start_date,cruise_title) VALUES (?,?,?)",
            (r["ship"], r["start_date"], r["cruise_title"]),
        )
        new_assignments += cur.rowcount
    conn.commit()
    conn.close()
    print(f"  {new_assignments} new assignments added to database")

    # Update assignments.json
    save_json(ASSIGNMENTS_PATH, [
        {"ship": r["ship"], "start_date": r["start_date"], "cruise_title": r["cruise_title"]}
        for r in rows
    ])

    # Update cruises_master.json
    master = load_json(MASTER_PATH) if MASTER_PATH.exists() else []
    existing_titles = {m["title"] for m in master}
    new_titles = sorted({r["cruise_title"] for r in rows} - existing_titles)
    if new_titles:
        days_hint = {}
        for r in rows:
            if r["cruise_title"] not in days_hint and r["days"] is not None:
                days_hint[r["cruise_title"]] = r["days"]
        master += [{"title": t, "slug": slugify(t)} for t in new_titles]
        master.sort(key=lambda x: x["title"].lower())
        save_json(MASTER_PATH, master)
        print(f"  Added {len(new_titles)} new cruise types to cruises_master.json")

        # Create skeleton cruise JSONs for new types
        CRUISES_DIR.mkdir(parents=True, exist_ok=True)
        for title in new_titles:
            slug = slugify(title)
            path = CRUISES_DIR / f"{slug}.json"
            if not path.exists():
                days = days_hint.get(title)
                save_json(path, {
                    "title": title, "slug": slug,
                    "notes": f"TODO: Fill legs. days={days or 'unknown'}",
                    "legs": [],
                })
        print(f"  Created {len(new_titles)} new cruise stub files in data/cruises/")
    else:
        print("  No new cruise types found")

    print("  OK Ingest complete")

# ---------------------------------------------------------------------------
# Step 2 — Geocode
# ---------------------------------------------------------------------------
BUILTIN_PORTS: Dict[str, Tuple[float, float, str]] = {
    "Fort Lauderdale, Florida":                    (26.095, -80.126, "America/New_York"),
    "Miami, Florida":                              (25.774, -80.193, "America/New_York"),
    "Port Canaveral, Florida":                     (28.410, -80.630, "America/New_York"),
    "Nassau, Bahamas":                             (25.060, -77.345, "America/Nassau"),
    "Disney Castaway Cay, Bahamas":                (26.080, -77.548, "America/Nassau"),
    "Castaway Cay":                                (26.080, -77.548, "America/Nassau"),
    "Disney Lookout Cay at Lighthouse Point":      (22.193, -75.676, "America/Nassau"),
    "Lookout Cay at Lighthouse Point, Bahamas":    (22.193, -75.676, "America/Nassau"),
    "Cozumel, Mexico":                             (20.509, -86.949, "America/Cancun"),
    "Grand Cayman (George Town), Cayman Islands":  (19.292, -81.374, "America/Cayman"),
    "Falmouth, Jamaica":                           (18.493, -77.655, "America/Jamaica"),
    "San Juan, Puerto Rico":                       (18.466, -66.106, "America/Puerto_Rico"),
    "St. Thomas, U.S. Virgin Islands":             (18.342, -64.932, "America/St_Thomas"),
    "Tortola, British Virgin Islands":             (18.420, -64.640, "America/Tortola"),
    "Galveston, Texas":                            (29.301, -94.797, "America/Chicago"),
    "Civitavecchia (Rome), Italy":                 (42.093, 11.792,  "Europe/Rome"),
    "Civitavecchia (Rome)":                        (42.093, 11.792,  "Europe/Rome"),
    "Livorno (Florence), Italy":                   (43.548, 10.300,  "Europe/Rome"),
    "Naples, Italy":                               (40.842, 14.268,  "Europe/Rome"),
    "Barcelona, Spain":                            (41.355,  2.158,  "Europe/Madrid"),
    "Barcelona":                                   (41.355,  2.158,  "Europe/Madrid"),
    "Piraeus (Athens), Greece":                    (37.942, 23.646,  "Europe/Athens"),
    "Piraeus (Athens)":                            (37.942, 23.646,  "Europe/Athens"),
    "Santorini, Greece":                           (36.393, 25.461,  "Europe/Athens"),
    "Mykonos, Greece":                             (37.446, 25.328,  "Europe/Athens"),
    "Chania, Greece":                              (35.515, 24.018,  "Europe/Athens"),
    "Corfu, Greece":                               (39.619, 19.919,  "Europe/Athens"),
    "Dubrovnik, Croatia":                          (42.659, 18.090,  "Europe/Zagreb"),
    "Zeebrugge (Bruges), Belgium":                 (51.327,  3.204,  "Europe/Brussels"),
    "Cobh (Cork), Ireland":                        (51.850, -8.294,  "Europe/Dublin"),
    "Dublin, Ireland":                             (53.347, -6.258,  "Europe/Dublin"),
    "Southampton, England":                        (50.896, -1.404,  "Europe/London"),
    "Liverpool, England":                          (53.405, -2.998,  "Europe/London"),
    "Bergen, Norway":                              (60.392,  5.324,  "Europe/Oslo"),
    "Reykjavik, Iceland":                          (64.152,-21.942,  "Atlantic/Reykjavik"),
    "Juneau, Alaska":                              (58.301,-134.421, "America/Juneau"),
    "Skagway, Alaska":                             (59.457,-135.315, "America/Juneau"),
    "Ketchikan, Alaska":                           (55.343,-131.648, "America/Sitka"),
    "Sitka, Alaska":                               (57.053,-135.330, "America/Sitka"),
    "Honolulu, Hawaii":                            (21.306,-157.867, "Pacific/Honolulu"),
    "Vancouver, Canada":                           (49.289,-123.115, "America/Vancouver"),
    "Vancouver (British Columbia), Canada":        (49.289,-123.115, "America/Vancouver"),
    "Sydney, Australia":                           (-33.869, 151.205,"Australia/Sydney"),
    "Auckland, New Zealand":                       (-36.844, 174.767,"Pacific/Auckland"),
    "Fiordland National Park, New Zealand":        (-45.415, 167.718,"Pacific/Auckland"),
}

def step_geocode(verbose: bool = False) -> None:
    print("\n-- Step 2: Geocode -----------------------------------------")
    try:
        import requests
        from timezonefinder import TimezoneFinder
        HAS_GEOCODE_DEPS = True
    except ImportError:
        HAS_GEOCODE_DEPS = False
        print("  WARNING: 'requests' or 'timezonefinder' not installed; "
              "only built-in port coordinates will be used.")

    # Collect all port names referenced in cruise legs
    used: Set[str] = set()
    for fp in sorted(CRUISES_DIR.glob("*.json")):
        try:
            data = load_json(fp)
        except Exception:
            continue
        for leg in data.get("legs") or []:
            for k in ("from", "to"):
                n = (leg.get(k) or "").strip()
                if n and n.lower() not in PSEUDO_PORTS:
                    used.add(n)

    ports = load_json(PORTS_PATH) if PORTS_PATH.exists() else []
    existing = {(p.get("name") or "").lower() for p in ports}
    missing = [n for n in sorted(used) if n.lower() not in existing]

    print(f"  {len(used)} port names in use, {len(missing)} missing from ports.json")
    if not missing:
        print("  OK All ports already geocoded")
        return

    builtin_lower = {k.lower(): v for k, v in BUILTIN_PORTS.items()}
    cache_path = DATA / ".geocache.json"
    cache: Dict = {}
    if cache_path.exists():
        try:
            cache = load_json(cache_path)
        except Exception:
            pass

    additions = []
    for name in missing:
        key = name.lower()
        lat = lon = tz = None

        if key in builtin_lower:
            lat, lon, tz = builtin_lower[key]
            if verbose:
                print(f"  builtin: '{name}' -> {lat:.4f}, {lon:.4f}")
        elif HAS_GEOCODE_DEPS:
            from timezonefinder import TimezoneFinder
            import requests
            if key in cache and cache[key]:
                lat, lon = cache[key]["lat"], cache[key]["lon"]
                tf = TimezoneFinder()
                tz = tf.timezone_at(lat=lat, lng=lon) or ""
            else:
                cleaned = re.sub(r"\s*\([^)]*\)", "", name).strip()
                for q in ([cleaned, name] if cleaned != name else [name]):
                    try:
                        r = requests.get(
                            "https://nominatim.openstreetmap.org/search",
                            params={"q": q, "format": "json", "limit": 1},
                            headers={"User-Agent": "CruiseTracker/2.0"},
                            timeout=25,
                        )
                        time.sleep(1.0)
                        arr = r.json()
                        if arr:
                            lat, lon = float(arr[0]["lat"]), float(arr[0]["lon"])
                            cache[key] = {"lat": lat, "lon": lon}
                            cache_path.write_text(json.dumps(cache, indent=2), encoding="utf-8")
                            tf = TimezoneFinder()
                            tz = tf.timezone_at(lat=lat, lng=lon) or ""
                            if verbose:
                                print(f"  geocoded: '{name}' -> {lat:.4f}, {lon:.4f}")
                            break
                    except Exception as e:
                        if verbose:
                            print(f"  geocode failed '{name}': {e}")
        else:
            print(f"  !! cannot resolve '{name}' — add manually to ports.json")

        additions.append({
            "name": name, "lat": lat, "lon": lon, "tz": tz or "",
            "notes": "" if lat is not None else "TODO: verify / fill details",
        })

    ports.extend(additions)
    ports.sort(key=lambda p: (p.get("name") or "").lower())
    save_json(PORTS_PATH, ports)
    print(f"  Added {len(additions)} ports to ports.json")
    print("  OK Geocode complete")

# ---------------------------------------------------------------------------
# Step 3 — Route (water-safe A* routing + itinerary generation)
# ---------------------------------------------------------------------------

# --- Geo math helpers ---
try:
    from pyproj import Geod
    _WGS84 = Geod(ellps="WGS84")
    def gc_distance_km(a: Tuple[float,float], b: Tuple[float,float]) -> float:
        _, _, m = _WGS84.inv(a[1], a[0], b[1], b[0])
        return m / 1000.0
except ImportError:
    def gc_distance_km(a: Tuple[float,float], b: Tuple[float,float]) -> float:
        R = 6371.0
        lat1, lon1 = math.radians(a[0]), math.radians(a[1])
        lat2, lon2 = math.radians(b[0]), math.radians(b[1])
        dlat, dlon = lat2-lat1, lon2-lon1
        h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
        return 2*R*math.asin(math.sqrt(h))

def gc_interp(a: Tuple[float,float], b: Tuple[float,float], t: float) -> Tuple[float,float]:
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    d = 2*math.asin(math.sqrt(
        math.sin((lat2-lat1)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
    ))
    if d == 0:
        return a
    A = math.sin((1-t)*d)/math.sin(d)
    B = math.sin(t*d)/math.sin(d)
    x = A*math.cos(lat1)*math.cos(lon1) + B*math.cos(lat2)*math.cos(lon2)
    y = A*math.cos(lat1)*math.sin(lon1) + B*math.cos(lat2)*math.sin(lon2)
    z = A*math.sin(lat1)               + B*math.sin(lat2)
    return (math.degrees(math.atan2(z, math.hypot(x, y))), norm_lon(math.degrees(math.atan2(y, x))))

def parse_local_dt(start_date: str, day: int, time_str: str, tz_name: str) -> datetime:
    base = datetime.fromisoformat(start_date)
    if base.tzinfo is None:
        base = base.replace(tzinfo=ZoneInfo(tz_name))
    else:
        base = base.astimezone(ZoneInfo(tz_name))
    hh, mm = map(int, time_str.split(":"))
    return (base + timedelta(days=day, hours=hh, minutes=mm)).astimezone(timezone.utc)

def cross_track_km(a, b, p) -> float:
    R = 6371.0
    def to_r(pt): return math.radians(pt[0]), math.radians(pt[1])
    lat1,lon1 = to_r(a); lat2,lon2 = to_r(b); latp,lonp = to_r(p)
    d13 = 2*math.asin(math.sqrt(math.sin((latp-lat1)/2)**2 + math.cos(lat1)*math.cos(latp)*math.sin((lonp-lon1)/2)**2))
    def bearing(la,lo,la2,lo2):
        y = math.sin(lo2-lo)*math.cos(la2)
        x = math.cos(la)*math.sin(la2) - math.sin(la)*math.cos(la2)*math.cos(lo2-lo)
        return math.atan2(y,x)
    xt = math.asin(math.sin(d13)*math.sin(bearing(lat1,lon1,latp,lonp)-bearing(lat1,lon1,lat2,lon2)))
    return abs(xt)*R

# --- Coastline & graph (requires geopandas, shapely, networkx, numpy) ---
def _load_routing_deps():
    try:
        import geopandas as gpd
        import networkx as nx
        import numpy as np
        from shapely.geometry import Point
        from shapely.ops import unary_union
        from shapely.strtree import STRtree
        return gpd, nx, np, Point, unary_union, STRtree
    except ImportError as e:
        print(f"\nERROR: Routing dependencies not installed: {e}", file=sys.stderr)
        print("Run: pip install shapely pyproj rtree networkx geopandas numpy", file=sys.stderr)
        sys.exit(1)

def load_coastlines(coast_path: Path, bbox=None, verbose=False):
    gpd, nx, np, Point, unary_union, STRtree = _load_routing_deps()
    try:
        from shapely.validation import make_valid as _mv
    except ImportError:
        _mv = None
    try:
        from shapely import set_precision as _sp
    except ImportError:
        _sp = None

    if verbose:
        print(f"  Loading coastline: {coast_path.name} (bbox={bbox})")
    gdf = gpd.read_file(coast_path, bbox=bbox)
    if gdf.empty:
        raise RuntimeError("Coastline empty after bbox clip.")

    repaired = []
    for geom in gdf.geometry:
        if geom is None or geom.is_empty:
            continue
        if not geom.is_valid:
            geom = (_mv(geom) if _mv else geom.buffer(0)) or geom
        if _sp:
            geom = _sp(geom, 1e-6)
        if geom and not geom.is_empty:
            repaired.append(geom)

    merged = unary_union(repaired)
    if any(t in str(coast_path) for t in ("GSHHS_f_","GSHHS_h_")):
        merged = merged.simplify(0.0005, preserve_topology=True)

    if merged.is_empty:
        polys = []
    elif merged.geom_type == "Polygon":
        polys = [merged]
    elif merged.geom_type == "MultiPolygon":
        polys = list(merged.geoms)
    else:
        polys = list(getattr(merged, "geoms", [merged]))

    if verbose:
        print(f"  Coastline: {len(polys)} polygons — building spatial index")
    return polys, STRtree(polys)

def hex_grid_for_bbox(lat_min, lat_max, lon_min, lon_max, hex_km):
    dlat = hex_km / 111.0
    nodes = []
    lat = lat_min; row = 0
    while lat <= lat_max:
        dlon = hex_km / (111.0 * max(0.2, math.cos(math.radians(lat))))
        lon = lon_min + (0.5*dlon if row%2 else 0.0)
        while lon <= lon_max:
            nodes.append((lat, lon)); lon += dlon
        lat += 0.866*dlat; row += 1
    return nodes

def build_graph(nodes, land_polys, land_index, hex_km, verbose=False, buffer_km=0.0):
    gpd, nx, np, Point, unary_union, STRtree = _load_routing_deps()
    G = nx.Graph()

    def is_water(pt):
        lat, lon = pt
        p = Point(lon, lat)
        deg_lat = max(buffer_km, 1.0) / 111.0
        deg_lon = deg_lat / max(0.2, math.cos(math.radians(lat)))
        idxs = np.asarray(land_index.query(p.buffer(max(deg_lat, deg_lon)))).ravel().tolist()
        for ix in idxs:
            c = land_polys[int(ix)]
            if c.contains(p):
                return False
            if buffer_km > 0:
                d_deg = c.exterior.distance(p) if c.geom_type == "Polygon" else c.distance(p)
                d_km = 111.0 * d_deg * max(0.2, math.cos(math.radians(lat)))
                if d_km < buffer_km:
                    return False
        return True

    kept = [pt for pt in nodes if is_water(pt)]
    for i, pt in enumerate(kept):
        G.add_node(i, lat=pt[0], lon=pt[1])
    if not kept:
        return G

    lats = [p[0] for p in kept]; lons = [p[1] for p in kept]
    lat_min_k, lon_min_k = min(lats), min(lons)
    midlat = sum(lats)/len(lats)
    dlat = hex_km/111.0
    dlon = hex_km/(111.0*max(0.2, math.cos(math.radians(midlat))))
    rad_km = 1.8*hex_km
    buckets: Dict[Tuple, List] = {}

    def bkey(pt):
        return (int((pt[0]-lat_min_k)//(0.9*dlat)), int((pt[1]-lon_min_k)//(0.9*dlon)))

    for idx, pt in enumerate(kept):
        buckets.setdefault(bkey(pt), []).append(idx)

    for idx, pt in enumerate(kept):
        ky = bkey(pt)
        for dy in (-1,0,1):
            for dx in (-1,0,1):
                for j in buckets.get((ky[0]+dy, ky[1]+dx), []):
                    if j <= idx:
                        continue
                    d = gc_distance_km(pt, kept[j])
                    if d < rad_km:
                        mid = Point((pt[1]+kept[j][1])/2, (pt[0]+kept[j][0])/2)
                        hit = any(land_polys[int(ix)].contains(mid)
                                  for ix in np.asarray(land_index.query(mid)).ravel().tolist())
                        if not hit:
                            G.add_edge(idx, j, w=d)
    if verbose:
        print(f"    graph: {G.number_of_nodes()} water nodes, {G.number_of_edges()} edges")
    return G

def nearest_node(G, lat, lon):
    import networkx as nx
    if G.number_of_nodes() == 0:
        raise RuntimeError("No water nodes in graph.")
    return min(G.nodes, key=lambda n: gc_distance_km((lat, lon), (G.nodes[n]["lat"], G.nodes[n]["lon"])))

def route_leg(a, b, land_polys, land_index, hex_km, pad_deg, verbose=False, buffer_km=0.0):
    import networkx as nx

    def attempt(hexk, pad, buff, corridor):
        lat0, lon0 = a; lat1, lon1 = b
        lat_min = min(lat0, lat1)-pad; lat_max = max(lat0, lat1)+pad
        lon_min = min(lon0, lon1)-pad; lon_max = max(lon0, lon1)+pad
        nodes = [pt for pt in hex_grid_for_bbox(lat_min, lat_max, lon_min, lon_max, hexk)
                 if cross_track_km(a, b, pt) <= corridor]
        if verbose:
            print(f"    try hex={hexk}km buff={buff}km pad=±{pad}° corridor={int(corridor)}km nodes={len(nodes)}")
        G = build_graph(nodes, land_polys, land_index, hexk, verbose=False, buffer_km=buff)
        try:
            s = nearest_node(G, lat0, lon0)
            t = nearest_node(G, lat1, lon1)
        except RuntimeError:
            return None
        try:
            path = nx.astar_path(G, s, t,
                heuristic=lambda u,v: gc_distance_km((G.nodes[u]["lat"],G.nodes[u]["lon"]),(lat1,lon1)),
                weight="w")
            if verbose:
                print(f"    ok — {len(path)} hops")
            return [(G.nodes[n]["lat"], G.nodes[n]["lon"]) for n in path]
        except nx.NetworkXNoPath:
            return None

    base_corridor = max(200.0, 12.0*hex_km)
    for (hx, pd, bf, cor) in [
        (hex_km,        pad_deg,    buffer_km,            base_corridor),
        (hex_km,        pad_deg,    buffer_km,            350.0),
        (hex_km,        pad_deg+4,  buffer_km,            350.0),
        (max(12,hex_km-2), pad_deg+4, max(6,buffer_km-2),350.0),
        (10.0,          pad_deg+6,  max(5,buffer_km-3),  500.0),
        (10.0,          pad_deg+8,  max(4,buffer_km-4),  600.0),
    ]:
        poly = attempt(hx, pd, bf, cor)
        if poly:
            return poly

    # last resort: split at midpoint
    mid = gc_interp(a, b, 0.5)
    if verbose:
        print("    splitting at midpoint…")
    left  = route_leg(a,   mid, land_polys, land_index, max(12,hex_km-2), pad_deg+6, verbose, max(5,buffer_km-3))
    right = route_leg(mid, b,   land_polys, land_index, max(12,hex_km-2), pad_deg+6, verbose, max(5,buffer_km-3))
    return left[:-1] + right

def resample_polyline(poly, depart_utc, arrive_utc):
    total = (arrive_utc - depart_utc).total_seconds()
    steps = max(1, int(total // (STEP_MINUTES * 60)))
    segs = [gc_distance_km(poly[i], poly[i+1]) for i in range(len(poly)-1)]
    L = sum(segs)
    cum = [0.0]
    for d in segs:
        cum.append(cum[-1]+d)
    out = []
    for i in range(1, steps):
        t = i/steps
        s = t*L
        k = 0
        while k < len(segs) and cum[k+1] < s:
            k += 1
        k = min(k, len(segs)-1)
        u = 0.0 if segs[k] == 0 else (s-cum[k])/segs[k]
        lat, lon = gc_interp(poly[k], poly[k+1], u)
        out.append((depart_utc + timedelta(seconds=t*total), lat, lon))
    return out

def step_route(coast_path: Path, hex_km=12.0, buffer_km=4.0, pad_deg=12.0,
               rebuild_all=False, verbose=False) -> None:
    print("\n-- Step 3: Route -------------------------------------------")

    ports_list  = load_json(PORTS_PATH)
    master      = load_json(MASTER_PATH)
    assignments = load_json(ASSIGNMENTS_PATH)
    port_by     = {p["name"]: p for p in ports_list}
    slug_by     = {m["title"]: m["slug"] for m in master}

    def coords(name):
        p = port_by.get(name)
        if not p:
            raise KeyError(name)
        return p["lat"], p["lon"], p["tz"]

    conn = open_db()
    setup_db(conn)
    migrate_routes_cache(conn, verbose)

    coast_tag = f"coast={coast_path.name};hex_km={hex_km};buffer_km={buffer_km};pad_deg={pad_deg}"

    # Collect unique legs we need (only from assignments with filled-in legs)
    needed_legs: List[Tuple[str,str]] = []
    skipped_cruises: Set[str] = set()
    for a in assignments:
        title = a["cruise_title"]
        slug  = slug_by.get(title)
        if not slug:
            continue
        tpl_path = CRUISES_DIR / f"{slug}.json"
        if not tpl_path.exists():
            skipped_cruises.add(title)
            continue
        tpl = load_json(tpl_path)
        if not tpl.get("legs"):
            skipped_cruises.add(title)
            continue
        for leg in tpl["legs"]:
            fr, to = leg.get("from",""), leg.get("to","")
            if fr not in port_by:
                print(f"  WARNING: unknown port '{fr}' in {slug} — skipping leg")
                skipped_cruises.add(title)
            elif to not in port_by:
                print(f"  WARNING: unknown port '{to}' in {slug} — skipping leg")
                skipped_cruises.add(title)
            else:
                needed_legs.append((fr, to))

    unique_legs = sorted(set(needed_legs))
    if skipped_cruises:
        print(f"  Skipping {len(skipped_cruises)} cruise types with empty/incomplete legs")

    # Which legs still need routing?
    legs_to_route = []
    for fr, to in unique_legs:
        key = f"{fr}|{to}"
        if not rebuild_all:
            row = conn.execute(
                "SELECT 1 FROM routes WHERE route_key=? AND coast_tag=?", (key, coast_tag)
            ).fetchone()
            if row:
                continue
        legs_to_route.append((fr, to))

    print(f"  {len(unique_legs)} unique legs total; {len(legs_to_route)} need routing")

    if legs_to_route:
        # Compute bounding box for coastline clip
        all_pts = [(port_by[fr]["lat"], port_by[fr]["lon"],
                    port_by[to]["lat"], port_by[to]["lon"])
                   for fr, to in legs_to_route]
        lat_min = min(r[0] for r in all_pts) - pad_deg
        lat_max = max(r[2] for r in all_pts) + pad_deg
        lon_min = min(r[1] for r in all_pts) - pad_deg
        lon_max = max(r[3] for r in all_pts) + pad_deg
        bbox = (lon_min, lat_min, lon_max, lat_max)

        land_polys, land_index = load_coastlines(coast_path, bbox=bbox, verbose=verbose)

        for i, (fr, to) in enumerate(legs_to_route, 1):
            f_lat, f_lon, _ = coords(fr)
            t_lat, t_lon, _ = coords(to)
            print(f"  [{i}/{len(legs_to_route)}] {fr} -> {to}")
            poly = route_leg((f_lat,f_lon),(t_lat,t_lon), land_polys, land_index,
                             hex_km, pad_deg, verbose, buffer_km)
            pts_json = json.dumps([[lat, lon] for lat, lon in poly])
            rev_json = json.dumps([[lon, lat] for lat, lon in reversed(poly)])  # note: correct order
            rev_json = json.dumps([[lat, lon] for lat, lon in reversed(poly)])
            key_fwd = f"{fr}|{to}"
            key_rev = f"{to}|{fr}"
            conn.execute("INSERT OR REPLACE INTO routes(route_key,points_json,coast_tag) VALUES(?,?,?)",
                         (key_fwd, pts_json, coast_tag))
            conn.execute("INSERT OR REPLACE INTO routes(route_key,points_json,coast_tag) VALUES(?,?,?)",
                         (key_rev, rev_json, coast_tag))
            conn.commit()

        # Persist back to routes_cache.json for compatibility
        _persist_routes_cache(conn, coast_tag, verbose)
    else:
        print("  All routes cached — skipping A* computation")
        land_polys = land_index = None

    # Build itinerary positions
    print("  Building itinerary positions…")
    archive_old_positions(conn, verbose)

    inserted = 0
    for a in assignments:
        title = a["cruise_title"]
        slug  = slug_by.get(title)
        if not slug:
            continue
        tpl_path = CRUISES_DIR / f"{slug}.json"
        if not tpl_path.exists():
            continue
        tpl = load_json(tpl_path)
        if not tpl.get("legs"):
            continue

        rows_to_insert = []
        ok = True
        for leg in tpl["legs"]:
            fr, to = leg.get("from",""), leg.get("to","")
            if fr not in port_by or to not in port_by:
                ok = False
                break
            f_lat, f_lon, f_tz = coords(fr)
            t_lat, t_lon, t_tz = coords(to)
            try:
                depart_utc = parse_local_dt(a["start_date"], leg["depart_day"], leg["depart_time_local"], f_tz)
                arrive_utc = parse_local_dt(a["start_date"], leg["arrive_day"], leg["arrive_time_local"], t_tz)
            except Exception as e:
                print(f"  WARNING: time parse error in {slug}: {e}")
                ok = False
                break
            if arrive_utc <= depart_utc:
                print(f"  WARNING: arrive ≤ depart in {slug} leg {fr}->{to}")
                ok = False
                break

            key = f"{fr}|{to}"
            row = conn.execute("SELECT points_json FROM routes WHERE route_key=?", (key,)).fetchone()
            if not row:
                print(f"  WARNING: no cached route for {key} — skipping assignment")
                ok = False
                break
            poly = [tuple(p) for p in json.loads(row[0])]

            rows_to_insert.append({"ship": a["ship"], "ts": depart_utc.isoformat().replace("+00:00","Z"),
                                   "lat": f_lat, "lon": f_lon, "status": "depart", "port": fr})
            for ts, lat, lon in resample_polyline(poly, depart_utc, arrive_utc):
                rows_to_insert.append({"ship": a["ship"], "ts": ts.isoformat().replace("+00:00","Z"),
                                       "lat": lat, "lon": lon, "status": "sea", "port": ""})
            rows_to_insert.append({"ship": a["ship"], "ts": arrive_utc.isoformat().replace("+00:00","Z"),
                                   "lat": t_lat, "lon": t_lon, "status": "arrive", "port": to})

        if ok and rows_to_insert:
            conn.executemany(
                "INSERT OR IGNORE INTO itinerary(ship,timestamp_utc,lat,lon,status,port_name) "
                "VALUES(:ship,:ts,:lat,:lon,:status,:port)",
                rows_to_insert,
            )
            inserted += conn.execute("SELECT changes()").fetchone()[0]

    conn.commit()
    conn.close()
    print(f"  Inserted {inserted} new position records")
    print("  OK Route complete")

def _persist_routes_cache(conn: sqlite3.Connection, coast_tag: str, verbose: bool) -> None:
    rows = conn.execute("SELECT route_key, points_json FROM routes WHERE coast_tag=?", (coast_tag,)).fetchall()
    cache = {"_meta": {"tag": coast_tag}}
    for r in rows:
        cache[r["route_key"]] = json.loads(r["points_json"])
    ROUTES_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    if verbose:
        print(f"  routes_cache.json updated ({len(rows)} routes)")

# ---------------------------------------------------------------------------
# One-time migration: itineraries.json -> SQLite
# ---------------------------------------------------------------------------
def _migrate_itineraries_json(conn: sqlite3.Connection, verbose: bool = False) -> None:
    """Import existing itineraries.json into SQLite on first run (one-time)."""
    existing = conn.execute("SELECT COUNT(*) FROM itinerary").fetchone()[0]
    if existing > 0:
        return  # already populated
    legacy = DATA / "itineraries.json"
    if not legacy.exists():
        return
    print("  Migrating itineraries.json into SQLite (one-time)...")
    try:
        records = json.loads(legacy.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  WARNING: could not read itineraries.json: {e}")
        return
    batch = [
        (r["ship"], r["timestamp_utc"], r["lat"], r["lon"], r["status"], r.get("port_name",""))
        for r in records
        if isinstance(r, dict) and "ship" in r and "timestamp_utc" in r
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO itinerary(ship,timestamp_utc,lat,lon,status,port_name) VALUES(?,?,?,?,?,?)",
        batch,
    )
    conn.commit()
    print(f"  Migrated {len(batch)} records from itineraries.json")

# ---------------------------------------------------------------------------
# Step 4 — Export
# ---------------------------------------------------------------------------
def step_export(verbose: bool = False) -> None:
    print("\n-- Step 4: Export ------------------------------------------")
    SITE_DATA.mkdir(parents=True, exist_ok=True)

    conn = open_db()
    setup_db(conn)
    migrate_routes_cache(conn, verbose)
    _migrate_itineraries_json(conn, verbose)
    archive_old_positions(conn, verbose)

    # Get all ships present in itinerary
    ships_in_db = [r[0] for r in conn.execute(
        "SELECT DISTINCT ship FROM itinerary ORDER BY ship"
    ).fetchall()]

    if not ships_in_db:
        print("  WARNING: itinerary table is empty. Run --step route first.")
        conn.close()
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    date_min = date_max = None
    total_written = 0
    skipped_slugs = _get_skipped_cruises()

    for ship in ships_in_db:
        rows = conn.execute(
            "SELECT timestamp_utc, lat, lon, status, port_name "
            "FROM itinerary WHERE ship=? ORDER BY timestamp_utc",
            (ship,),
        ).fetchall()
        if not rows:
            continue

        records = [
            {"t": r["timestamp_utc"], "lat": round(r["lat"],6),
             "lon": round(r["lon"],6), "s": r["status"], "p": r["port_name"]}
            for r in rows
        ]

        t0 = records[0]["t"]; t1 = records[-1]["t"]
        if date_min is None or t0 < date_min: date_min = t0[:10]
        if date_max is None or t1 > date_max: date_max = t1[:10]

        slug = ship.lower()
        out_path = SITE_DATA / f"{slug}.json.gz"
        data_bytes = json.dumps(records, ensure_ascii=False, separators=(",",":")).encode("utf-8")
        with gzip.open(out_path, "wb", compresslevel=9) as f:
            f.write(data_bytes)

        size_kb = out_path.stat().st_size // 1024
        if verbose:
            print(f"  {ship}: {len(records)} records -> {slug}.json.gz ({size_kb} KB)")
        total_written += len(records)

    conn.close()

    # Manifest
    manifest = {
        "built_at": now_iso,
        "ships": ships_in_db,
        "date_range": {"from": date_min, "to": date_max},
        "skipped_cruises": sorted(skipped_slugs),
    }
    (SITE_DATA / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(f"  Exported {total_written} records for {len(ships_in_db)} ships to site/data/")
    print(f"  Date range: {date_min} -> {date_max}")
    print("  OK Export complete")

def _get_skipped_cruises() -> Set[str]:
    """Return cruise titles that have empty legs."""
    master = load_json(MASTER_PATH) if MASTER_PATH.exists() else []
    skipped = set()
    for m in master:
        tpl_path = CRUISES_DIR / f"{m['slug']}.json"
        if not tpl_path.exists():
            skipped.add(m["title"])
            continue
        tpl = load_json(tpl_path)
        if not tpl.get("legs"):
            skipped.add(m["title"])
    return skipped

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Disney Cruise Tracker — Unified Build Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python build.py --all --csv data/2025-08-26_1907_ALL_SHIPS.csv --coast data/coastlines/ne_110m_land.shp
  python build.py --step ingest  --csv data/NEW_SCRAPE.csv
  python build.py --step geocode
  python build.py --step route   --coast data/coastlines/ne_110m_land.shp
  python build.py --step export
        """,
    )
    ap.add_argument("--all", action="store_true", help="Run all steps in sequence")
    ap.add_argument("--step", choices=["ingest","geocode","route","export"],
                    help="Run a single step")
    ap.add_argument("--csv", help="Path to scraper CSV (required for ingest)")
    ap.add_argument("--coast", help="Path to coastline shapefile (required for route)")
    ap.add_argument("--hex-km",    type=float, default=12.0, help="Hex grid spacing in km (default 12)")
    ap.add_argument("--buffer-km", type=float, default=4.0,  help="Offshore clearance in km (default 4)")
    ap.add_argument("--pad-deg",   type=float, default=12.0, help="BBox padding in degrees (default 12)")
    ap.add_argument("--rebuild-all", action="store_true",    help="Recompute all routes, ignore cache")
    ap.add_argument("--verbose", "-v", action="store_true",  help="Verbose output")
    args = ap.parse_args()

    if not args.all and not args.step:
        ap.print_help()
        sys.exit(0)

    steps = ["ingest","geocode","route","export"] if args.all else [args.step]

    for step in steps:
        if step == "ingest":
            if not args.csv:
                print("ERROR: --csv required for ingest step", file=sys.stderr); sys.exit(1)
            step_ingest(Path(args.csv), verbose=args.verbose)
        elif step == "geocode":
            step_geocode(verbose=args.verbose)
        elif step == "route":
            if not args.coast:
                print("ERROR: --coast required for route step", file=sys.stderr); sys.exit(1)
            step_route(Path(args.coast), args.hex_km, args.buffer_km, args.pad_deg,
                       args.rebuild_all, args.verbose)
        elif step == "export":
            step_export(verbose=args.verbose)

    print("\nOK Build complete.")

if __name__ == "__main__":
    main()
