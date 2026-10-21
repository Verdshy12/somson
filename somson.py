#!/usr/bin/env python3
"""
Universal OpenStreetMap (.osm / .osc / .osh / .gz / .bz2 / .xz / .zip / .zst / .pbf)
to OSM JSON, GeoJSON, or NDJSON converter.
Uses streaming I/O and SQLite disk-backed indexing for bounded memory usage.
"""

import argparse
import bz2
import contextlib
import gzip
import io
import json
import lzma
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

# OSM keys that indicate a closed way is an area/polygon rather than a closed line
POLYGON_KEYS = {
    "building", "landuse", "amenity", "leisure", "natural", "shop",
    "tourism", "historic", "man_made", "military", "aeroway", "boundary",
    "craft", "office", "place", "power", "public_transport", "waterway"
}

NON_POLYGON_VALUES = {
    "natural": {"coastline", "ridge", "arete", "tree_row", "cliff"},
    "waterway": {"river", "stream", "canal", "drain", "ditch", "weir", "dam"},
    "aeroway": {"taxiway"},
}


def strip_ns(tag: str) -> str:
    """Strip XML namespace if present (e.g., '{http://...}node' -> 'node')."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


@contextlib.contextmanager
def open_osm_stream(path: Path):
    """
    Open any OSM file format (.osm, .osc, .osh, .gz, .bz2, .xz, .zip, .zst, .pbf, or '-' for stdin)
    and yield a readable binary or UTF-8 text stream of XML.
    """
    if str(path) == "-":
        yield io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8-sig", errors="replace")
        return

    name_lower = path.name.lower()

    # 1. Handle PBF binary files via osmium CLI if available
    if name_lower.endswith(".pbf"):
        osmium_bin = shutil.which("osmium")
        if osmium_bin:
            proc = subprocess.Popen(
                [osmium_bin, "cat", str(path), "-f", "osm"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                yield io.TextIOWrapper(proc.stdout, encoding="utf-8-sig", errors="replace")
            finally:
                proc.stdout.close()
                proc.wait()
            return
        raise RuntimeError(
            f"'{path.name}' is a binary .pbf file. Install 'pyosmium' (`pip install osmium`) "
            "or 'osmium-tool' (`sudo apt install osmium-tool`) to convert .pbf files."
        )

    # 2. Handle compressed XML archives
    if name_lower.endswith(".gz"):
        with gzip.open(path, mode="rt", encoding="utf-8-sig", errors="replace") as f:
            yield f
    elif name_lower.endswith(".bz2"):
        with bz2.open(path, mode="rt", encoding="utf-8-sig", errors="replace") as f:
            yield f
    elif name_lower.endswith((".xz", ".lzma")):
        with lzma.open(path, mode="rt", encoding="utf-8-sig", errors="replace") as f:
            yield f
    elif name_lower.endswith(".zip"):
        with zipfile.ZipFile(path, "r") as zf:
            osm_members = [
                m for m in zf.namelist()
                if m.lower().endswith((".osm", ".xml", ".osc", ".osh"))
            ]
            target = osm_members[0] if osm_members else zf.namelist()[0]
            with zf.open(target, "r") as raw:
                yield io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace")
    elif name_lower.endswith(".zst"):
        try:
            import zstandard as zstd
            with open(path, "rb") as fh:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(fh) as reader:
                    yield io.TextIOWrapper(reader, encoding="utf-8-sig", errors="replace")
        except ImportError:
            zstd_bin = shutil.which("zstd")
            if not zstd_bin:
                raise RuntimeError(
                    "To read .zst files, install zstandard (`pip install zstandard`) or `sudo apt install zstd`."
                )
            proc = subprocess.Popen([zstd_bin, "-dc", str(path)], stdout=subprocess.PIPE)
            try:
                yield io.TextIOWrapper(proc.stdout, encoding="utf-8-sig", errors="replace")
            finally:
                proc.stdout.close()
                proc.wait()
    else:
        with open(path, mode="rt", encoding="utf-8-sig", errors="replace") as f:
            yield f


class StreamingJSONWriter:
    """Writes JSON arrays or FeatureCollections incrementally to avoid holding lists in RAM."""

    def __init__(self, fp, mode: str = "osm", indent: Optional[int] = None, bounds: Optional[dict] = None):
        self.fp = fp
        self.mode = mode  # 'osm', 'geojson', or 'ndjson'
        self.indent = indent
        self.first = True
        self.bounds = bounds

    def __enter__(self):
        if self.mode == "osm":
            header = '{\n  "version": 0.6,\n  "generator": "osm2json-universal"'
            if self.bounds:
                header += f',\n  "bounds": {json.dumps(self.bounds)}'
            header += ',\n  "elements": [\n' if self.indent else ',"elements":['
            if not self.indent:
                header = header.replace("\n", "").replace("  ", "")
            self.fp.write(header)
        elif self.mode == "geojson":
            if self.indent:
                self.fp.write('{\n  "type": "FeatureCollection",\n')
                if self.bounds:
                    bbox = [
                        self.bounds["minlon"], self.bounds["minlat"],
                        self.bounds["maxlon"], self.bounds["maxlat"]
                    ]
                    self.fp.write(f'  "bbox": {json.dumps(bbox)},\n')
                self.fp.write('  "features": [\n')
            else:
                self.fp.write('{"type":"FeatureCollection",')
                if self.bounds:
                    bbox = [
                        self.bounds["minlon"], self.bounds["minlat"],
                        self.bounds["maxlon"], self.bounds["maxlat"]
                    ]
                    self.fp.write(f'"bbox":{json.dumps(bbox)},')
                self.fp.write('"features":[')
        return self

    def write_item(self, item: dict):
        if self.mode == "ndjson":
            self.fp.write(json.dumps(item, ensure_ascii=False) + "\n")
            return

        if not self.first:
            self.fp.write(",\n" if self.indent else ",")
        else:
            self.first = False

        if self.indent:
            serialized = json.dumps(item, ensure_ascii=False, indent=self.indent)
            indented = "\n".join("    " + line for line in serialized.splitlines())
            self.fp.write(indented)
        else:
            self.fp.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")))

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.mode in ("osm", "geojson"):
            if self.indent:
                self.fp.write("\n  ]\n}\n")
            else:
                self.fp.write("]}\n")


def extract_metadata(attrib: dict, current_action: Optional[str], include_meta: bool) -> dict:
    """Extract optional OSM metadata (version, timestamp, uid, user, visible, action)."""
    meta = {}
    if "visible" in attrib:
        meta["visible"] = attrib["visible"].lower() != "false"
    if "action" in attrib:
        meta["action"] = attrib["action"]
    elif current_action:
        meta["action"] = current_action

    if include_meta:
        for int_key in ("version", "changeset", "uid"):
            if int_key in attrib:
                try:
                    meta[int_key] = int(attrib[int_key])
                except ValueError:
                    meta[int_key] = attrib[int_key]
        for str_key in ("timestamp", "user"):
            if str_key in attrib:
                meta[str_key] = attrib[str_key]
    return meta


def parse_child_data(elem: ET.Element) -> Tuple[dict, list, list, Optional[dict], Optional[dict]]:
    """
    Single-pass extraction of child elements inside a node, way, or relation:
    - tags (<tag k="" v=""/>)
    - nds (<nd ref="" lat="" lon=""/>)
    - members (<member .../>)
    - center (<center lat="" lon=""/>)
    - inline bounds (<bounds minlat="" .../>)
    """
    tags = {}
    nds = []
    members = []
    center = None
    bounds = None

    for child in elem:
        ctag = strip_ns(child.tag)
        cattrib = child.attrib
        if ctag == "tag":
            k = cattrib.get("k")
            v = cattrib.get("v")
            if k is not None and v is not None:
                tags[k] = v
        elif ctag == "nd":
            nd_entry = {}
            if "ref" in cattrib:
                try:
                    nd_entry["ref"] = int(cattrib["ref"])
                except ValueError:
                    nd_entry["ref"] = cattrib["ref"]
            if "lat" in cattrib and "lon" in cattrib:
                try:
                    nd_entry["lat"] = float(cattrib["lat"])
                    nd_entry["lon"] = float(cattrib["lon"])
                except ValueError:
                    pass
            nds.append(nd_entry)
        elif ctag == "member":
            m_entry = {
                "type": cattrib.get("type", ""),
                "role": cattrib.get("role", ""),
            }
            if "ref" in cattrib:
                try:
                    m_entry["ref"] = int(cattrib["ref"])
                except ValueError:
                    m_entry["ref"] = cattrib["ref"]
            if "lat" in cattrib and "lon" in cattrib:
                try:
                    m_entry["lat"] = float(cattrib["lat"])
                    m_entry["lon"] = float(cattrib["lon"])
                except ValueError:
                    pass
            # Overpass 'out geom;' embeds <nd lat="" lon=""/> inside relation <member>
            m_geom = []
            for mchild in child:
                if strip_ns(mchild.tag) == "nd" and "lat" in mchild.attrib and "lon" in mchild.attrib:
                    try:
                        m_geom.append({
                            "lat": float(mchild.attrib["lat"]),
                            "lon": float(mchild.attrib["lon"]),
                        })
                    except ValueError:
                        pass
            if m_geom:
                m_entry["geometry"] = m_geom
            members.append(m_entry)
        elif ctag == "center":
            if "lat" in cattrib and "lon" in cattrib:
                try:
                    center = {"lat": float(cattrib["lat"]), "lon": float(cattrib["lon"])}
                except ValueError:
                    pass
        elif ctag == "bounds":
            try:
                bounds = {
                    "minlat": float(cattrib["minlat"]),
                    "minlon": float(cattrib["minlon"]),
                    "maxlat": float(cattrib["maxlat"]),
                    "maxlon": float(cattrib["maxlon"]),
                }
            except (KeyError, ValueError):
                pass

    return tags, nds, members, center, bounds


def convert_to_osm_json(
    input_path: Path,
    output_path: Path,
    indent: Optional[int] = None,
    ndjson: bool = False,
    include_meta: bool = True,
):
    """Stream .osm XML directly to standard Overpass-style OSM JSON (or NDJSON)."""
    mode = "ndjson" if ndjson else "osm"
    current_action = None
    auto_id_counter = -1

    with open_osm_stream(input_path) as stream, open(output_path, "w", encoding="utf-8") as out_fp:
        context = iter(ET.iterparse(stream, events=("start", "end")))
        try:
            _, root = next(context)
        except StopIteration:
            raise ValueError("Input XML file is empty.")

        with StreamingJSONWriter(out_fp, mode=mode, indent=indent) as writer:
            for event, elem in context:
                tag = strip_ns(elem.tag)

                if event == "start":
                    if tag in ("create", "modify", "delete"):
                        current_action = tag
                    continue

                # event == "end"
                if tag in ("create", "modify", "delete"):
                    current_action = None
                    elem.clear()
                    continue

                if tag in ("node", "way", "relation"):
                    attrib = elem.attrib
                    raw_id = attrib.get("id")
                    try:
                        elem_id = int(raw_id) if raw_id is not None else auto_id_counter
                    except ValueError:
                        elem_id = raw_id
                    if raw_id is None:
                        auto_id_counter -= 1

                    obj = {"type": tag, "id": elem_id}

                    if tag == "node":
                        if "lat" in attrib and "lon" in attrib:
                            try:
                                obj["lat"] = float(attrib["lat"])
                                obj["lon"] = float(attrib["lon"])
                            except ValueError:
                                pass

                    tags, nds, members, center, bounds = parse_child_data(elem)

                    if tag == "way":
                        if any("ref" in n for n in nds):
                            obj["nodes"] = [n["ref"] for n in nds if "ref" in n]
                        if any("lat" in n and "lon" in n for n in nds):
                            obj["geometry"] = [
                                {"lat": n["lat"], "lon": n["lon"]}
                                for n in nds
                                if "lat" in n and "lon" in n
                            ]

                    elif tag == "relation":
                        obj["members"] = members

                    if center:
                        obj["center"] = center
                    if bounds:
                        obj["bounds"] = bounds
                    if tags:
                        obj["tags"] = tags

                    meta = extract_metadata(attrib, current_action, include_meta)
                    obj.update(meta)

                    writer.write_item(obj)
                    elem.clear()
                    root.clear()

                elif tag in ("bounds", "note", "meta", "remark"):
                    elem.clear()
                    root.clear()


def is_area_way(coords: List[List[float]], tags: dict) -> bool:
    """Determine if a closed coordinate sequence represents a Polygon rather than a LineString."""
    if len(coords) < 4 or coords[0] != coords[-1]:
        return False
    area_val = tags.get("area", "").lower()
    if area_val == "no":
        return False
    if area_val == "yes":
        return True
    for k, v in tags.items():
        if k in POLYGON_KEYS:
            if k in NON_POLYGON_VALUES and v in NON_POLYGON_VALUES[k]:
                continue
            return True
    return False


def stitch_ways_into_rings(segments: List[List[List[float]]]) -> Tuple[List[List[List[float]]], List[List[List[float]]]]:
    """
    Join disconnected way coordinate segments into closed polygon rings.
    Returns (closed_rings, unclosed_lines).
    """
    pool = [list(seg) for seg in segments if len(seg) >= 2]
    closed_rings = []
    unclosed_lines = []

    while pool:
        current = pool.pop(0)
        if len(current) >= 4 and current[0] == current[-1]:
            closed_rings.append(current)
            continue

        changed = True
        while changed:
            changed = False
            for i, candidate in enumerate(pool):
                if current[-1] == candidate[0]:
                    current.extend(candidate[1:])
                elif current[-1] == candidate[-1]:
                    current.extend(reversed(candidate[:-1]))
                elif current[0] == candidate[-1]:
                    current = candidate[:-1] + current
                elif current[0] == candidate[0]:
                    current = list(reversed(candidate[1:])) + current
                else:
                    continue

                pool.pop(i)
                changed = True
                break

            if len(current) >= 4 and current[0] == current[-1]:
                break

        if len(current) >= 4 and current[0] == current[-1]:
            closed_rings.append(current)
        else:
            unclosed_lines.append(current)

    return closed_rings, unclosed_lines


def point_in_ring(point: List[float], ring: List[List[float]]) -> bool:
    """Ray-casting point-in-polygon check to assign inner rings to their enclosing outer ring."""
    x, y = point[0], point[1]
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi):
            inside = not inside
        j = i
    return inside


def build_multipolygon_geometry(
    outer_segments: List[List[List[float]]],
    inner_segments: List[List[List[float]]],
) -> Optional[dict]:
    """Build a GeoJSON Polygon, MultiPolygon, or MultiLineString from outer and inner segments."""
    outer_rings, outer_unclosed = stitch_ways_into_rings(outer_segments)
    inner_rings, inner_unclosed = stitch_ways_into_rings(inner_segments)

    if not outer_rings:
        all_lines = outer_unclosed + inner_unclosed + inner_rings
        if not all_lines:
            return None
        if len(all_lines) == 1:
            return {"type": "LineString", "coordinates": all_lines[0]}
        return {"type": "MultiLineString", "coordinates": all_lines}

    polygons = [[ring] for ring in outer_rings]
    for iring in inner_rings:
        sample_pt = iring[0]
        assigned = False
        for poly in polygons:
            if point_in_ring(sample_pt, poly[0]):
                poly.append(iring)
                assigned = True
                break
        if not assigned:
            polygons[0].append(iring)

    if len(polygons) == 1 and not outer_unclosed:
        return {"type": "Polygon", "coordinates": polygons[0]}
    return {"type": "MultiPolygon", "coordinates": polygons}


def convert_to_geojson(
    input_path: Path,
    output_path: Path,
    indent: Optional[int] = None,
    ndjson: bool = False,
    include_untagged: bool = False,
    include_meta: bool = False,
):
    """
    Convert any .osm file to GeoJSON using a temporary SQLite database.
    Handles out-of-order elements, inline Overpass geometries, and multipolygon relations.
    """
    mode = "ndjson" if ndjson else "geojson"
    temp_dir = tempfile.mkdtemp(prefix="osm2json_")
    db_path = os.path.join(temp_dir, "index.sqlite")

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("PRAGMA synchronous = OFF;")
        cur.execute("PRAGMA journal_mode = MEMORY;")
        cur.execute("PRAGMA temp_store = MEMORY;")

        cur.execute("""
            CREATE TABLE nodes (
                id TEXT PRIMARY KEY,
                lon REAL,
                lat REAL,
                tags TEXT,
                meta TEXT,
                used INTEGER DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE ways (
                id TEXT PRIMARY KEY,
                nds TEXT,
                inline_coords TEXT,
                center TEXT,
                tags TEXT,
                meta TEXT,
                used_in_rel INTEGER DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE relations (
                id TEXT PRIMARY KEY,
                members TEXT,
                center TEXT,
                tags TEXT,
                meta TEXT
            )
        """)

        file_bounds = None
        current_action = None
        auto_id = -1

        # PASS 1: Stream XML into SQLite tables
        with open_osm_stream(input_path) as stream:
            context = iter(ET.iterparse(stream, events=("start", "end")))
            try:
                _, root = next(context)
            except StopIteration:
                raise ValueError("Input XML file is empty.")

            batch_nodes, batch_ways, batch_rels = [], [], []
            used_node_ids = set()

            for event, elem in context:
                tag = strip_ns(elem.tag)
                if event == "start":
                    if tag in ("create", "modify", "delete"):
                        current_action = tag
                    continue

                if tag in ("create", "modify", "delete"):
                    current_action = None
                    elem.clear()
                    continue

                if tag == "bounds" and file_bounds is None:
                    try:
                        file_bounds = {
                            "minlat": float(elem.attrib["minlat"]),
                            "minlon": float(elem.attrib["minlon"]),
                            "maxlat": float(elem.attrib["maxlat"]),
                            "maxlon": float(elem.attrib["maxlon"]),
                        }
                    except (KeyError, ValueError):
                        pass
                    elem.clear()
                    root.clear()

                elif tag == "node":
                    attrib = elem.attrib
                    if attrib.get("visible", "true").lower() == "false" or attrib.get("action") == "delete" or current_action == "delete":
                        elem.clear()
                        root.clear()
                        continue

                    nid = str(attrib.get("id", auto_id))
                    if "id" not in attrib:
                        auto_id -= 1

                    lon = float(attrib["lon"]) if "lon" in attrib else None
                    lat = float(attrib["lat"]) if "lat" in attrib else None
                    tags, _, _, _, _ = parse_child_data(elem)
                    meta = extract_metadata(attrib, current_action, include_meta)

                    batch_nodes.append((
                        nid,
                        lon,
                        lat,
                        json.dumps(tags, ensure_ascii=False) if tags else None,
                        json.dumps(meta, ensure_ascii=False) if meta else None,
                    ))

                    if len(batch_nodes) >= 25000:
                        cur.executemany(
                            "INSERT OR REPLACE INTO nodes (id, lon, lat, tags, meta) VALUES (?, ?, ?, ?, ?)",
                            batch_nodes,
                        )
                        batch_nodes.clear()
                    elem.clear()
                    root.clear()

                elif tag == "way":
                    attrib = elem.attrib
                    if attrib.get("visible", "true").lower() == "false" or attrib.get("action") == "delete" or current_action == "delete":
                        elem.clear()
                        root.clear()
                        continue

                    wid = str(attrib.get("id", auto_id))
                    if "id" not in attrib:
                        auto_id -= 1

                    tags, nds, _, center, _ = parse_child_data(elem)
                    meta = extract_metadata(attrib, current_action, include_meta)

                    refs = [str(n["ref"]) for n in nds if "ref" in n]
                    used_node_ids.update(refs)
                    inline_coords = [
                        [n["lon"], n["lat"]] for n in nds if "lon" in n and "lat" in n
                    ]

                    batch_ways.append((
                        wid,
                        json.dumps(refs) if refs else None,
                        json.dumps(inline_coords) if inline_coords else None,
                        json.dumps([center["lon"], center["lat"]]) if center else None,
                        json.dumps(tags, ensure_ascii=False) if tags else None,
                        json.dumps(meta, ensure_ascii=False) if meta else None,
                    ))

                    if len(batch_ways) >= 10000:
                        cur.executemany(
                            "INSERT OR REPLACE INTO ways (id, nds, inline_coords, center, tags, meta) VALUES (?, ?, ?, ?, ?, ?)",
                            batch_ways,
                        )
                        batch_ways.clear()
                    elem.clear()
                    root.clear()

                elif tag == "relation":
                    attrib = elem.attrib
                    if attrib.get("visible", "true").lower() == "false" or attrib.get("action") == "delete" or current_action == "delete":
                        elem.clear()
                        root.clear()
                        continue

                    rid = str(attrib.get("id", auto_id))
                    if "id" not in attrib:
                        auto_id -= 1

                    tags, _, members, center, _ = parse_child_data(elem)
                    meta = extract_metadata(attrib, current_action, include_meta)

                    for m in members:
                        if m.get("type") == "node" and "ref" in m:
                            used_node_ids.add(str(m["ref"]))

                    batch_rels.append((
                        rid,
                        json.dumps(members, ensure_ascii=False),
                        json.dumps([center["lon"], center["lat"]]) if center else None,
                        json.dumps(tags, ensure_ascii=False) if tags else None,
                        json.dumps(meta, ensure_ascii=False) if meta else None,
                    ))

                    if len(batch_rels) >= 5000:
                        cur.executemany(
                            "INSERT OR REPLACE INTO relations (id, members, center, tags, meta) VALUES (?, ?, ?, ?, ?)",
                            batch_rels,
                        )
                        batch_rels.clear()
                    elem.clear()
                    root.clear()

                else:
                    elem.clear()
                    root.clear()

            if batch_nodes:
                cur.executemany(
                    "INSERT OR REPLACE INTO nodes (id, lon, lat, tags, meta) VALUES (?, ?, ?, ?, ?)",
                    batch_nodes,
                )
            if batch_ways:
                cur.executemany(
                    "INSERT OR REPLACE INTO ways (id, nds, inline_coords, center, tags, meta) VALUES (?, ?, ?, ?, ?, ?)",
                    batch_ways,
                )
            if batch_rels:
                cur.executemany(
                    "INSERT OR REPLACE INTO relations (id, members, center, tags, meta) VALUES (?, ?, ?, ?, ?)",
                    batch_rels,
                )

            # Mark vertex nodes used by ways/relations
            used_list = list(used_node_ids)
            for i in range(0, len(used_list), 900):
                chunk = used_list[i:i + 900]
                placeholders = ",".join("?" for _ in chunk)
                cur.execute(f"UPDATE nodes SET used = 1 WHERE id IN ({placeholders})", chunk)
            conn.commit()

        def resolve_way_coords(wid: str) -> List[List[float]]:
            row = cur.execute("SELECT nds, inline_coords FROM ways WHERE id = ?", (wid,)).fetchone()
            if not row:
                return []
            nds_json, inline_json = row
            if inline_json:
                coords = json.loads(inline_json)
                if coords:
                    return coords
            if not nds_json:
                return []
            refs = json.loads(nds_json)
            if not refs:
                return []
            coord_map = {}
            for i in range(0, len(refs), 900):
                chunk = refs[i:i + 900]
                placeholders = ",".join("?" for _ in chunk)
                for nid, lon, lat in cur.execute(
                    f"SELECT id, lon, lat FROM nodes WHERE id IN ({placeholders})", chunk
                ):
                    if lon is not None and lat is not None:
                        coord_map[nid] = [lon, lat]
            return [coord_map[r] for r in refs if r in coord_map]

        # PASS 2: Stream out GeoJSON features (Nodes -> Ways -> Relations)
        with open(output_path, "w", encoding="utf-8") as out_fp:
            with StreamingJSONWriter(out_fp, mode=mode, indent=indent, bounds=file_bounds) as writer:

                # 1. Export Relations first (so we can mark untagged constituent ways as used)
                rel_cursor = conn.cursor()
                for rid, members_json, center_json, tags_json, meta_json in rel_cursor.execute(
                    "SELECT id, members, center, tags, meta FROM relations"
                ):
                    tags = json.loads(tags_json) if tags_json else {}
                    meta = json.loads(meta_json) if meta_json else {}
                    members = json.loads(members_json) if members_json else []
                    rel_type = tags.get("type", "")

                    geometry = None
                    if rel_type in ("multipolygon", "boundary"):
                        outer_segs, inner_segs = [], []
                        way_ids_used = []
                        for m in members:
                            if m.get("type") == "way":
                                coords = []
                                if "geometry" in m:
                                    coords = [[pt["lon"], pt["lat"]] for pt in m["geometry"]]
                                elif "ref" in m:
                                    wref = str(m["ref"])
                                    way_ids_used.append(wref)
                                    coords = resolve_way_coords(wref)
                                if len(coords) >= 2:
                                    if m.get("role") == "inner":
                                        inner_segs.append(coords)
                                    else:
                                        outer_segs.append(coords)
                        geometry = build_multipolygon_geometry(outer_segs, inner_segs)
                        if way_ids_used:
                            for i in range(0, len(way_ids_used), 900):
                                chunk = way_ids_used[i:i + 900]
                                ph = ",".join("?" for _ in chunk)
                                cur.execute(f"UPDATE ways SET used_in_rel = 1 WHERE id IN ({ph})", chunk)

                    else:
                        # Route, collection, or other relation -> GeometryCollection / MultiLineString
                        geoms = []
                        for m in members:
                            mtype = m.get("type")
                            if mtype == "node":
                                if "lon" in m and "lat" in m:
                                    geoms.append({"type": "Point", "coordinates": [m["lon"], m["lat"]]})
                                elif "ref" in m:
                                    nrow = cur.execute(
                                        "SELECT lon, lat FROM nodes WHERE id = ?", (str(m["ref"]),)
                                    ).fetchone()
                                    if nrow and nrow[0] is not None and nrow[1] is not None:
                                        geoms.append({"type": "Point", "coordinates": [nrow[0], nrow[1]]})
                            elif mtype == "way":
                                coords = []
                                if "geometry" in m:
                                    coords = [[pt["lon"], pt["lat"]] for pt in m["geometry"]]
                                elif "ref" in m:
                                    coords = resolve_way_coords(str(m["ref"]))
                                if len(coords) >= 2:
                                    geoms.append({"type": "LineString", "coordinates": coords})
                        if geoms:
                            if all(g["type"] == "LineString" for g in geoms):
                                geometry = {
                                    "type": "MultiLineString",
                                    "coordinates": [g["coordinates"] for g in geoms],
                                }
                            else:
                                geometry = {"type": "GeometryCollection", "geometries": geoms}

                    if not geometry and center_json:
                        geometry = {"type": "Point", "coordinates": json.loads(center_json)}

                    if geometry:
                        props = {"@id": f"relation/{rid}", **meta, **tags}
                        writer.write_item({
                            "type": "Feature",
                            "id": f"relation/{rid}",
                            "properties": props,
                            "geometry": geometry,
                        })

                conn.commit()

                # 2. Export Ways
                way_cursor = conn.cursor()
                for wid, nds_json, inline_json, center_json, tags_json, meta_json, used_in_rel in way_cursor.execute(
                    "SELECT id, nds, inline_coords, center, tags, meta, used_in_rel FROM ways"
                ):
                    tags = json.loads(tags_json) if tags_json else {}
                    if used_in_rel and not tags and not include_untagged:
                        continue
                    if not tags and not include_untagged:
                        # Still export untagged ways if no relations consumed them only when requested,
                        # OR if it's a standalone geometry without tags in a tiny file
                        pass

                    coords = resolve_way_coords(wid)
                    geometry = None
                    if len(coords) >= 2:
                        if is_area_way(coords, tags):
                            geometry = {"type": "Polygon", "coordinates": [coords]}
                        else:
                            geometry = {"type": "LineString", "coordinates": coords}
                    elif len(coords) == 1:
                        geometry = {"type": "Point", "coordinates": coords[0]}
                    elif center_json:
                        geometry = {"type": "Point", "coordinates": json.loads(center_json)}

                    if geometry and (tags or include_untagged or not used_in_rel):
                        meta = json.loads(meta_json) if meta_json else {}
                        props = {"@id": f"way/{wid}", **meta, **tags}
                        writer.write_item({
                            "type": "Feature",
                            "id": f"way/{wid}",
                            "properties": props,
                            "geometry": geometry,
                        })

                # 3. Export Nodes (Tagged POIs, plus standalone untagged nodes if requested)
                node_cursor = conn.cursor()
                for nid, lon, lat, tags_json, meta_json, used in node_cursor.execute(
                    "SELECT id, lon, lat, tags, meta, used FROM nodes"
                ):
                    if lon is None or lat is None:
                        continue
                    tags = json.loads(tags_json) if tags_json else {}
                    if not tags and (used == 1 or not include_untagged):
                        continue
                    meta = json.loads(meta_json) if meta_json else {}
                    props = {"@id": f"node/{nid}", **meta, **tags}
                    writer.write_item({
                        "type": "Feature",
                        "id": f"node/{nid}",
                        "properties": props,
                        "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    })

        conn.close()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def convert_pbf_directly_with_pyosmium(
    input_path: Path,
    output_path: Path,
    geojson: bool,
    indent: Optional[int],
    ndjson: bool,
) -> bool:
    """If pyosmium is installed and osmium CLI isn't, handle .osm.pbf natively in Python."""
    try:
        import osmium
    except ImportError:
        return False

    # If osmium CLI exists, open_osm_stream already streams it cleanly.
    if shutil.which("osmium"):
        return False

    mode = "ndjson" if ndjson else ("geojson" if geojson else "osm")
    with open(output_path, "w", encoding="utf-8") as out_fp:
        with StreamingJSONWriter(out_fp, mode=mode, indent=indent) as writer:

            class PBFHandler(osmium.SimpleHandler):
                def __init__(self):
                    super().__init__()
                    self.fab = osmium.geom.GeoJSONFactory() if geojson else None

                def node(self, n):
                    tags = {t.k: t.v for t in n.tags}
                    if geojson:
                        if not tags:
                            return
                        geom = json.loads(self.fab.create_point(n))
                        writer.write_item({
                            "type": "Feature",
                            "id": f"node/{n.id}",
                            "properties": {"@id": f"node/{n.id}", **tags},
                            "geometry": geom,
                        })
                    else:
                        item = {"type": "node", "id": n.id, "lat": n.location.lat, "lon": n.location.lon}
                        if tags:
                            item["tags"] = tags
                        writer.write_item(item)

                def way(self, w):
                    tags = {t.k: t.v for t in w.tags}
                    if geojson:
                        try:
                            geom = json.loads(self.fab.create_linestring(w))
                            if is_area_way(geom["coordinates"], tags):
                                geom = {"type": "Polygon", "coordinates": [geom["coordinates"]]}
                            writer.write_item({
                                "type": "Feature",
                                "id": f"way/{w.id}",
                                "properties": {"@id": f"way/{w.id}", **tags},
                                "geometry": geom,
                            })
                        except Exception:
                            pass
                    else:
                        item = {"type": "way", "id": w.id, "nodes": [n.ref for n in w.nodes]}
                        if tags:
                            item["tags"] = tags
                        writer.write_item(item)

                def area(self, a):
                    if not geojson:
                        return
                    tags = {t.k: t.v for t in a.tags}
                    try:
                        geom = json.loads(self.fab.create_multipolygon(a))
                        osm_type = "way" if a.from_way() else "relation"
                        orig_id = a.orig_id()
                        writer.write_item({
                            "type": "Feature",
                            "id": f"{osm_type}/{orig_id}",
                            "properties": {"@id": f"{osm_type}/{orig_id}", **tags},
                            "geometry": geom,
                        })
                    except Exception:
                        pass

                def relation(self, r):
                    if geojson:
                        return
                    tags = {t.k: t.v for t in r.tags}
                    item = {
                        "type": "relation",
                        "id": r.id,
                        "members": [{"type": m.type, "ref": m.ref, "role": m.role} for m in r.members],
                    }
                    if tags:
                        item["tags"] = tags
                    writer.write_item(item)

            handler = PBFHandler()
            handler.apply_file(str(input_path), locations=geojson)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Universal converter for any .osm/.osc/.osh/.gz/.bz2/.xz/.zip/.zst/.pbf file to JSON or GeoJSON."
    )
    parser.add_argument("input", type=Path, help="Input file path (or '-' for stdin)")
    parser.add_argument("output", type=Path, help="Output .json, .geojson, or .ndjson file path")
    parser.add_argument(
        "--geojson",
        action="store_true",
        help="Convert to GeoJSON FeatureCollection (automatically enabled if output ends with .geojson)",
    )
    parser.add_argument(
        "--ndjson",
        action="store_true",
        help="Stream output as newline-delimited JSON (one JSON object/Feature per line)",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output with 2-space indentation",
    )
    parser.add_argument(
        "--include-untagged",
        action="store_true",
        help="In GeoJSON mode, also export standalone untagged nodes and ways",
    )
    parser.add_argument(
        "--meta",
        action="store_true",
        help="Include OSM metadata (version, timestamp, changeset, uid, user) in GeoJSON properties",
    )

    args = parser.parse_args()

    if str(args.input) != "-" and not args.input.exists():
        print(f"Error: Input file '{args.input}' does not exist.", file=sys.stderr)
        sys.exit(1)

    use_geojson = args.geojson or args.output.name.lower().endswith(".geojson")
    use_ndjson = args.ndjson or args.output.name.lower().endswith((".ndjson", ".jsonl", ".geojsonl"))
    indent = 2 if args.pretty and not use_ndjson else None

    try:
        if str(args.input).lower().endswith(".pbf") and convert_pbf_directly_with_pyosmium(
            args.input, args.output, use_geojson, indent, use_ndjson
        ):
            print(f"Converted (.pbf via pyosmium) -> {args.output}")
            return

        if use_geojson:
            convert_to_geojson(
                args.input,
                args.output,
                indent=indent,
                ndjson=use_ndjson,
                include_untagged=args.include_untagged,
                include_meta=args.meta,
            )
        else:
            convert_to_osm_json(
                args.input,
                args.output,
                indent=indent,
                ndjson=use_ndjson,
                include_meta=True,
            )
        print(f"Successfully converted '{args.input}' -> '{args.output}'")

    except Exception as exc:
        print(f"Conversion failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
