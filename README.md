# somson

A fast, zero-dependency Python CLI tool that converts OpenStreetMap (`.osm`) files into **OSM JSON**, **GeoJSON**, or **NDJSON**. Uses streaming I/O and disk-backed SQLite indexing to handle large map extracts without running out of memory.

## Features
* **Universal input:** Supports `.osm`, `.osc`, `.osh`, `.xml`, and compressed `.gz`, `.bz2`, `.xz`, `.zip`, `.zst`, and `.pbf` files.
* **Low memory footprint:** Streams XML and writes JSON incrementally.
* **GeoJSON geometry resolution:** Automatically resolves node coordinates, ways, and multipolygon relations.

## Installation

Open your terminal in the directory containing `somson.py` and run:

```bash
chmod +x somson.py
sudo mv somson.py /usr/local/bin/somson
```

*(Alternatively, run it directly without installing via `python3 somson.py ...`)*

## Usage

### 1. Convert to Standard OSM JSON
```bash
# Minified JSON (smaller file size for huge extracts)
somson map.osm output.json

# Pretty-printed JSON
somson map.osm output.json --pretty
```

### 2. Convert to GeoJSON
```bash
# Force GeoJSON inside a .json file
somson map.osm output.json --geojson --pretty

# Or name the output .geojson to trigger GeoJSON mode automatically
somson map.osm output.geojson --pretty
```

### 3. Stream Newline-Delimited JSON (NDJSON)
```bash
somson map.osm output.ndjson --ndjson
```
