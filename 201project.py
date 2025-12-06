'''Anu, Jun, Becca
SI 201 Museums Project'''


import requests
import json
import sqlite3
import time
import random

def create_tables(conn, cur):
    # ---------------------------
    # Lookup Tables (store TEXT)
    # ---------------------------

    cur.execute("""
        CREATE TABLE IF NOT EXISTS museum (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS titles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_text TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist_name TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mediums (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            medium_text TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            classification_text TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cultures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            culture_text TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_text TEXT UNIQUE
        );
    """)

    # ---------------------------
    # Main Artworks Table (INT ONLY)
    # ---------------------------

    cur.execute("""
        CREATE TABLE IF NOT EXISTS artworks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            original_id INTEGER,                -- API's object_id (OK to keep as integer)

            museum_id INTEGER,                  -- FK → museum.id
            title_id INTEGER,                   -- FK → titles.id
            artist_id INTEGER,                  -- FK → artists.id
            medium_id INTEGER,                  -- FK → mediums.id
            classification_id INTEGER,          -- FK → classifications.id
            culture_id INTEGER,                 -- FK → cultures.id
            date_id INTEGER,                    -- FK → dates.id

            -- Prevent duplicates from same museum
            UNIQUE(museum_id, original_id),

            FOREIGN KEY (museum_id) REFERENCES museum(id),
            FOREIGN KEY (title_id) REFERENCES titles(id),
            FOREIGN KEY (artist_id) REFERENCES artists(id),
            FOREIGN KEY (medium_id) REFERENCES mediums(id),
            FOREIGN KEY (classification_id) REFERENCES classifications(id),
            FOREIGN KEY (culture_id) REFERENCES cultures(id),
            FOREIGN KEY (date_id) REFERENCES dates(id)
        );
    """)

    conn.commit()
    print("Tables created successfully!")


def get_harvard_data(target_count=25):
    """
    Fetch artwork metadata from the Harvard Art Museums API and normalize it
    to match the MET schema used by insert_met_data().

    Returns

    list[dict]
        List of normalized artwork dicts with keys:
        objectID, title, artistDisplayName, medium,
        classification, culture, objectDate
    """

    api_key = "58f23874-0244-4cb8-b162-ae364e69d3e0"

    base_url = "https://api.harvardartmuseums.org/object"
    params = {
        "apikey": api_key,
        "size": min(target_count, 25),   # Harvard max 100; project max 25
        "page": 1
    }

    print("Requesting Harvard Art Museums object list...")

    raw_objects = []
    required_fields = ["title", "artistDisplayName", "medium",
                       "classification", "culture", "objectDate"]

    while len(raw_objects) < target_count:
        try:
            response = requests.get(base_url, params=params, timeout=10)
            data = response.json()
        except Exception as e:
            print("Error fetching Harvard data:", e)
            break

        records = data.get("records", [])
        if not records:
            print("No more Harvard records returned.")
            break

        for rec in records:
            # Extract artist name from "people" list if available
            if rec.get("people"):
                artist_name = rec["people"][0].get("name")
            else:
                artist_name = None

            # Normalize fields to match MET structure
            normalized = {
                "objectID": rec.get("objectid"),
                "title": rec.get("title"),
                "artistDisplayName": artist_name,
                "medium": rec.get("medium"),
                "classification": rec.get("classification"),
                "culture": rec.get("culture"),
                "objectDate": rec.get("dated")
            }

            if not all(normalized.get(f) for f in required_fields):
                continue

            raw_objects.append(normalized)

            if len(raw_objects) >= target_count:
                break

        # Pagination handling
        info = data.get("info", {})
        current_page = info.get("page")
        total_pages = info.get("pages")

        if not current_page or not total_pages or current_page >= total_pages:
            break

        params["page"] = current_page + 1
        time.sleep(0.1)

    print(f"Fetched {len(raw_objects)} objects from the Harvard Art Museums API.")
    print(raw_objects[:2])  # preview first 2 objects
    return raw_objects


def get_coop_data(target_count=25):
    """
    Fetch artwork metadata from the Cooper Hewitt API and normalize it
    to match the MET schema used by insert_met_data().

    Returns
    -------
    list[dict]
        List of normalized artwork dicts with keys:
        objectID, title, artistDisplayName, medium,
        classification, culture, objectDate
    """

    access_token = "d53d099f0b54183fa59f1b54475e2489"

    base_url = "https://api.collection.cooperhewitt.org/rest/"
    params = {
        "method": "cooperhewitt.objects.getRandom",
        "access_token": access_token,
        "has_image": 0  # we don't *need* images for this project
    }

    print("Requesting Cooper Hewitt random objects...")

    raw_objects = []

    # We’ll make sure these core fields exist (the others have safe defaults).
    required_core_fields = ["objectID", "title", "artistDisplayName", "medium", "objectDate"]

    while len(raw_objects) < target_count:
        try:
            resp = requests.get(base_url, params=params, timeout=10)
            data = resp.json()
        except Exception as e:
            print("Error fetching Cooper Hewitt data:", e)
            break

        obj = data.get("object")
        if not obj:
            continue

        # Try to get an artist / designer name from participants or people
        artist_name = None
        for key in ("participants", "people"):
            people_list = obj.get(key)
            if isinstance(people_list, list) and people_list:
                person = people_list[0]
                artist_name = (
                    person.get("person_name")
                    or person.get("name")
                    or person.get("display_name")
                )
                if artist_name:
                    break

        # Normalize to MET-style keys so insert_met_data can reuse it
        normalized = {
            "objectID": obj.get("id"),
            "title": obj.get("title") or "Unknown Title",
            "artistDisplayName": artist_name or "Unknown Artist",
            "medium": (
                obj.get("medium")
                or obj.get("medium_description")
                or "Unknown Medium"
            ),
            # Cooper Hewitt uses "type" for kind of object; good enough as "classification"
            "classification": (
                obj.get("type")
                or obj.get("type_name")
                or "Unknown Classification"
            ),
            # Rough stand-in for "culture"
            "culture": (
                obj.get("woe:country")
                or obj.get("country")
                or "Unknown Culture"
            ),
            # Date fields vary widely; pick whatever exists
            "objectDate": (
                obj.get("date")
                or obj.get("display_date")
                or obj.get("year_start")
                or "Unknown Date"
            )
        }

        # Require the core fields to be present / non-empty
        if not all(normalized.get(f) for f in required_core_fields):
            continue

        raw_objects.append(normalized)
        time.sleep(0.1)  # be polite to the API

    print(f"Fetched {len(raw_objects)} objects from the Cooper Hewitt API.")
    return raw_objects


def main():
    conn = sqlite3.connect("artmuseum.db")
    cur = conn.cursor()

    create_tables(conn, cur)
    # Harvard
    raw_harvard_data = get_harvard_data(target_count=5)
    # Cooper Hewitt
    raw_coop_data = get_coop_data(target_count=5)




    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
