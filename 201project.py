'''Anu, Jun, Becca
SI 201 Museums Project'''

import requests
import json
import sqlite3
import time

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


def get_met_data(target_count=100):
    # 1. get all object IDs
    ids_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
    ids_data = requests.get(ids_url, timeout=10).json()

    if "objectIDs" not in ids_data:
        print("Error: MET did not return object IDs:", ids_data)
        return []

    object_ids = ids_data["objectIDs"]

    raw_objects = []
    count = 0

    for oid in object_ids:
        if count >= target_count:
            break

        # prevent rate limiting
        time.sleep(0.1)

        obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{oid}"
        try:
            data = requests.get(obj_url, timeout=10).json()
        except:
            continue

        # check if data is valid
        if not isinstance(data, dict):
            continue
        if "title" not in data:
            continue

        raw_objects.append(data)
        count += 1

    return raw_objects


def insert_met_data(conn, cur, raw_data):
    for item in raw_data:
        # Insert into lookup tables and get their IDs
        museum_name = "The Met"
        cur.execute("INSERT OR IGNORE INTO museum (name) VALUES (?);", (museum_name,))
        cur.execute("SELECT id FROM museum WHERE name = ?;", (museum_name,))
        museum_id = cur.fetchone()[0]

        title_text = item.get("title", "Unknown Title")
        cur.execute("INSERT OR IGNORE INTO titles (title_text) VALUES (?);", (title_text,))
        cur.execute("SELECT id FROM titles WHERE title_text = ?;", (title_text,))
        title_id = cur.fetchone()[0]

        artist_name = item.get("artistDisplayName", "Unknown Artist")
        cur.execute("INSERT OR IGNORE INTO artists (artist_name) VALUES (?);", (artist_name,))
        cur.execute("SELECT id FROM artists WHERE artist_name = ?;", (artist_name,))
        artist_id = cur.fetchone()[0]

        medium_text = item.get("medium", "Unknown Medium")
        cur.execute("INSERT OR IGNORE INTO mediums (medium_text) VALUES (?);", (medium_text,))
        cur.execute("SELECT id FROM mediums WHERE medium_text = ?;", (medium_text,))
        medium_id = cur.fetchone()[0]

        classification_text = item.get("classification", "Unknown Classification")
        cur.execute("INSERT OR IGNORE INTO classifications (classification_text) VALUES (?);", (classification_text,))
        cur.execute("SELECT id FROM classifications WHERE classification_text = ?;", (classification_text,))
        classification_id = cur.fetchone()[0]

        culture_text = item.get("culture", "Unknown Culture")
        cur.execute("INSERT OR IGNORE INTO cultures (culture_text) VALUES (?);", (culture_text,))
        cur.execute("SELECT id FROM cultures WHERE culture_text = ?;", (culture_text,))
        culture_id = cur.fetchone()[0]

        date_text = item.get("objectDate", "Unknown Date")
        cur.execute("INSERT OR IGNORE INTO dates (date_text) VALUES (?);", (date_text,))
        cur.execute("SELECT id FROM dates WHERE date_text = ?;", (date_text,))
        date_id = cur.fetchone()[0]

        # Insert into artworks table
        original_id = item.get("objectID")
        cur.execute("""
            INSERT OR IGNORE INTO artworks (
                original_id, museum_id, title_id, artist_id,
                medium_id, classification_id, culture_id, date_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            original_id, museum_id, title_id, artist_id,
            medium_id, classification_id, culture_id, date_id
        ))  


def main():
    # 1. Connect to the database
    conn = sqlite3.connect("artmuseum.db")
    cur = conn.cursor()

    # 2. Ensure tables exist
    create_tables(conn, cur)
    
    # 3. Get the raw data from the MET API
    # I'll use 5 here, as per your previous example
    raw_met_data = get_met_data(target_count=5) 

    # 4. Insert the data into the tables
    insert_met_data(conn, cur, raw_met_data)

    # 5. Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
