'''Anu, Jun, Becca
SI 201 Museums Project'''

import requests
import json
import sqlite3

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


def main():
    # connect to the database
    conn = sqlite3.connect("artmuseum.db")
    cur = conn.cursor()

    # call your create tables function
    create_tables(conn, cur)

    conn.close()

if __name__ == "__main__":
    main()