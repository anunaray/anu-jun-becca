'''Anu, Jun, Becca
SI 201 Museums Project'''

import time
import requests
import json
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import re

### DATABASE SETUP ###

def create_tables(conn, cur):
    # Lookup Tables (store TEXT)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS museum (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS titles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_text TEXT UNIQUE NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist_name TEXT UNIQUE NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mediums (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            medium_text TEXT UNIQUE NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            classification_text TEXT UNIQUE NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cultures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            culture_text TEXT UNIQUE NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_text TEXT UNIQUE NOT NULL
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

def insert_museum (conn, cur, museum_name):
    cur.execute("INSERT OR IGNORE INTO museum (name) VALUES (?);", (museum_name,))
    conn.commit()
    cur.execute("SELECT id FROM museum WHERE name = ?;", (museum_name,))
    museum_id = cur.fetchone()[0]
    return museum_id

### AIC API DATA RETRIEVAL AND INSERTION ###

def get_aic_data(page):
    print("Requesting Art Institute of Chicago artwork data...")
    all_url = f"https://api.artic.edu/api/v1/artworks?page={page}&limit=100"
    response = requests.get(all_url)
    data = response.json()
    return data['data']

def get_or_create_id(cur, conn, table, column, value, fallback):
    """
    Safely gets or creates a lookup-table ID.
    Never allows NULL or empty strings into lookup tables.
    """
    if value is None or str(value).strip() == "":
        value = fallback

    cur.execute(
        f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?);",
        (value,)
    )
    conn.commit()

    cur.execute(
        f"SELECT id FROM {table} WHERE {column} = ?;",
        (value,)
    )
    return cur.fetchone()[0]

def insert_aic_data(conn, cur, data_dict_list, limit=25): 
    """
    Insert Art Institute of Chicago artworks into the database.
    """
    print("Inserting Art Institute of Chicago artwork data into database...")
    museum_name = "Art Institute of Chicago"
    museum_id = insert_museum(conn, cur, museum_name)

    '''subsections to get:
        title
        place_of_origin
        artist_title
        medium_display
        classification_title
        date_display
     '''
    count = 0
    for item in data_dict_list:
        if count >= limit:
            break  # Stop after 25 inserts

        title = item.get("title")
        artist = item.get("artist_title")
        origin = item.get("place_of_origin")
        medium = item.get("medium_display")
        classification = item.get("classification_title")
        date = item.get("date_display")
        original_id = item.get("id")

        cur.execute("SELECT 1 FROM artworks WHERE original_id = ? AND museum_id = ?", (original_id, museum_id))
        if cur.fetchone():
            continue # if already in db, skip

        # Get IDs from lookup tables with fallbacks to avoid NULL values
        title_id = get_or_create_id(cur, conn, "titles", "title_text", title, "Untitled" )
        artist_id = get_or_create_id(cur, conn, "artists", "artist_name", artist, "Unknown Artist")
        culture_id = get_or_create_id(cur, conn, "cultures", "culture_text", origin, "Unknown Culture")
        medium_id = get_or_create_id(cur, conn, "mediums", "medium_text", medium, "Unknown Medium")
        classification_id = get_or_create_id(cur, conn, "classifications", "classification_text", classification, "Unclassified")
        date_id = get_or_create_id(cur, conn, "dates", "date_text", date, "Unknown Date")

        # Insert into artworks
        cur.execute("""
            INSERT OR IGNORE INTO artworks
            (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id))

        count += 1

    conn.commit()
    print(f"AIC data insertion complete - INSERTED {count} ARTWORKS")

### COOPER HEWITT API DATA RETRIEVAL AND INSERTION ###
def get_coop_data(target_count=200):
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
        "method": "cooperhewitt.objects.getOnDisplay",
        "access_token": access_token,
        "has_image": 0  # we don't *need* images for this project
    }

    print("Requesting Cooper Hewitt random objects...")

    raw_objects = []

    # We’ll make sure these core fields exist (the others have safe defaults).
    required_core_fields = ["id", "title", "artist_title", "medium_display", "date_display"]

    objects = []

    try:
        resp = requests.get(base_url, params=params, timeout=10)
        data = resp.json()
        objects = data.get("objects", [])
    except Exception as e:
        print("Error fetching Cooper Hewitt data:", e)

    for obj in objects:
        if len(raw_objects) >= target_count:
            break

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
            "id": obj.get("id"),
            "title": obj.get("title") or "Unknown Title",
            "artist_title": artist_name or "Unknown Artist",
            "medium_display": (
                obj.get("medium")
                or obj.get("medium_description")
                or "Unknown Medium"
            ),
            # Cooper Hewitt uses "type" for kind of object; good enough as "classification"
            "classification_title": (
                obj.get("type")
                or obj.get("type_name")
                or "Unknown Classification"
            ),
            # Rough stand-in for "culture"
            "place_of_origin": (
                obj.get("woe:country")
                or obj.get("country")
                or "Unknown Culture"
            ),
            # Date fields vary widely; pick whatever exists
            "date_display": (
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

    print(f"Fetched {len(raw_objects)} objects from the Cooper Hewitt API.")
    return raw_objects

def insert_coop_data(conn, cur, data_dict_list, limit=25): 
    """
    Insert Cooper Hewitt artworks into the database.
    """
    print("Inserting Cooper Hewitt artwork data into database...")
    museum_name = "Cooper Hewitt"
    museum_id = insert_museum(conn, cur, museum_name)

    '''subsections to get:
        title
        place_of_origin
        artist_title
        medium_display
        classification_title
        date_display
     '''
    count = 0
    for item in data_dict_list:
        if count >= limit:
            break  # Stop after 25 inserts

        title = item.get("title")
        artist = item.get("artist_title")
        origin = item.get("place_of_origin")
        medium = item.get("medium_display")
        classification = item.get("classification_title")
        date = item.get("date_display")
        original_id = item.get("id")

        cur.execute("SELECT 1 FROM artworks WHERE original_id = ? AND museum_id = ?", (original_id, museum_id))
        if cur.fetchone():
            continue # if already in db, skip

        # Get IDs from lookup tables with fallbacks to avoid NULL values
        title_id = get_or_create_id(cur, conn, "titles", "title_text", title, "Untitled" )
        artist_id = get_or_create_id(cur, conn, "artists", "artist_name", artist, "Unknown Artist")
        culture_id = get_or_create_id(cur, conn, "cultures", "culture_text", origin, "Unknown Culture")
        medium_id = get_or_create_id(cur, conn, "mediums", "medium_text", medium, "Unknown Medium")
        classification_id = get_or_create_id(cur, conn, "classifications", "classification_text", classification, "Unclassified")
        date_id = get_or_create_id(cur, conn, "dates", "date_text", date, "Unknown Date")

        # Insert into artworks
        cur.execute("""
            INSERT OR IGNORE INTO artworks
            (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id))

        count += 1

    conn.commit()
    print(f"Cooper Hewitt data insertion complete - INSERTED {count} ARTWORKS")

### HARVARD ART MUSEUM API DATA RETRIEVAL AND INSERTION ###
def get_harvard_data(target_count=200):
   """
   Fetch artwork metadata from the Harvard Art Museums API and normalize it
   to match the MET schema used by insert_met_data().


   Returns
   -------
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
   required_fields = ["title", "artist_title", "medium_display",
                      "classification_title", "place_of_origin", "date_display"]


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
               "id": rec.get("objectid"),
               "title": rec.get("title"),
               "artist_title": artist_name,
               "medium_display": rec.get("medium"),
               "classification_title": rec.get("classification"),
               "place_of_origin": rec.get("culture"),
               "date_display": rec.get("dated")
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
   return raw_objects

def insert_harvard_data(conn, cur, data_dict_list, limit=25):
    """
    Insert Harvard Art Museum artworks into the database.
    """
    print("Inserting Harvard Art Museum artwork data into database...")
    museum_name = "Harvard Art Museum"
    museum_id = insert_museum(conn, cur, museum_name)

    '''subsections to get:
        title
        place_of_origin
        artist_title
        medium_display
        classification_title
        date_display
     '''
    count = 0
    for item in data_dict_list:
        if count >= limit:
            break 

        title = item.get("title")
        artist = item.get("artist_title")
        origin = item.get("place_of_origin")
        medium = item.get("medium_display")
        classification = item.get("classification_title")
        date = item.get("date_display")
        original_id = item.get("id")

        cur.execute("SELECT 1 FROM artworks WHERE original_id = ? AND museum_id = ?", (original_id, museum_id))
        if cur.fetchone():
            continue # if already in db, skip

        # Get IDs from lookup tables with fallbacks to avoid NULL values
        title_id = get_or_create_id(cur, conn, "titles", "title_text", title, "Untitled" )
        artist_id = get_or_create_id(cur, conn, "artists", "artist_name", artist, "Unknown Artist")
        culture_id = get_or_create_id(cur, conn, "cultures", "culture_text", origin, "Unknown Culture")
        medium_id = get_or_create_id(cur, conn, "mediums", "medium_text", medium, "Unknown Medium")
        classification_id = get_or_create_id(cur, conn, "classifications", "classification_text", classification, "Unclassified")
        date_id = get_or_create_id(cur, conn, "dates", "date_text", date, "Unknown Date")

        # Insert into artworks
        cur.execute("""
            INSERT OR IGNORE INTO artworks
            (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id))
        count += 1

    conn.commit()
    print(F"Harvard Art Museum data insertion complete - INSERTED {count} ARTWORKS")

### CLEVELAND MUSEUM API DATA RETRIEVAL AND INSERTION ###

def get_cleveland_data():
    print("Requesting Cleveland Museum of Art artwork data...")
    url = "https://openaccess-api.clevelandart.org/api/artworks/"
    response = requests.get(url)
    data = response.json()
    
    return data['data']

def insert_cleveland_data(conn, cur, data_dict_list, limit = 25):
    ''' items to get
    "id" - original id
    "culture" - this is a lst in this db, get just the first item - culture
    "technique" - medium
    "type" - classification
    "title"
    "creation_date_latest" - date. only get the latest date
    "creators" lst of dicts - get item 0, and then description. use regex to filter out name only
    '''
    print("Inserting Cleveland Museum of Art artwork data into database...")
    museum_name = "Cleveland Museum of Art"
    museum_id = insert_museum(conn, cur, museum_name)

    count = 0

    for item in data_dict_list:
        if count >= limit:
            break

        original_id = item.get("id")
        title = item.get("title")
        culture_lst = item.get("culture", [])
        culture = culture_lst[0] if culture_lst else "Unknown Culture"
        medium = item.get("technique")
        classification = item.get("type")
        date = item.get("creation_date_latest")
        
        creators_lst = item.get("creators", [])
        if creators_lst:
            creator_desc = creators_lst[0].get("description", "")
            # use regex to extract name before any comma or parenthesis
            match = re.match(r"^([^,(]+)", creator_desc)
            artist = match.group(1).strip() if match else "Unknown Artist"
        else:
            artist = "Unknown Artist"

        cur.execute("SELECT 1 FROM artworks WHERE original_id = ? AND museum_id = ?", (original_id, museum_id))
        if cur.fetchone():
            continue # if already in db, skip

        # Get IDs from lookup tables with fallbacks to avoid NULL values
        title_id = get_or_create_id(cur, conn, "titles", "title_text", title, "Untitled" )
        artist_id = get_or_create_id(cur, conn, "artists", "artist_name", artist, "Unknown Artist")
        culture_id = get_or_create_id(cur, conn, "cultures", "culture_text", culture, "Unknown Culture")
        medium_id = get_or_create_id(cur, conn, "mediums", "medium_text", medium, "Unknown Medium")
        classification_id = get_or_create_id(cur, conn, "classifications", "classification_text", classification, "Unclassified")
        date_id = get_or_create_id(cur, conn, "dates", "date_text", date, "Unknown Date")  
        # Insert into artworks
        cur.execute("""
            INSERT OR IGNORE INTO artworks
            (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id))  

        count += 1
    
    #print (data_dict_list[2]["creators"][0]["description"])
    #print (data_dict_list[3]["creators"][0]["description"])
    conn.commit()
    print(f"Cleveland Museum of Art data insertion complete - INSERTED {count} ARTWORKS")


### VISUALIZATIONS ###
def select_and_calculate_metrics(conn):
    """
    Pull data from SQLite and compute comparative metrics:
    - medium distribution per museum
    - cultural/geographic distribution per museum
    - simple period distribution (by century) per museum

    Returns:
        dict[str, pd.DataFrame]
    """
    cur = conn.cursor()

    # Join everything into one wide table
    query = """
        SELECT
            m.name AS museum_name,
            t.title_text AS title,
            a2.artist_name AS artist,
            md.medium_text AS medium,
            cl.classification_text AS classification,
            c.culture_text AS culture,
            d.date_text AS date_text
        FROM artworks aw
        JOIN museum m ON aw.museum_id = m.id
        JOIN titles t ON aw.title_id = t.id
        JOIN artists a2 ON aw.artist_id = a2.id
        JOIN mediums md ON aw.medium_id = md.id
        JOIN classifications cl ON aw.classification_id = cl.id
        JOIN cultures c ON aw.culture_id = c.id
        JOIN dates d ON aw.date_id = d.id;
    """

    df = pd.read_sql_query(query, conn)

    # Medium distribution
    medium_dist = (
        df.groupby(["museum_name", "medium"])
        .size()
        .reset_index(name="count")
    )

    # Culture distribution
    culture_dist = (
        df.groupby(["museum_name", "culture"])
        .size()
        .reset_index(name="count")
    )

    # Simple "century" extraction from date_text
    def extract_century(date_str):
        # crude but OK for this class: look for a 4-digit year and bucket it
        import re
        match = re.search(r"(\d{4})", date_str or "")
        if not match:
            return "Unknown"
        year = int(match.group(1))
        century = (year - 1) // 100 + 1
        return f"{century}th c."

    df["century"] = df["date_text"].apply(extract_century)

    century_dist = (
        df.groupby(["museum_name", "century"])
        .size()
        .reset_index(name="count")
    )

    return {
        "medium_dist": medium_dist,
        "culture_dist": culture_dist,
        "century_dist": century_dist,
        "full_df": df
    }

def visualize_results(metrics):
    medium_dist = metrics["medium_dist"]
    culture_dist = metrics["culture_dist"]
    century_dist = metrics["century_dist"]

    # 1. Medium Distribution Across Museums (bar chart)
    plt.figure(figsize=(10, 6))
    museums = medium_dist["museum_name"].unique()
    media = medium_dist["medium"].unique()

    # simple grouped bars
    x = range(len(media))
    width = 0.8 / max(len(museums), 1)  # width per museum

    for i, museum in enumerate(museums):
        subset = medium_dist[medium_dist["museum_name"] == museum]
        counts = []
        for m in media:
            row = subset[subset["medium"] == m]
            counts.append(int(row["count"].iloc[0]) if not row.empty else 0)
        offset = [xi + i * width for xi in x]
        plt.bar(offset, counts, width=width, label=museum)

    plt.xticks([xi + width * (len(museums) - 1) / 2 for xi in x], media, rotation=45, ha="right")
    plt.ylabel("Number of Artworks")
    plt.title("Medium Distribution Across Museums")
    plt.legend()
    plt.tight_layout()
    plt.savefig("medium_distribution.png")
    plt.show()
    plt.close()

    # 2. Cultural Regions of Art Across Museums (pie chart per museum)
    for museum in museums:
        subset = culture_dist[culture_dist["museum_name"] == museum]
        top = subset.sort_values("count", ascending=False).head(8)  # top 8 cultures
        labels = top["culture"]
        sizes = top["count"]

        plt.figure(figsize=(6, 6))
        plt.pie(sizes, labels=labels, autopct="%1.1f%%")
        plt.title(f"Cultural Origins for {museum}")
        plt.tight_layout()
        plt.savefig(f"culture_pie_{museum.replace(' ', '_')}.png")
        plt.show()
        plt.close()

    # 3. Artworks by Century (stacked bar chart)
    plt.figure(figsize=(10, 6))
    centuries = century_dist["century"].unique()
    centuries_sorted = sorted(centuries)  # rough ordering

    bottom = [0] * len(centuries_sorted)

    for museum in museums:
        subset = century_dist[century_dist["museum_name"] == museum]
        counts = []
        for c in centuries_sorted:
            row = subset[subset["century"] == c]
            counts.append(int(row["count"].iloc[0]) if not row.empty else 0)

        plt.bar(centuries_sorted, counts, bottom=bottom, label=museum)
        # update bottom for stacked bars
        bottom = [bottom[i] + counts[i] for i in range(len(counts))]

    plt.ylabel("Number of Artworks")
    plt.title("Artworks by Century Across Museums")
    plt.legend()
    plt.tight_layout()
    plt.savefig("century_stacked.png")
    plt.show()  
    plt.close()

def main():
    conn = sqlite3.connect("artmuseumV4.db")
    #conn = sqlite3.connect("artmuseumV3.db")
    #conn = sqlite3.connect("artmuseumV2.db")
    #conn = sqlite3.connect("artmuseum.db")
    cur = conn.cursor()

    create_tables(conn, cur)

    aic_lst = get_aic_data(1)
    #aic_lst.extend(get_aic_data(2))
    #aic_lst.extend(get_aic_data(3)) #getting 300 records from multiple pages of the aic db
    insert_aic_data(conn, cur, aic_lst)
    #print(f"the lenght of the data list is {len(aic_lst)}")

    cleveland_lst = get_cleveland_data()
    insert_cleveland_data(conn, cur, cleveland_lst)

    coop_lst = get_coop_data()
    insert_coop_data(conn, cur, coop_lst)

    harvard_lst = get_harvard_data()
    insert_harvard_data(conn, cur, harvard_lst)
    # Run metrics + visualizations

    metrics = select_and_calculate_metrics(conn)
    visualize_results(metrics)
    
    conn.close()


if __name__ == "__main__":
    main()
