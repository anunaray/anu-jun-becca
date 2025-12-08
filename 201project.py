
import time
import requests
import json
import sqlite3
import matplotlib.pyplot as plt
import re

### DATABASE SETUP ###

def create_tables(conn, cur):
    #  lookup tables

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

    # main Artworks Table (INT ONLY)

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

def insert_museum (conn, cur, museum_name): #function to insert museum names only.
    cur.execute("INSERT OR IGNORE INTO museum (name) VALUES (?);", (museum_name,))
    conn.commit()
    cur.execute("SELECT id FROM museum WHERE name = ?;", (museum_name,))
    museum_id = cur.fetchone()[0]
    return museum_id

### AIC API DATA RETRIEVAL AND INSERTION ###

def get_aic_data(page):
    print("Requesting Art Institute of Chicago artwork data...")
    all_url = f"https://api.artic.edu/api/v1/artworks?page={page}&limit=100" #so that the next function can iterate through pages
    response = requests.get(all_url)
    data = response.json()
    return data['data']

def get_or_create_id(cur, conn, table, column, value, fallback):
    """
    Safely gets or creates a lookup-table ID.
    Never allows NULL or empty strings into lookup tables.
    """
    if value is None or str(value).strip() == "":
        value = fallback #ensures NULL values dont accumulate in the supplementary tables. This was an issue we had in previous versions of the db

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

def insert_aic_data(conn, cur, pages, limit=25): 
    """
    Insert Art Institute of Chicago artworks into the database.
    """
    print("Inserting Art Institute of Chicago artwork data into database...")
    museum_name = "Art Institute of Chicago"
    museum_id = insert_museum(conn, cur, museum_name)

    count = 0
    for i in range(pages): #loop through pages that. only inserts what has not already been inserted
        data_dict_list = get_aic_data(i)
        if count >= limit:
                break  # stop after 25 inserts - outer loop
        '''subsections to get:
        title
        place_of_origin
        artist_title
        medium_display
        classification_title
        date_display
        '''
        for item in data_dict_list:
            if count >= limit:
                break  # stop after 25 inserts - inner loop

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
            title_id = get_or_create_id(cur, conn, "titles", "title_text", title, "Untitled" ) #"untitled" etc are fallbacks so that null values dont accumulate
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

### MET MUSEUM API DATA RETRIEVAL AND INSERTION ###

def get_met_start_index(cur): #this finds how many met museum artworks are already in the db so the next functions know where to start iterating from
    cur.execute("""
        SELECT COUNT(*) FROM artworks aw
        JOIN museum m ON aw.museum_id = m.id
        WHERE m.name = 'Metropolitan Museum of Art';
    """)
    return cur.fetchone()[0] #use this number as the start index

def get_met_data(start_index=0, batch_size=25): #default start index is zero, batch size is how many we want to fetch

    #limiting how much we fetch because of MET museum rate limitation. this is not the case with the other APIS
    #playing it safe by only fetching what we need

    objects_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects" #all obj ids - met is structured differently from the other apis
    object_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects/{}" #to fetch 1 obj at a time

    try:
        all_ids = requests.get(objects_url, timeout=10).json()["objectIDs"]
    except Exception as e:
        print("Failed to fetch MET object IDs:", e)
        return [] #this was because in previous versions, we ran into many issues of rate limitation with the MET api.
                 # just to keep things running smoothly

    results = [] # stores all fetched data. empty at first
    idx = start_index

    while len(results) < batch_size and idx < len(all_ids):  
        object_id = all_ids[idx]
        idx += 1 #loop continues only if we havent collected 25 and we havent run out of object ids in the met database

        try:
            resp = requests.get(object_url.format(object_id), timeout=10) 
            data = resp.json()
        except Exception:
            continue #try to fetch one, but if it fails continue the loop because MET has a lot of broken records

        if not data.get("title"):
            continue #skip data with no title

        normalized = {
            "id": data.get("objectID"),
            "title": data.get("title"),
            "artist_title": data.get("artistDisplayName"),
            "medium_display": data.get("medium"),
            "classification_title": data.get("classification"),
            "place_of_origin": data.get("culture"),
            "date_display": data.get("objectDate")
        } # only fetching the data we need

        results.append(normalized)
        time.sleep(0.1)

    print(f"Fetched {len(results)} artworks starting at index {start_index} from the MET Museum API")
    return results

def insert_met_data(conn, cur, data_dict_list, limit=25):
    
    print("Inserting MET artwork data into database...")

    museum_name = "Metropolitan Museum of Art"
    museum_id = insert_museum(conn, cur, museum_name)

    count = 0 
    for item in data_dict_list:
        if count >= limit:
            break

        original_id = item.get("id")

        cur.execute(
            "SELECT 1 FROM artworks WHERE original_id = ? AND museum_id = ?",
            (original_id, museum_id)
        )
        if cur.fetchone():
            continue

        title_id = get_or_create_id(cur, conn, "titles", "title_text", item["title"], "Untitled")
        artist_id = get_or_create_id(cur, conn, "artists", "artist_name", item["artist_title"], "Unknown Artist")
        medium_id = get_or_create_id(cur, conn, "mediums", "medium_text", item["medium_display"], "Unknown Medium")
        classification_id = get_or_create_id(cur, conn, "classifications", "classification_text", item["classification_title"], "Unclassified")
        culture_id = get_or_create_id(cur, conn, "cultures", "culture_text", item["place_of_origin"], "Unknown Culture")
        date_id = get_or_create_id(cur, conn, "dates", "date_text", item["date_display"], "Unknown Date")

        cur.execute("""
            INSERT OR IGNORE INTO artworks
            (original_id, museum_id, title_id, artist_id, medium_id, classification_id, culture_id, date_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            original_id, museum_id, title_id, artist_id,
            medium_id, classification_id, culture_id, date_id
        ))

        count += 1

    conn.commit()
    print(f"MET Museum of Art insertion complete - INSERTED {count} ARTWORKS")


### HARVARD ART MUSEUM API DATA RETRIEVAL AND INSERTION ###


def get_harvard_data(target_count=600):
   #harvard api just fetches the same number of data everytime
   #the 25 count limitation is implemented in the insert function, not the get function
   #the function pulls the same 600 every time, but only inserts 25 at a time while avoiding duplicates


   api_key = "58f23874-0244-4cb8-b162-ae364e69d3e0" #harvard uses an api key


   base_url = "https://api.harvardartmuseums.org/object"
   params = {
       "apikey": api_key,
       "size": min(target_count, 25),
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
           break # we have not had any errors yet, but just in case


       records = data.get("records", [])
       if not records:
           print("No more Harvard records returned.")
           break #this is if all harvard records are inserted into the db and there are none left


       for rec in records:
           # get artist name from "people" list if available
           if rec.get("people"):
               artist_name = rec["people"][0].get("name")
           else:
               artist_name = None


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


       # pagination handling
       info = data.get("info", {})
       current_page = info.get("page")
       total_pages = info.get("pages")

       if not current_page or not total_pages or current_page >= total_pages:
           break

       params["page"] = current_page + 1 #looping through multiple pages till we hit target count
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
    print(f"Harvard Art Museum data insertion complete - INSERTED {count} ARTWORKS")

### CLEVELAND MUSEUM API DATA RETRIEVAL AND INSERTION ###

def get_cleveland_data():
    print("Requesting Cleveland Museum of Art artwork data...")
    url = "https://openaccess-api.clevelandart.org/api/artworks/"
    response = requests.get(url)
    data = response.json()
    
    return data['data']

def insert_cleveland_data(conn, cur, data_dict_list, limit=25):
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

### CALCULATIONS ###

def load_artworks_raw(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            m.name,
            cl.classification_text,
            c.culture_text,
            a2.artist_name,
            d.date_text
        FROM artworks aw
        JOIN museum m ON aw.museum_id = m.id
        JOIN classifications cl ON aw.classification_id = cl.id
        JOIN cultures c ON aw.culture_id = c.id
        JOIN artists a2 ON aw.artist_id = a2.id
        JOIN dates d ON aw.date_id = d.id;
    """)
    return cur.fetchall() #each row is returned as a tuple

def normalize(text):
    if text is None:
        return "Unknown"
    text = text.strip()
    return text.title() if text else "Unknown"

def calculate_culture_distribution(rows, top_n=8):
    data = {}

    for museum, _, culture, _, _ in rows: # ignoring values we dont need
        museum = normalize(museum)
        culture = normalize(culture)

        if museum not in data:
            data[museum] = {}
        
        if culture not in data[museum]:
            data[museum][culture] = 0
        data[museum][culture] += 1
    
    # keep only top N per museum

    result = {}
    for museum, culture_counts, in data.items():
        sorted_items = sorted(
            culture_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )
        result[museum] = sorted_items[:top_n]
    
    return result

def calculate_top_artists(rows, top_n=8):
    artist_counts = {}

    for _, _, _, artist, _ in rows:
        a = normalize(artist)
        artist_counts[a] = artist_counts.get(a, 0) + 1

    return sorted(
        artist_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:top_n]

def calculate_top_classifications(rows, top_n=8):
    total_counts = {}

    for _, classification, _, _, _ in rows:
        c= normalize(classification)
        total_counts[c] = total_counts.get(c, 0) +1

    top_classes = sorted(
        total_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:top_n]

    return top_classes

def calculate_century_distribution(rows):
    #returns a nested dict of museum: {century_label: count}}

    century_data = {}

    for museum, _, _, _, date_text in rows:
        museum = normalize(museum)
        date_text = normalize(date_text)

        # get a 4-digit year from the string
        match = re.search(r'(\d{4})', date_text) # using regex to extract just the year
        if match:
            year = int(match.group(1))
            century = (year - 1) // 100 + 1
            century_label = f"{century}th c." # figure out the century from the year
        else:
            century_label = "Unknown"

        if museum not in century_data:
            century_data[museum] = {}

        if century_label not in century_data[museum]:
            century_data[museum][century_label] = 0

        century_data[museum][century_label] += 1

    return century_data

### VISUALIZATIONS ###

def plot_culture_pies(culture_data): # pie chart
    for museum, data in culture_data.items():
        labels = [x[0] for x in data]
        sizes = [x[1] for x in data]

        plt.figure(figsize=(6,6))
        plt.pie(sizes, labels=labels, autopct="%1.1f%%")
        plt.title(f"Cultural Origins of Artworks - {museum}")
        plt.tight_layout()
        plt.savefig(f"culturepie_{museum.replace(' ', '_')}.png")
        plt.show()
        plt.close()

def plot_top_artists(top_artists):
    names = [x[0] for x in top_artists]
    counts = [x[1] for x in top_artists]

    plt.figure(figsize=(10,6))
    plt.barh(names[::-1], counts[::-1])
    plt.xlabel("Number of Artworks")
    plt.title("Top Artists Across All Museums")
    plt.tight_layout()
    plt.savefig(f"top_artists.png")
    plt.show()
    plt.close()

def plot_top_classifications(top_classes):
    
    names = [x[0] for x in top_classes]
    counts = [x[1] for x in top_classes]

    plt.figure(figsize=(10,6))
    plt.barh(names[::-1], counts[::-1])
    plt.xlabel("Number of Artworks")
    plt.title("Top Classifications Across All Museums")
    plt.tight_layout()
    plt.savefig("top_classifications.png")
    plt.show()
    plt.close()

def plot_century_stacked_bar(century_data):
    # get all unique centuries across all museums for consistent x-axis
    all_centuries = set()
    for counts in century_data.values():
        all_centuries.update(counts.keys())
    all_centuries = sorted(all_centuries)  # rough chronological order

    museums = list(century_data.keys())
    bottom = [0] * len(all_centuries)

    plt.figure(figsize=(12, 6))

    for museum in museums:
        counts = [century_data[museum].get(c, 0) for c in all_centuries]
        plt.bar(all_centuries, counts, bottom=bottom, label=museum)
        bottom = [bottom[i] + counts[i] for i in range(len(counts))]

    plt.xlabel("Century")
    plt.ylabel("Number of Artworks")
    plt.title("Artworks by Century Across Museums")
    plt.legend()
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig("century_stacked.png")
    plt.show()
    plt.close()

def write_metrics_to_txt(culture_data, top_artists, top_classes, filename="art_metrics.txt"):
    with open(filename, "w") as f:
        f.write("=== Top Cultures per Museum ===\n")
        for museum, data in culture_data.items():
            f.write(f"{museum}:\n")
            for culture, count in data:
                f.write(f"  {culture}: {count}\n")
        f.write("\n=== Top Artists ===\n")
        for artist, count in top_artists:
            f.write(f"{artist}: {count}\n")
        f.write("\n=== Top Classifications ===\n")
        for cls, count in top_classes:
            f.write(f"{cls}: {count}\n")


def main_visualizations(conn):
    rows = load_artworks_raw(conn)

    culture_data = calculate_culture_distribution(rows)
    top_artists = calculate_top_artists(rows)
    top_classes = calculate_top_classifications(rows)
    century_stacked = calculate_century_distribution(rows)

    plot_culture_pies(culture_data)
    plot_top_artists(top_artists)
    plot_top_classifications(top_classes)
    plot_century_stacked_bar(century_stacked)
    write_metrics_to_txt(culture_data, top_artists, top_classes, century_stacked)


def main():
    conn = sqlite3.connect("artmuseumV5.db")
    #conn = sqlite3.connect("artmuseumV4.db")
    #conn = sqlite3.connect("artmuseumV3.db")
    #conn = sqlite3.connect("artmuseumV2.db")
    #conn = sqlite3.connect("artmuseum.db")
    cur = conn.cursor()
    
    create_tables(conn, cur)

    #moved get_aic_data to within insert_aic_data to loop through pages
    '''insert_aic_data(conn, cur, 6)

    cleveland_lst = get_cleveland_data()
    insert_cleveland_data(conn, cur, cleveland_lst)

    harvard_lst = get_harvard_data()
    insert_harvard_data(conn, cur, harvard_lst)

    start_index = get_met_start_index(cur)
    met_batch = get_met_data(start_index=start_index)
    insert_met_data(conn, cur, met_batch)
   
    '''
    main_visualizations(conn)

    conn.close()

    
if __name__ == "__main__":
    main()