import re

def normalize(text):
    if text is None:
        return "Unknown"
    text = text.strip()
    return text.title() if text else "Unknown"

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
    return cur.fetchall()


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