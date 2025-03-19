import sqlite3
import datetime
import requests
from bs4 import BeautifulSoup
import json

DB_PATH = "scrape_data.db"
BASE_URL = "https://www.tlamagames.com"

def create_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS product (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp TEXT,
            name TEXT,
            availability TEXT,
            price TEXT,
            link TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS schedule_config (
            id INTEGER PRIMARY KEY,
            frequency TEXT,
            days TEXT,
            time_of_day TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS comparison_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts1 TEXT,
        ts2 TEXT,
        new_count INTEGER,
        removed_count INTEGER,
        updated_count INTEGER,
        new_items TEXT,
        removed_items TEXT,
        updated_items TEXT,
        log_time TEXT
    )
    ''')
    conn.commit()
    conn.close()

def insert_products(products):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    timestamp = datetime.datetime.now().isoformat()
    for prod in products:
        c.execute("""
            INSERT INTO product (run_timestamp, name, availability, price, link)
            VALUES (?, ?, ?, ?, ?)
        """, (timestamp, prod["name"], prod["availability"], prod["price"], prod["link"]))
    
    conn.commit()
    conn.close()
    return timestamp

def clean_old_data():
    """
    Keeps only rows from the 10 latest unique timestamps.
    Assumes the table 'product' has a column 'run_timestamp'.
    """
    conn = sqlite3.connect("scrape_data.db")
    cursor = conn.cursor()
    
    # Retrieve the 10 latest unique run_timestamps.
    cursor.execute("""
        SELECT DISTINCT run_timestamp
        FROM product
        ORDER BY run_timestamp DESC
        LIMIT 10
    """)
    latest_timestamps = [row[0] for row in cursor.fetchall()]
    
    if not latest_timestamps:
        conn.close()
        return

    placeholders = ','.join(['?'] * len(latest_timestamps))
    sql = f"DELETE FROM product WHERE run_timestamp NOT IN ({placeholders})"
    cursor.execute(sql, latest_timestamps)
    conn.commit()
    conn.close()

def get_all_run_timestamps():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT run_timestamp FROM product ORDER BY run_timestamp")
    rows = c.fetchall()
    conn.close()
    return [row[0] for row in rows]

def get_products_by_timestamp(timestamp):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name, availability, price, link FROM product WHERE run_timestamp = ?", (timestamp,))
    rows = c.fetchall()
    conn.close()
    products = {}
    for row in rows:
        name, availability, price, link = row
        products[name] = {"availability": availability, "price": price, "link": link}
    return products

def get_schedule_config():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT frequency, days, time_of_day FROM schedule_config WHERE id=1")
    row = c.fetchone()
    conn.close()
    if row:
        return {"frequency": row[0], "days": row[1], "time_of_day": row[2]}
    else:
        return None

def update_schedule_config(frequency, days, time_of_day):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("REPLACE INTO schedule_config (id, frequency, days, time_of_day) VALUES (1, ?, ?, ?)",
              (frequency, days, time_of_day))
    conn.commit()
    conn.close()

def fetch_data(page_url):
    response = requests.get(page_url)
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    product_divs = soup.select("div#products div.product")
    products = []
    for product in product_divs:
        name_tag = product.select_one("span[data-testid='productCardName']")
        name = name_tag.get_text(strip=True) if name_tag else "No Name"
        avail_tag = product.select_one("div.availability span")
        availability = avail_tag.get_text(strip=True) if avail_tag else "Unknown"
        price_tag = product.select_one("div.price.price-final strong")
        price = price_tag.get_text(strip=True) if price_tag else "No Price"
        link_tag = product.select_one("a.image")
        link = BASE_URL + link_tag["href"] if link_tag and link_tag.has_attr("href") else ""
        products.append({
            "name": name,
            "availability": availability,
            "price": price,
            "link": link
        })
    return products

def get_total_pages(page_url):
    response = requests.get(page_url)
    if response.status_code != 200:
        return 1
    soup = BeautifulSoup(response.text, "html.parser")
    pagination = soup.select_one("div.pagination")
    if pagination:
        last_page_link = pagination.select_one("a[data-testid='linkLastPage']")
        if last_page_link:
            try:
                return int(last_page_link.get_text(strip=True))
            except ValueError:
                pass
    return 1

def fetch_all_data():
    all_products = []
    first_page_url = BASE_URL + "/poskozene-rozbalene/"
    total_pages = get_total_pages(first_page_url)
    for page in range(1, total_pages + 1):
        page_url = first_page_url if page == 1 else BASE_URL + f"/poskozene-rozbalene/strana-{page}/"
        products = fetch_data(page_url)
        all_products.extend(products)
    return all_products

def compare_runs(ts1, ts2):
    run1 = get_products_by_timestamp(ts1)
    run2 = get_products_by_timestamp(ts2)
    names1 = set(run1.keys())
    names2 = set(run2.keys())
    new_products = {name: run2[name] for name in names2 - names1}
    removed_products = {name: run1[name] for name in names1 - names2}
    updated_products = {}
    for name in names1 & names2:
        diff = {}
        if run1[name]["availability"] != run2[name]["availability"]:
            diff["availability"] = f"{run1[name]["availability"]} -> {run2[name]["availability"]}"
        else:
            diff["availability"] = run2[name]["availability"]
        if run1[name]["price"] != run2[name]["price"]:
            diff["price"] = f"{run1[name]["availability"]} -> {run2[name]["availability"]}"
        else:
            diff["price"] = run2[name]["price"]
        if diff:
            diff["link"] = run2[name]["link"]
            updated_products[name] = diff
    return new_products, removed_products, updated_products


def log_comparison_to_db():
    """Log the comparison results into the comparison_log table."""

    ts1, ts2 = get_latest_timestamps()
    new_products, removed_products, updated_products = compare_runs(ts1, ts2)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    log_time = datetime.datetime.now().isoformat()
    cursor.execute("""
        INSERT INTO comparison_log 
        (ts1, ts2, new_count, removed_count, updated_count, new_items, removed_items, updated_items, log_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ts1,
        ts2,
        len(new_products),
        len(removed_products),
        len(updated_products),
        json.dumps(new_products),
        json.dumps(removed_products),
        json.dumps(updated_products),
        log_time
    ))
    conn.commit()
    conn.close()


def get_latest_timestamps():
    """Fetch the two most recent timestamps from the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT run_timestamp FROM product 
        ORDER BY run_timestamp DESC 
        LIMIT 2
    """)
    timestamps = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    # Ensure there are at least two timestamps to compare
    if len(timestamps) < 2:
        return timestamps[0] if timestamps else None, timestamps[0] if timestamps else None
    return timestamps[1], timestamps[0] 