import json
import mysql.connector
from pydantic import BaseModel
from datetime import datetime

conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",      # 🔁 change if your password is different
            database="airbnb_db"     # 🔁 make sure this database exists
        )
class Airbnb(BaseModel):
    Listing_id: str
    Name: str
    picture_url: str
    property_type: str
    house_rules:dict
    totalReviews: int
    categoryRating: list[dict[str, str | float]]
    roomdetails: dict[str, list[str] | str]
    AmenitiesGroup: list[dict[str, object]]  # <-- flexible dict
    description: str
    hostdata: dict[str, str | list | dict]
    media_image: list
def input_data(raw_data):
    with open(raw_data, "rb") as f:
        data = json.loads(f.read().decode())
    return data
def proceess_data(data):
    s_dict = {}
    base_path = data.get("niobeClientData", [])[0][1]
    section_path = base_path.get("data", {}).get("presentation", {}).get("stayProductDetailPage", {}).get("sections", {})
    sections_path = section_path.get("sections", [])
    sbui = section_path.get("sbuiData", {})
    root_sections = sbui.get("sectionConfiguration", {}).get("root", {}).get("sections", [])

    room_data = {}
    if len(root_sections) > 1:
        room_data = root_sections[1].get("sectionData", {})
    for i in sections_path:
        section_path = i.get("section")
        if not isinstance(section_path, dict):
            continue
        section = section_path.get("shareSave")
        if not isinstance(section, dict):
            continue
        b = section["embedData"]
        s_dict["Listing_id"] = b.get("id")
        s_dict["Name"] = b.get("name")
        s_dict["picture_url"] = b.get("pictureUrl")
        s_dict["property_type"] = b.get("propertyType")
        s_dict["totalReviews"] = b.get("reviewCount")
        house_rules = {"checkIn": None, "checkout": None, "title": None}
        for sec in sections_path:
            section_data = sec.get("section")
            if not isinstance(section_data, dict):
                continue
            hr = section_data.get("houseRules")
            if isinstance(hr, list) and len(hr) > 0:
                for idx, item in enumerate(hr):
                    if not isinstance(item, dict):
                        continue
                    title = item.get("title")
                    if title:
                        title = title.replace("\u202f", " ").replace("\u00a0", " ")
                    if idx == 0:
                        house_rules["checkIn"] = title
                    elif idx == 1:
                        house_rules["checkout"] = title
                    elif idx == 2:
                        house_rules["title"] = title
                break
        s_dict["house_rules"] = house_rules
        for sec in sections_path:
            rating = sec.get("section")
            if not isinstance(rating, dict):
                continue
            rate = rating.get("ratings")
            if isinstance(rate, list):
                ratings = []
                for item in rate:
                     if isinstance(item, dict):
                         ratings.append({
                             "categoryType": item.get("label") or "",
                             "categoryRating": item.get("localizedRating") or 0.0,
                         })
                s_dict["categoryRating"] = ratings
# Image Url Process
        images = section_path["mediaItems"]
        img_list = []
        for img in images:
            if isinstance(img, dict):
                img_list.append(img.get("baseUrl"))

        s_dict["media_image"] = img_list
        break
# Room data process
    overview_titles = []
    overview_items = room_data.get("overviewItems", [])
    if isinstance(overview_items, list):
        for item in overview_items:
            if isinstance(item, dict):
                overview_titles.append(item.get("title"))
    s_dict["roomdetails"] = {
        # "roomtitle": room_data.get("title"),
        "city":room_data.get("title")[8:12],
        "country":room_data.get("title")[19:24],
        "overviewitems": overview_titles
    }
    s_dict["AmenitiesGroup"] = []
    for i in sections_path:
        s = i.get("section")
        if not isinstance(s, dict):
            continue
        ser = s.get("seeAllAmenitiesGroups")
        if ser is None:
            continue
        if isinstance(ser, list):
            for se in ser:
                if isinstance(se, dict):
                    group_title = se.get("title")
                    amenities_list = []
                    for a in se.get("amenities", []):
                        if isinstance(a, dict):
                            amenities_list.append(a.get("title"))

                    s_dict["AmenitiesGroup"].append({
                        "service_title": group_title,
                        "amenities": amenities_list
                    })

#Description process
    for sec in sections_path:
        section_data = sec.get("section")
        if isinstance(section_data, dict):
            html_desc = section_data.get("htmlDescription")
            if isinstance(html_desc, dict):
                s_dict["description"] = html_desc.get("htmlText")
# Host Data Process
    for h in sections_path:
        section = h.get("section")
        if not isinstance(section, dict):
            continue
        card = section.get("cardData")
        cards =section.get("cohosts")
        if isinstance(card, dict):
            s_dict["hostdata"]={
            "hostname" : card["name"],
            "user_id": card["userId"],
            "host_reviews" :card["stats"][0]["value"],
            "host_rating" :card["stats"][1]["value"],
            "year_hosting" :card["stats"][2]["value"],
            "co-host":[]
            }
            for co_host in cards:
                if isinstance(co_host, dict):
                    co_host_data = {
                        "co_host_name": co_host.get("name"),
                        "co_host_user_id": co_host.get("userId"),
                    }
                    s_dict["hostdata"]["co-host"].append(co_host_data)

    validated_model = Airbnb.model_validate(s_dict)
    return validated_model
def write_file(file):
    file_name = datetime.now().strftime("%Y-%m-%d")
    with open(f"Air_bnb_{file_name}.json", "w") as f:
        json.dump(file.model_dump(), f, indent=4)




def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(50) UNIQUE,
        name VARCHAR(255),
        picture_url TEXT,
        property_type VARCHAR(150),
        city VARCHAR(100),
        country VARCHAR(100),
        description LONGTEXT,
        total_reviews INT,
        checkin_time VARCHAR(100),
        checkout_time VARCHAR(100),
        guest_limit VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS hosts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(50),
        hostname VARCHAR(150),
        user_id VARCHAR(255),
        host_reviews INT,
        host_rating DECIMAL(3,2),
        year_hosting INT,
        FOREIGN KEY (listing_id) REFERENCES listings(listing_id) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS category_ratings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(50),
        category_type VARCHAR(100),
        category_rating DECIMAL(2,1),
        FOREIGN KEY (listing_id) REFERENCES listings(listing_id) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS amenities (
        id INT AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(50),
        service_title VARCHAR(150),
        amenity_name VARCHAR(150),
        FOREIGN KEY (listing_id) REFERENCES listings(listing_id) ON DELETE CASCADE
    )
    """)
       # 5️⃣ Media Images Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS media_images (
        id INT AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(50),
        image_url TEXT,
        FOREIGN KEY (listing_id) REFERENCES listings(listing_id) ON DELETE CASCADE
    )
    """)

    # 6️⃣ Co-Hosts Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS co_hosts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(50),
        co_host_name VARCHAR(150),
        co_host_user_id VARCHAR(255),
        FOREIGN KEY (listing_id) REFERENCES listings(listing_id) ON DELETE CASCADE
    )
    """)

    conn.commit()
    cursor.close()
def insert_listing_data(conn, data):
    cursor = conn.cursor()

    # -------------------------
    # 1️⃣ Insert into listings
    # -------------------------
    cursor.execute("""
        INSERT IGNORE INTO listings
        (listing_id, name, picture_url, property_type,
         description, total_reviews, checkin_time, checkout_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data.get("Listing_id"),
        data.get("Name"),
        data.get("picture_url"),
        data.get("property_type"),
        data.get("description"),
        data.get("totalReviews"),
        data.get("house_rules", {}).get("checkIn"),
        data.get("house_rules", {}).get("checkout")
    ))

    # -------------------------
    # 2️⃣ Insert into hosts
    # -------------------------
    host = data.get("hostdata", {})

    if host:
        cursor.execute("""
            INSERT IGNORE INTO hosts
            (listing_id, hostname, user_id,
             host_reviews, host_rating, year_hosting)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data.get("Listing_id"),
            host.get("hostname"),
            host.get("user_id"),
            host.get("host_reviews"),
            host.get("host_rating"),
            host.get("year_hosting")
        ))

    # -------------------------
    # 3️⃣ Insert into category_ratings
    # -------------------------
    for rating in data.get("categoryRating", []):
        cursor.execute("""
            INSERT INTO category_ratings
            (listing_id, category_type, category_rating)
            VALUES (%s, %s, %s)
        """, (
            data.get("Listing_id"),
            rating.get("categoryType"),
            rating.get("categoryRating")
        ))

    # -------------------------
    # 4️⃣ Insert into amenities
    # -------------------------
    for group in data.get("AmenitiesGroup", []):
        service_title = group.get("service_title")

        for amenity in group.get("amenities", []):
            cursor.execute("""
                INSERT INTO amenities
                (listing_id, service_title, amenity_name)
                VALUES (%s, %s, %s)
            """, (
                data.get("Listing_id"),
                service_title,
                amenity
            ))
        # -------------------------
    # 5️⃣ Insert into media_images
    # -------------------------
    for image in data.get("media_image", []):
        cursor.execute("""
            INSERT INTO media_images
            (listing_id, image_url)
            VALUES (%s, %s)
        """, (
            data.get("Listing_id"),
            image
        ))
    
        # -------------------------
    # 6️⃣ Insert into co_hosts
    # -------------------------
    host = data.get("hostdata", {})
    for co_host in host.get("co-host", []):
        cursor.execute("""
            INSERT INTO co_hosts
            (listing_id, co_host_name, co_host_user_id)
            VALUES (%s, %s, %s)
        """, (
            data.get("Listing_id"),
            co_host.get("co_host_name"),
            co_host.get("co_host_user_id")
        ))

    conn.commit()
    cursor.close()


user_file_input = r"C:\Users\vishal.mistry\Desktop\Mistry Vishal\airbnb\air_bnb.json"

s = input_data(user_file_input)

processed_data = proceess_data(s)

write_file(processed_data)

create_tables(conn)

insert_listing_data(conn, processed_data.model_dump())

conn.close()