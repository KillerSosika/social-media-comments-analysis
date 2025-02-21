import pandas as pd
from langdetect import detect, DetectorFactory
from sqlalchemy import create_engine, text


DB_NAME = "mediasocial"
DB_USER = "root"
DB_PASSWORD = "rootroot"
DB_HOST = "localhost"
DB_PORT = "3306"  

file_path = "test_sample.csv"
# Розмір порції
chunksize = 2000

# Список відомих платформ (якщо хочете фільтрувати)
KNOWN_PLATFORMS = [
    "instagram", "facebook", "youtube", "tiktok",
    "twitter", "telegram", "vkontakte", "reddit"
]


DetectorFactory.seed = 0


engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def insert_ignore(table, conn, keys, data_iter):
    """
    INSERT IGNORE INTO <table> (...) VALUES (...)
    щоб під час дубліката PRIMARY KEY / UNIQUE запис ігнорувався.
    """
    data = list(data_iter)
    if not data:
        return

    placeholders = [
        "(" + ", ".join(["%s"] * len(row)) + ")"
        for row in data
    ]
    insert_sql = f"INSERT IGNORE INTO {table.name} ({', '.join(keys)}) VALUES "
    insert_sql += ", ".join(placeholders)

    flattened = []
    for row in data:
        flattened.extend(row)

    raw_conn = conn.connection.driver_connection
    cursor = raw_conn.cursor()
    try:
        cursor.execute(insert_sql, flattened)
        raw_conn.commit()
    finally:
        cursor.close()


def detect_language_safe(txt):
    try:
        return detect(txt)
    except:
        return "unknown"


with engine.begin() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT AUTO_INCREMENT PRIMARY KEY,
        account_id VARCHAR(255) UNIQUE
    ) ENGINE=InnoDB;
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS platforms (
        platform_id INT AUTO_INCREMENT PRIMARY KEY,
        platform_name VARCHAR(255) UNIQUE
    ) ENGINE=InnoDB;
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS regions (
        region_id INT AUTO_INCREMENT PRIMARY KEY,
        language_code VARCHAR(16) UNIQUE
    ) ENGINE=InnoDB;
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS comments (
        comment_id VARCHAR(255) NOT NULL PRIMARY KEY,
        user_id INT,
        platform_id INT,
        region_id INT,
        created_time DATETIME,
        text TEXT,
        likes_count INT,
        comments_count INT,
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (platform_id) REFERENCES platforms(platform_id),
        FOREIGN KEY (region_id) REFERENCES regions(region_id)
    ) ENGINE=InnoDB;
    """))



csv_reader = pd.read_csv(
    file_path,
    chunksize=chunksize,
    engine="python",       
    on_bad_lines="skip",   
    encoding="utf-8",      
    sep=","                
)

for chunk in csv_reader:
    drop_cols = ["text_additional", "shares_count", "views_count"]
    existing_drop = [c for c in drop_cols if c in chunk.columns]
    if existing_drop:
        chunk.drop(columns=existing_drop, inplace=True)


    if "account_id" not in chunk.columns:
        chunk["account_id"] = "unknown_user"


    if "text_original" in chunk.columns:
        chunk["text_original"] = chunk["text_original"].astype(str).fillna("[EMPTY]")
    chunk.fillna(0, inplace=True)


    if "text_original" in chunk.columns:
        chunk["language"] = chunk["text_original"].apply(detect_language_safe)
    else:
        chunk["language"] = "unknown"


    if "platform" in chunk.columns:
        chunk["platform"] = chunk["platform"].astype(str).str.lower()
    else:
        chunk["platform"] = "unknown"

    chunk = chunk[chunk["platform"].isin(KNOWN_PLATFORMS)]
    if len(chunk) == 0:
        continue  # нічого вставляти

 
    users_df = pd.DataFrame(chunk["account_id"].unique(), columns=["account_id"])
    users_df.to_sql("users", engine, if_exists="append", index=False, method=insert_ignore)

    platforms_df = pd.DataFrame(chunk["platform"].unique(), columns=["platform_name"])
    platforms_df.to_sql("platforms", engine, if_exists="append", index=False, method=insert_ignore)

    regions_df = pd.DataFrame(chunk["language"].unique(), columns=["language_code"])
    regions_df.to_sql("regions", engine, if_exists="append", index=False, method=insert_ignore)


    with engine.begin() as conn:
        user_rows = conn.execute(text("SELECT user_id, account_id FROM users")).fetchall()
        platform_rows = conn.execute(text("SELECT platform_id, platform_name FROM platforms")).fetchall()
        region_rows = conn.execute(text("SELECT region_id, language_code FROM regions")).fetchall()

    user_map = {row.account_id: row.user_id for row in user_rows}
    platform_map = {row.platform_name: row.platform_id for row in platform_rows}
    region_map = {row.language_code: row.region_id for row in region_rows}

    rename_map = {
        "id": "comment_id",
        "created_time": "created_time",
        "text_original": "text",
        "likes_count": "likes_count",
        "comments_count": "comments_count",
        "account_id": "account_id",
        "platform": "platform",
        "language": "language"
    }
    existing_rename_map = {old: new for old, new in rename_map.items() if old in chunk.columns}
    comments_df = chunk.rename(columns=existing_rename_map)

    needed = ["comment_id", "account_id", "platform", "language", "created_time", "text", "likes_count", "comments_count"]
    for col in needed:
        if col not in comments_df.columns:
            comments_df[col] = None

    comments_df["user_id"] = comments_df["account_id"].map(user_map)
    comments_df["platform_id"] = comments_df["platform"].map(platform_map)
    comments_df["region_id"] = comments_df["language"].map(region_map)

    comments_df.drop(columns=["account_id", "platform", "language"], inplace=True)

    
    comments_df.to_sql("comments", engine, if_exists="append", index=False, method=insert_ignore)

    print(f"Оброблено та завантажено {len(chunk)} рядків із CSV")


print("Уся обробка CSV успішно завершена!")
