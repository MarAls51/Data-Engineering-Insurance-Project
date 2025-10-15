import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("REDSHIFT_HOST")
PORT = os.getenv("REDSHIFT_PORT")
USER = os.getenv("REDSHIFT_USER")
PASSWORD = os.getenv("REDSHIFT_PASSWORD")
DB_NAME = "postgres" 

try:
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'data'
    ORDER BY table_name;
    """
    
    cursor.execute(query)
    tables = cursor.fetchall()

    print("\n✅ TABLES FOUND IN 'data' SCHEMA:")
    if tables:
        for table in tables:
            print(f"- {table[0]}")
    else:
        print("No tables found.")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"\n❌ FAILED TO LIST TABLES. Error: {e}")