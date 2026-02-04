"""
Database initialization script for asset_us.
Creates the database and all required tables.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pymysql
from config.settings import Settings


def init_database():
    """Initialize the asset_us database and tables."""
    settings = Settings()

    # Try to connect to existing database first
    try:
        conn = pymysql.connect(
            host=settings.DB_HOST,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            database=settings.DB_NAME,
            charset="utf8mb4",
        )
        print(f"Connected to existing database '{settings.DB_NAME}'.")
    except pymysql.err.OperationalError as e:
        if e.args[0] == 1049:  # Unknown database
            print(f"Database '{settings.DB_NAME}' does not exist.")
            print("Please create it manually with:")
            print(f"  CREATE DATABASE {settings.DB_NAME} DEFAULT CHARACTER SET utf8mb4;")
            return
        raise

    try:
        with conn.cursor() as cur:

            # Use database
            cur.execute(f"USE {settings.DB_NAME}")

            # Read and execute all schema files
            schema_dir = Path(__file__).resolve().parent.parent / "db"
            schema_files = [
                "schema.sql",
                "schema_market_index.sql",
                "schema_daily_snapshot.sql",
            ]

            for schema_file in schema_files:
                schema_path = schema_dir / schema_file
                if not schema_path.exists():
                    print(f"  Skipping {schema_file} (not found)")
                    continue

                print(f"  Executing {schema_file}...")
                with open(schema_path, "r", encoding="utf-8") as f:
                    schema_sql = f.read()

                # Split by semicolon and execute each statement
                statements = [s.strip() for s in schema_sql.split(";") if s.strip()]

                for statement in statements:
                    # Skip USE and CREATE DATABASE statements (already handled)
                    if statement.upper().startswith("CREATE DATABASE"):
                        continue
                    if statement.upper().startswith("USE "):
                        continue

                    try:
                        cur.execute(statement)
                    except pymysql.err.OperationalError as e:
                        # Ignore "table already exists" errors
                        if e.args[0] != 1050:
                            raise

            conn.commit()
            print("All tables created successfully.")

            # Show created tables
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
            print(f"\nTables in {settings.DB_NAME}:")
            for table in tables:
                print(f"  - {table[0]}")

    finally:
        conn.close()


if __name__ == "__main__":
    init_database()
