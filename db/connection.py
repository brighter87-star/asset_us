import pymysql
from config.settings import Settings


def get_connection(database=None):
    """
    Get a database connection to the asset_us database or specified database.

    Args:
        database: Optional database name. If None, uses Settings().DB_NAME

    Returns:
        pymysql.connections.Connection
    """
    settings = Settings()
    db_name = database if database is not None else settings.DB_NAME

    return pymysql.connect(
        host=settings.DB_HOST,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=db_name,
        charset="utf8mb4",
        autocommit=False,
    )
