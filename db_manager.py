"""
Interact with the DailyVideoData table
"""

import os
from datetime import datetime
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool
from threading import Lock
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

class DatabasePool:
    _instance = None
    _lock = Lock()
    _pool = None

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabasePool, cls).__new__(cls)
                cls._instance._initialize_pool()
            return cls._instance

    def _initialize_pool(self):
        """Initialize the connection pool"""
        if self._pool is None:
            DATABASE_URL = os.getenv('DATABASE_URL')
            # Maintain minimum 1 connection, maximum 20 connections
            self._pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=20,
                dsn=DATABASE_URL
            )

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
        finally:
            if conn:
                self._pool.putconn(conn)

    def close_all(self):
        """Close all connections in the pool"""
        if self._pool:
            self._pool.closeall()

# Database operations class
class DailyVideoDataDB:
    def __init__(self):
        self.db_pool = DatabasePool()

    def ensure_table_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS DailyVideoData (
            id SERIAL PRIMARY KEY,
            post_url TEXT,
            creator_username TEXT,
            marketing_associate TEXT,
            app TEXT,
            view_count INTEGER,
            comment_count INTEGER,
            caption TEXT,
            create_time TIMESTAMP,
            log_time TIMESTAMP
        );
        """
        with self.db_pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
            conn.commit()

    def insert_row(self, url, username, associate, app, view_count, comment_count, caption, created_at, insert_time):
        """
        Insert a new video record
        Returns: The ID of the inserted record
        """
        query = """
        INSERT INTO DailyVideoData 
        (post_url, creator_username, marketing_associate, app, view_count, comment_count, caption, create_time, log_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        
        with self.db_pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (
                    url,
                    username,
                    associate,
                    app,
                    view_count,
                    comment_count,
                    caption,
                    created_at,
                    insert_time
                ))
                view_id = cur.fetchone()[0]
                conn.commit()
                return view_id