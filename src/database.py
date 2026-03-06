"""
Database connection module
Handles PostgreSQL connection and initialization
"""

import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseConnection:
    """Manages PostgreSQL database connections"""
    
    def __init__(self):
        """Initialize database connection parameters from .env"""
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')
        self.database = os.getenv('DB_NAME', 'ginja_claims')
        self.conn = None
    
    def connect(self):
        """Establish connection to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=int(self.port),
                user=self.user,
                password=self.password,
                database=self.database
            )
            return self.conn
        except Exception as e:
            raise Exception(f"Failed to connect to database: {e}")
    
    def connect_to_postgres(self):
        """Connect to default 'postgres' database for creating new database"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=int(self.port),
                user=self.user,
                password=self.password,
                database='postgres'
            )
            return conn
        except Exception as e:
            raise Exception(f"Failed to connect to postgres: {e}")
    
    def create_database_if_not_exists(self):
        """Create the main database if it doesn't exist"""
        try:
            conn = self.connect_to_postgres()
            conn.autocommit = True
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.database,)
            )
            
            if not cursor.fetchone():
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(self.database)
                ))
            
            cursor.close()
            conn.close()
        except Exception as e:
            raise Exception(f"Error creating database: {e}")
    
    def execute_query(self, query, params=None):
        """Execute a query and return cursor"""
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()