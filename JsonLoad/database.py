import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
from models import Base

# Load environment variables with fallback
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception as e:
    print(f"Warning: Could not load .env file: {e}")
    print("Using default environment variables")

# Database configuration - using default values
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'hierarchy_db'
DB_USER = 'postgres'
DB_PASSWORD = 'test@1234'

# Create database URL with URL-encoded password
DATABASE_URL = f"postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Debug: Print connection details
print(f"Database URL: {DATABASE_URL}")
print(f"Host: {DB_HOST}, Port: {DB_PORT}, DB: {DB_NAME}, User: {DB_USER}")

# Create engine
engine = create_engine(DATABASE_URL, echo=True)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_tables():
    """Create all tables in the database"""
    try:
        Base.metadata.create_all(bind=engine)
        print("SUCCESS: Database tables created successfully!")
        return True
    except Exception as e:
        print(f"ERROR: Error creating database tables: {e}")
        return False

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def test_connection():
    """Test database connection"""
    try:
        with engine.connect() as connection:
            from sqlalchemy import text
            result = connection.execute(text("SELECT 1"))
            print("SUCCESS: Database connection successful!")
            return True
    except Exception as e:
        print(f"ERROR: Database connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing database connection...")
    if test_connection():
        print("Creating database tables...")
        create_tables()
    else:
        print("Please check your database configuration in .env file")
