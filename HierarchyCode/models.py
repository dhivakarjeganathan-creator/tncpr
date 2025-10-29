"""
Database models for the Hierarchy Management System
"""
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, CheckConstraint, UniqueConstraint, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import json
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import config

logger = logging.getLogger(__name__)

Base = declarative_base()

class Region(Base):
    """Region model - highest level entity"""
    __tablename__ = 'regions'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    markets = relationship("Market", back_populates="region", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Region(id={self.id}, name='{self.name}')>"

class Market(Base):
    """Market model - belongs to a region"""
    __tablename__ = 'markets'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    region_id = Column(Integer, ForeignKey('regions.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    region = relationship("Region", back_populates="markets")
    gnbs = relationship("GNB", back_populates="market", cascade="all, delete-orphan")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('name', 'region_id', name='uq_market_region'),
    )
    
    def __repr__(self):
        return f"<Market(id={self.id}, name='{self.name}', region_id={self.region_id})>"

class GNB(Base):
    """GNB model - belongs to a market"""
    __tablename__ = 'gnbs'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    market_id = Column(Integer, ForeignKey('markets.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    market = relationship("Market", back_populates="gnbs")
    dus = relationship("DU", back_populates="gnb", cascade="all, delete-orphan")
    sectors = relationship("Sector", back_populates="gnb", cascade="all, delete-orphan")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('name', 'market_id', name='uq_gnb_market'),
    )
    
    def __repr__(self):
        return f"<GNB(id={self.id}, name='{self.name}', market_id={self.market_id})>"

class DU(Base):
    """DU model - belongs to a GNB"""
    __tablename__ = 'dus'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    gnb_id = Column(Integer, ForeignKey('gnbs.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    gnb = relationship("GNB", back_populates="dus")
    sectors = relationship("Sector", back_populates="du", cascade="all, delete-orphan")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('name', 'gnb_id', name='uq_du_gnb'),
    )
    
    def __repr__(self):
        return f"<DU(id={self.id}, name='{self.name}', gnb_id={self.gnb_id})>"

class Sector(Base):
    """Sector model - belongs to a DU or directly to a GNB"""
    __tablename__ = 'sectors'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    du_id = Column(Integer, ForeignKey('dus.id', ondelete='CASCADE'), nullable=True)
    gnb_id = Column(Integer, ForeignKey('gnbs.id', ondelete='CASCADE'), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    du = relationship("DU", back_populates="sectors")
    gnb = relationship("GNB", back_populates="sectors")
    carriers = relationship("Carrier", back_populates="sector", cascade="all, delete-orphan")
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            '(du_id IS NOT NULL AND gnb_id IS NULL) OR (du_id IS NULL AND gnb_id IS NOT NULL)',
            name='check_sector_parent'
        ),
        UniqueConstraint('name', 'du_id', 'gnb_id', name='uq_sector_parent'),
    )
    
    def __repr__(self):
        return f"<Sector(id={self.id}, name='{self.name}', du_id={self.du_id}, gnb_id={self.gnb_id})>"

class Carrier(Base):
    """Carrier model - belongs to a sector"""
    __tablename__ = 'carriers'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    sector_id = Column(Integer, ForeignKey('sectors.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    sector = relationship("Sector", back_populates="carriers")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('name', 'sector_id', name='uq_carrier_sector'),
    )
    
    def __repr__(self):
        return f"<Carrier(id={self.id}, name='{self.name}', sector_id={self.sector_id})>"

class AuditLog(Base):
    """Audit log model - tracks CSV file processing"""
    __tablename__ = 'audit_log'
    
    id = Column(Integer, primary_key=True)
    file_name = Column(String(500), nullable=False, unique=True)
    file_path = Column(String(1000), nullable=False)
    file_hash = Column(String(64), nullable=False)  # SHA-256 hash
    processed_at = Column(DateTime, default=datetime.utcnow)
    records_added = Column(String)  # JSON string storing count of new records
    status = Column(String(50), default='SUCCESS')
    error_message = Column(String)
    processing_time_seconds = Column(String(20))
    
    def __repr__(self):
        return f"<AuditLog(id={self.id}, file_name='{self.file_name}', status='{self.status}')>"
    
    def get_records_added(self):
        """Parse and return records_added as dictionary"""
        if self.records_added:
            return json.loads(self.records_added)
        return {}
    
    def set_records_added(self, records_dict):
        """Set records_added from dictionary"""
        self.records_added = json.dumps(records_dict)

# Database engine and session
engine = create_engine(config.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_database_if_not_exists():
    """Create database if it doesn't exist"""
    try:
        # Connect to PostgreSQL server (not to specific database)
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database='postgres'  # Connect to default postgres database
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{config.DB_NAME}'")
        exists = cursor.fetchone()
        
        if not exists:
            logger.info(f"Creating database '{config.DB_NAME}'...")
            cursor.execute(f'CREATE DATABASE "{config.DB_NAME}"')
            logger.info(f"Database '{config.DB_NAME}' created successfully")
        else:
            logger.info(f"Database '{config.DB_NAME}' already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        return False

def create_tables():
    """Create all tables"""
    logger.info("Creating tables...")
    Base.metadata.create_all(bind=engine)
