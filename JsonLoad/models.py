from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, JSON, ForeignKey, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class GroupConfiguration(Base):
    __tablename__ = 'group_configurations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    condition = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    group_by = Column(String(255), nullable=True)
    group_name = Column(String(255), nullable=False, unique=True)
    group_type = Column(String(50), nullable=False)  # 'dynamic' or 'static'
    relation = Column(String(50), nullable=True)
    resources = Column(JSON, nullable=True)  # For static groups with resource arrays
    start_time = Column(BigInteger, nullable=True)
    status = Column(String(50), nullable=True)
    update_time = Column(BigInteger, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TimeScheduling(Base):
    __tablename__ = 'time_schedulings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    time_period = Column(JSON, nullable=True)  # Array of time periods
    rank_of_week = Column(Integer, nullable=True)
    enabled = Column(Boolean, default=True)
    tz = Column(String(100), nullable=True)  # Timezone
    rank_of_week_day = Column(Integer, nullable=True)
    day_of_month = Column(Integer, nullable=True)
    end = Column(String(100), nullable=True)  # End date string
    frequency = Column(Integer, nullable=True)
    every_day = Column(Integer, nullable=True)
    start = Column(String(100), nullable=True)  # Start date string
    day = Column(Integer, nullable=True)
    day_of_month_type = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Resource(Base):
    __tablename__ = 'resources'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    resource_id = Column(String(255), nullable=False)
    tenant = Column(String(100), nullable=True)
    resource_type = Column(String(100), nullable=True)
    group_configuration_id = Column(Integer, ForeignKey('group_configurations.id'), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    group_configuration = relationship("GroupConfiguration", back_populates="resource_objects")

# Add the relationship to GroupConfiguration
GroupConfiguration.resource_objects = relationship("Resource", back_populates="group_configuration")

class Ericsson5GEnrichment(Base):
    __tablename__ = 'ericsson_5g_enrichment'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    band = Column(String(50), nullable=True)
    trans_cell_type = Column(String(100), nullable=True)
    primary_site_name = Column(String(255), nullable=True)
    administrative_state = Column(String(50), nullable=True)
    operational_state = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Samsung5GEnrichment(Base):
    __tablename__ = 'samsung_5g_enrichment'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    site_name = Column(String(255), nullable=True)
    trans_cell_type = Column(String(100), nullable=True)
    gnodeb_duid = Column(String(255), nullable=True)
    du_name = Column(String(255), nullable=True)
    band = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
