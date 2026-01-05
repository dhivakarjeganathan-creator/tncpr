"""
Main FastAPI application for KPI Timeseries API
"""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from api.routes import router
from database.connection import db_connection
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if Config.DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting KPI Timeseries API")
    logger.info(f"Database type: {Config.DB_TYPE}")
    logger.info(f"Database: {Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down KPI Timeseries API")
    db_connection.close()


# Create FastAPI app
app = FastAPI(
    title="KPI Timeseries API",
    description="REST API for querying KPI timeseries data from PostgreSQL or Watsonx Data (Presto)",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(router, prefix="/api/v1", tags=["timeseries"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "KPI Timeseries API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=Config.API_HOST,
        port=Config.API_PORT,
        reload=Config.DEBUG
    )

