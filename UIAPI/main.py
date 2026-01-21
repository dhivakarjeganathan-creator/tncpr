"""
Main FastAPI application for UI Timeseries API.
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import router
from config import Config
from database.connection import db_connection

logging.basicConfig(
    level=logging.DEBUG if Config.DEBUG else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting UI Timeseries API")
    logger.info("Database type: %s", Config.DB_TYPE)
    logger.info("Database: %s:%s/%s", Config.DB_HOST, Config.DB_PORT, Config.DB_NAME)
    yield
    logger.info("Shutting down UI Timeseries API")
    db_connection.close()


app = FastAPI(
    title="UI Timeseries API",
    description="REST API for UI Timeseries data on PostgreSQL or Watsonx Data (Presto)",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/")
async def root():
    return {
        "message": "UI Timeseries API",
        "version": "1.0.0",
        "docs": "/docs",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=Config.API_HOST,
        port=Config.API_PORT,
        reload=Config.DEBUG,
    )
