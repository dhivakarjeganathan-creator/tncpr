"""
CSV processing functionality for the Hierarchy Management System
"""
import pandas as pd
import os
import hashlib
import logging
import time
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from config import config
from crud_operations import CRUDOperations
from models import SessionLocal, Market, GNB, DU, Sector

logger = logging.getLogger(__name__)

class CSVProcessor:
    """Handles CSV file processing and data import"""
    
    def __init__(self):
        self.crud = None
        self.db = None
    
    def _get_db_session(self):
        """Get database session"""
        if not self.db:
            self.db = SessionLocal()
            self.crud = CRUDOperations(self.db)
        return self.db, self.crud
    
    def _close_db_session(self):
        """Close database session"""
        if self.db:
            self.db.close()
            self.db = None
            self.crud = None
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of file"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _normalize_column_name(self, column_name: str) -> Optional[str]:
        """Normalize column name to match our entity types"""
        column_name = column_name.strip().lower()
        
        for entity_type, possible_names in config.COLUMN_MAPPINGS.items():
            if column_name in [name.lower() for name in possible_names]:
                return entity_type
        
        return None
    
    def _find_entity_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Find entity columns in the DataFrame"""
        entity_columns = {}
        
        for col in df.columns:
            normalized_name = self._normalize_column_name(col)
            if normalized_name:
                entity_columns[normalized_name] = col
        
        return entity_columns
    
    def _process_region_data(self, df: pd.DataFrame, region_col: str) -> int:
        """Process region data and return count of new regions created"""
        new_count = 0
        
        for _, row in df.iterrows():
            region_name = str(row[region_col]).strip()
            if region_name and region_name.lower() not in ['nan', 'none', '']:
                existing_region = self.crud.get_region_by_name(region_name)
                if not existing_region:
                    self.crud.create_region(region_name)
                    new_count += 1
        
        return new_count
    
    def _process_market_data(self, df: pd.DataFrame, market_col: str, region_col: str = None) -> int:
        """Process market data and return count of new markets created"""
        new_count = 0
        
        for _, row in df.iterrows():
            market_name = str(row[market_col]).strip()
            if market_name and market_name.lower() not in ['nan', 'none', '']:
                region_name = None
                if region_col and region_col in row:
                    region_name = str(row[region_col]).strip()
                
                if region_name and region_name.lower() not in ['nan', 'none', '']:
                    region = self.crud.get_region_by_name(region_name)
                    if region:
                        existing_market = self.crud.get_market_by_name_and_region(market_name, region.id)
                        if not existing_market:
                            self.crud.create_market(market_name, region.id)
                            new_count += 1
        
        return new_count
    
    def _process_gnb_data(self, df: pd.DataFrame, gnb_col: str, market_col: str = None) -> int:
        """Process GNB data and return count of new GNBs created"""
        new_count = 0
        
        for _, row in df.iterrows():
            gnb_name = str(row[gnb_col]).strip()
            if gnb_name and gnb_name.lower() not in ['nan', 'none', '']:
                market_name = None
                if market_col and market_col in row:
                    market_name = str(row[market_col]).strip()
                
                if market_name and market_name.lower() not in ['nan', 'none', '']:
                    # Find market by name (assuming unique market names across regions)
                    market = self.crud.get_market_by_name(market_name)
                    if market:
                        existing_gnb = self.crud.get_gnb_by_name_and_market(gnb_name, market.id)
                        if not existing_gnb:
                            self.crud.create_gnb(gnb_name, market.id)
                            new_count += 1
        
        return new_count
    
    def _process_du_data(self, df: pd.DataFrame, du_col: str, gnb_col: str = None) -> int:
        """Process DU data and return count of new DUs created"""
        new_count = 0
        
        for _, row in df.iterrows():
            du_name = str(row[du_col]).strip()
            if du_name and du_name.lower() not in ['nan', 'none', '']:
                gnb_name = None
                if gnb_col and gnb_col in row:
                    gnb_name = str(row[gnb_col]).strip()
                
                if gnb_name and gnb_name.lower() not in ['nan', 'none', '']:
                    # Find GNB by name (assuming unique GNB names across markets)
                    gnb = self.crud.get_gnb_by_name(gnb_name)
                    if gnb:
                        existing_du = self.crud.get_du_by_name_and_gnb(du_name, gnb.id)
                        if not existing_du:
                            self.crud.create_du(du_name, gnb.id)
                            new_count += 1
        
        return new_count
    
    def _process_sector_data(self, df: pd.DataFrame, sector_col: str, du_col: str = None, gnb_col: str = None) -> int:
        """Process sector data and return count of new sectors created"""
        new_count = 0
        
        for _, row in df.iterrows():
            sector_name = str(row[sector_col]).strip()
            if sector_name and sector_name.lower() not in ['nan', 'none', '']:
                du_name = None
                gnb_name = None
                
                if du_col and du_col in row:
                    du_name = str(row[du_col]).strip()
                if gnb_col and gnb_col in row:
                    gnb_name = str(row[gnb_col]).strip()
                
                # Determine parent (DU takes precedence over GNB)
                du_id = None
                gnb_id = None
                
                if du_name and du_name.lower() not in ['nan', 'none', '']:
                    du = self.crud.get_du_by_name(du_name)
                    if du:
                        du_id = du.id
                elif gnb_name and gnb_name.lower() not in ['nan', 'none', '']:
                    gnb = self.crud.get_gnb_by_name(gnb_name)
                    if gnb:
                        gnb_id = gnb.id
                
                if du_id or gnb_id:
                    existing_sector = self.crud.get_sector_by_name_and_parent(sector_name, du_id, gnb_id)
                    if not existing_sector:
                        self.crud.create_sector(sector_name, du_id, gnb_id)
                        new_count += 1
        
        return new_count
    
    def _process_carrier_data(self, df: pd.DataFrame, carrier_col: str, sector_col: str = None) -> int:
        """Process carrier data and return count of new carriers created"""
        new_count = 0
        
        for _, row in df.iterrows():
            carrier_name = str(row[carrier_col]).strip()
            if carrier_name and carrier_name.lower() not in ['nan', 'none', '']:
                sector_name = None
                if sector_col and sector_col in row:
                    sector_name = str(row[sector_col]).strip()
                
                if sector_name and sector_name.lower() not in ['nan', 'none', '']:
                    # Find sector by name (assuming unique sector names across parents)
                    sector = self.crud.get_sector_by_name(sector_name)
                    if sector:
                        existing_carrier = self.crud.get_carrier_by_name_and_sector(carrier_name, sector.id)
                        if not existing_carrier:
                            self.crud.create_carrier(carrier_name, sector.id)
                            new_count += 1
        
        return new_count
    
    def process_csv_file(self, file_path: str) -> Dict[str, any]:
        """Process a single CSV file and return processing results"""
        start_time = time.time()
        file_hash = self._calculate_file_hash(file_path)
        
        # Check if file already processed
        db, crud = self._get_db_session()
        if crud.file_already_processed(file_hash):
            logger.warning(f"File {file_path} already processed, skipping")
            return {
                'status': 'SKIPPED',
                'message': 'File already processed',
                'file_hash': file_hash
            }
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path, dtype=str)
            logger.info(f"Processing CSV file: {file_path} with {len(df)} rows")
            
            # Find entity columns
            entity_columns = self._find_entity_columns(df)
            logger.info(f"Found entity columns: {entity_columns}")
            logger.info("Entering entity columns processing")
            
            if not entity_columns:
                raise ValueError("No recognized entity columns found in CSV file")
            
            # Process data based on available columns
            records_added = {}
            
            logger.info("Entering region data processing")
            # Process in hierarchy order
            if 'region' in entity_columns:
                records_added['regions'] = self._process_region_data(df, entity_columns['region'])
            
            logger.info("Entering market data processing")
            
            if 'market' in entity_columns:
                region_col = entity_columns.get('region')
                records_added['markets'] = self._process_market_data(df, entity_columns['market'], region_col)
            
            logger.info("Entering gnb data processing")
            
            if 'gnb' in entity_columns:
                market_col = entity_columns.get('market')
                records_added['gnbs'] = self._process_gnb_data(df, entity_columns['gnb'], market_col)

            logger.info("Entering dus data processing")
            
            if 'du' in entity_columns:
                gnb_col = entity_columns.get('gnb')
                records_added['dus'] = self._process_du_data(df, entity_columns['du'], gnb_col)

            logger.info("Entering sector data processing")
            
            if 'sector' in entity_columns:
                du_col = entity_columns.get('du')
                gnb_col = entity_columns.get('gnb')
                records_added['sectors'] = self._process_sector_data(df, entity_columns['sector'], du_col, gnb_col)
            
            logger.info("Entering carrier data processing")

            if 'carrier' in entity_columns:
                sector_col = entity_columns.get('sector')
                records_added['carriers'] = self._process_carrier_data(df, entity_columns['carrier'], sector_col)
            
            # Create audit log
            processing_time = time.time() - start_time
            file_name = os.path.basename(file_path)
            
            crud.create_audit_log(
                file_name=file_name,
                file_path=file_path,
                file_hash=file_hash,
                records_added=records_added,
                status='SUCCESS',
                processing_time=processing_time
            )
            
            logger.info(f"Successfully processed {file_path}. Records added: {records_added}")
            
            return {
                'status': 'SUCCESS',
                'records_added': records_added,
                'file_hash': file_hash,
                'processing_time': processing_time
            }
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            
            # Create error audit log
            processing_time = time.time() - start_time
            file_name = os.path.basename(file_path)
            
            crud.create_audit_log(
                file_name=file_name,
                file_path=file_path,
                file_hash=file_hash,
                records_added={},
                status='ERROR',
                error_message=str(e),
                processing_time=processing_time
            )
            
            return {
                'status': 'ERROR',
                'error': str(e),
                'file_hash': file_hash,
                'processing_time': processing_time
            }
        
        finally:
            self._close_db_session()
    
    def process_csv_folder(self, folder_path: str = None) -> List[Dict[str, any]]:
        """Process all CSV files in a folder"""
        if folder_path is None:
            folder_path = config.CSV_FOLDER_PATH
        
        if not os.path.exists(folder_path):
            logger.error(f"CSV folder does not exist: {folder_path}")
            return []
        
        results = []
        csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]
        
        logger.info(f"Found {len(csv_files)} CSV files in {folder_path}")
        
        for csv_file in csv_files:
            file_path = os.path.join(folder_path, csv_file)
            result = self.process_csv_file(file_path)
            results.append(result)
        
        return results
    
    def get_processing_summary(self) -> Dict[str, any]:
        """Get summary of all processed files"""
        db, crud = self._get_db_session()
        try:
            audit_logs = crud.get_all_audit_logs()
            
            summary = {
                'total_files_processed': len(audit_logs),
                'successful_files': len([log for log in audit_logs if log.status == 'SUCCESS']),
                'failed_files': len([log for log in audit_logs if log.status == 'ERROR']),
                'total_records_added': {
                    'regions': 0,
                    'markets': 0,
                    'gnbs': 0,
                    'dus': 0,
                    'sectors': 0,
                    'carriers': 0
                },
                'recent_files': []
            }
            
            for log in audit_logs[:10]:  # Last 10 files
                records = log.get_records_added()
                for entity_type, count in records.items():
                    if entity_type in summary['total_records_added']:
                        summary['total_records_added'][entity_type] += count
                
                summary['recent_files'].append({
                    'file_name': log.file_name,
                    'status': log.status,
                    'processed_at': log.processed_at,
                    'records_added': records
                })
            
            return summary
            
        finally:
            self._close_db_session()
