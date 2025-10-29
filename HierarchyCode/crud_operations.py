"""
CRUD operations for the Hierarchy Management System
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from models import Region, Market, GNB, DU, Sector, Carrier, AuditLog
from typing import List, Optional, Dict, Any
import logging
import json

logger = logging.getLogger(__name__)

class CRUDOperations:
    """CRUD operations for all hierarchy entities"""
    
    def __init__(self, db: Session):
        self.db = db
    
    # Region CRUD operations
    def create_region(self, name: str) -> Region:
        """Create a new region"""
        try:
            region = Region(name=name)
            self.db.add(region)
            self.db.commit()
            self.db.refresh(region)
            logger.info(f"Created region: {region}")
            return region
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating region {name}: {e}")
            raise
    
    def get_region(self, region_id: int) -> Optional[Region]:
        """Get region by ID"""
        return self.db.query(Region).filter(Region.id == region_id).first()
    
    def get_region_by_name(self, name: str) -> Optional[Region]:
        """Get region by name"""
        return self.db.query(Region).filter(Region.name == name).first()
    
    def get_all_regions(self) -> List[Region]:
        """Get all regions"""
        return self.db.query(Region).all()
    
    def update_region(self, region_id: int, name: str) -> Optional[Region]:
        """Update region"""
        try:
            region = self.get_region(region_id)
            if region:
                region.name = name
                self.db.commit()
                self.db.refresh(region)
                logger.info(f"Updated region: {region}")
            return region
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating region {region_id}: {e}")
            raise
    
    def delete_region(self, region_id: int) -> bool:
        """Delete region (cascades to all children)"""
        try:
            region = self.get_region(region_id)
            if region:
                self.db.delete(region)
                self.db.commit()
                logger.info(f"Deleted region: {region}")
                return True
            return False
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting region {region_id}: {e}")
            raise
    
    # Market CRUD operations
    def create_market(self, name: str, region_id: int) -> Market:
        """Create a new market"""
        try:
            market = Market(name=name, region_id=region_id)
            self.db.add(market)
            self.db.commit()
            self.db.refresh(market)
            logger.info(f"Created market: {market}")
            return market
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating market {name}: {e}")
            raise
    
    def get_market(self, market_id: int) -> Optional[Market]:
        """Get market by ID"""
        return self.db.query(Market).filter(Market.id == market_id).first()
    
    def get_market_by_name_and_region(self, name: str, region_id: int) -> Optional[Market]:
        """Get market by name and region"""
        return self.db.query(Market).filter(
            and_(Market.name == name, Market.region_id == region_id)
        ).first()
    
    def get_markets_by_region(self, region_id: int) -> List[Market]:
        """Get all markets for a region"""
        return self.db.query(Market).filter(Market.region_id == region_id).all()
    
    def get_market_by_name(self, name: str) -> Optional[Market]:
        """Get market by name (first match)"""
        return self.db.query(Market).filter(Market.name == name).first()
    
    def update_market(self, market_id: int, name: str, region_id: int) -> Optional[Market]:
        """Update market"""
        try:
            market = self.get_market(market_id)
            if market:
                market.name = name
                market.region_id = region_id
                self.db.commit()
                self.db.refresh(market)
                logger.info(f"Updated market: {market}")
            return market
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating market {market_id}: {e}")
            raise
    
    def delete_market(self, market_id: int) -> bool:
        """Delete market"""
        try:
            market = self.get_market(market_id)
            if market:
                self.db.delete(market)
                self.db.commit()
                logger.info(f"Deleted market: {market}")
                return True
            return False
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting market {market_id}: {e}")
            raise
    
    # GNB CRUD operations
    def create_gnb(self, name: str, market_id: int) -> GNB:
        """Create a new GNB"""
        try:
            gnb = GNB(name=name, market_id=market_id)
            self.db.add(gnb)
            self.db.commit()
            self.db.refresh(gnb)
            logger.info(f"Created GNB: {gnb}")
            return gnb
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating GNB {name}: {e}")
            raise
    
    def get_gnb(self, gnb_id: int) -> Optional[GNB]:
        """Get GNB by ID"""
        return self.db.query(GNB).filter(GNB.id == gnb_id).first()
    
    def get_gnb_by_name_and_market(self, name: str, market_id: int) -> Optional[GNB]:
        """Get GNB by name and market"""
        return self.db.query(GNB).filter(
            and_(GNB.name == name, GNB.market_id == market_id)
        ).first()
    
    def get_gnbs_by_market(self, market_id: int) -> List[GNB]:
        """Get all GNBs for a market"""
        return self.db.query(GNB).filter(GNB.market_id == market_id).all()
    
    def get_gnb_by_name(self, name: str) -> Optional[GNB]:
        """Get GNB by name (first match)"""
        return self.db.query(GNB).filter(GNB.name == name).first()
    
    def update_gnb(self, gnb_id: int, name: str, market_id: int) -> Optional[GNB]:
        """Update GNB"""
        try:
            gnb = self.get_gnb(gnb_id)
            if gnb:
                gnb.name = name
                gnb.market_id = market_id
                self.db.commit()
                self.db.refresh(gnb)
                logger.info(f"Updated GNB: {gnb}")
            return gnb
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating GNB {gnb_id}: {e}")
            raise
    
    def delete_gnb(self, gnb_id: int) -> bool:
        """Delete GNB"""
        try:
            gnb = self.get_gnb(gnb_id)
            if gnb:
                self.db.delete(gnb)
                self.db.commit()
                logger.info(f"Deleted GNB: {gnb}")
                return True
            return False
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting GNB {gnb_id}: {e}")
            raise
    
    # DU CRUD operations
    def create_du(self, name: str, gnb_id: int) -> DU:
        """Create a new DU"""
        try:
            du = DU(name=name, gnb_id=gnb_id)
            self.db.add(du)
            self.db.commit()
            self.db.refresh(du)
            logger.info(f"Created DU: {du}")
            return du
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating DU {name}: {e}")
            raise
    
    def get_du(self, du_id: int) -> Optional[DU]:
        """Get DU by ID"""
        return self.db.query(DU).filter(DU.id == du_id).first()
    
    def get_du_by_name_and_gnb(self, name: str, gnb_id: int) -> Optional[DU]:
        """Get DU by name and GNB"""
        return self.db.query(DU).filter(
            and_(DU.name == name, DU.gnb_id == gnb_id)
        ).first()
    
    def get_dus_by_gnb(self, gnb_id: int) -> List[DU]:
        """Get all DUs for a GNB"""
        return self.db.query(DU).filter(DU.gnb_id == gnb_id).all()
    
    def get_du_by_name(self, name: str) -> Optional[DU]:
        """Get DU by name (first match)"""
        return self.db.query(DU).filter(DU.name == name).first()
    
    def update_du(self, du_id: int, name: str, gnb_id: int) -> Optional[DU]:
        """Update DU"""
        try:
            du = self.get_du(du_id)
            if du:
                du.name = name
                du.gnb_id = gnb_id
                self.db.commit()
                self.db.refresh(du)
                logger.info(f"Updated DU: {du}")
            return du
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating DU {du_id}: {e}")
            raise
    
    def delete_du(self, du_id: int) -> bool:
        """Delete DU"""
        try:
            du = self.get_du(du_id)
            if du:
                self.db.delete(du)
                self.db.commit()
                logger.info(f"Deleted DU: {du}")
                return True
            return False
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting DU {du_id}: {e}")
            raise
    
    # Sector CRUD operations
    def create_sector(self, name: str, du_id: Optional[int] = None, gnb_id: Optional[int] = None) -> Sector:
        """Create a new sector (must have either du_id or gnb_id, not both)"""
        try:
            if not ((du_id is not None and gnb_id is None) or (du_id is None and gnb_id is not None)):
                raise ValueError("Sector must have either du_id or gnb_id, not both or neither")
            
            sector = Sector(name=name, du_id=du_id, gnb_id=gnb_id)
            self.db.add(sector)
            self.db.commit()
            self.db.refresh(sector)
            logger.info(f"Created sector: {sector}")
            return sector
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating sector {name}: {e}")
            raise
    
    def get_sector(self, sector_id: int) -> Optional[Sector]:
        """Get sector by ID"""
        return self.db.query(Sector).filter(Sector.id == sector_id).first()
    
    def get_sector_by_name_and_parent(self, name: str, du_id: Optional[int] = None, gnb_id: Optional[int] = None) -> Optional[Sector]:
        """Get sector by name and parent"""
        return self.db.query(Sector).filter(
            and_(
                Sector.name == name,
                Sector.du_id == du_id,
                Sector.gnb_id == gnb_id
            )
        ).first()
    
    def get_sectors_by_du(self, du_id: int) -> List[Sector]:
        """Get all sectors for a DU"""
        return self.db.query(Sector).filter(Sector.du_id == du_id).all()
    
    def get_sectors_by_gnb(self, gnb_id: int) -> List[Sector]:
        """Get all sectors for a GNB"""
        return self.db.query(Sector).filter(Sector.gnb_id == gnb_id).all()
    
    def get_sector_by_name(self, name: str) -> Optional[Sector]:
        """Get sector by name (first match)"""
        return self.db.query(Sector).filter(Sector.name == name).first()
    
    def update_sector(self, sector_id: int, name: str, du_id: Optional[int] = None, gnb_id: Optional[int] = None) -> Optional[Sector]:
        """Update sector"""
        try:
            if not ((du_id is not None and gnb_id is None) or (du_id is None and gnb_id is not None)):
                raise ValueError("Sector must have either du_id or gnb_id, not both or neither")
            
            sector = self.get_sector(sector_id)
            if sector:
                sector.name = name
                sector.du_id = du_id
                sector.gnb_id = gnb_id
                self.db.commit()
                self.db.refresh(sector)
                logger.info(f"Updated sector: {sector}")
            return sector
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating sector {sector_id}: {e}")
            raise
    
    def delete_sector(self, sector_id: int) -> bool:
        """Delete sector"""
        try:
            sector = self.get_sector(sector_id)
            if sector:
                self.db.delete(sector)
                self.db.commit()
                logger.info(f"Deleted sector: {sector}")
                return True
            return False
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting sector {sector_id}: {e}")
            raise
    
    # Carrier CRUD operations
    def create_carrier(self, name: str, sector_id: int) -> Carrier:
        """Create a new carrier"""
        try:
            carrier = Carrier(name=name, sector_id=sector_id)
            self.db.add(carrier)
            self.db.commit()
            self.db.refresh(carrier)
            logger.info(f"Created carrier: {carrier}")
            return carrier
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating carrier {name}: {e}")
            raise
    
    def get_carrier(self, carrier_id: int) -> Optional[Carrier]:
        """Get carrier by ID"""
        return self.db.query(Carrier).filter(Carrier.id == carrier_id).first()
    
    def get_carrier_by_name_and_sector(self, name: str, sector_id: int) -> Optional[Carrier]:
        """Get carrier by name and sector"""
        return self.db.query(Carrier).filter(
            and_(Carrier.name == name, Carrier.sector_id == sector_id)
        ).first()
    
    def get_carriers_by_sector(self, sector_id: int) -> List[Carrier]:
        """Get all carriers for a sector"""
        return self.db.query(Carrier).filter(Carrier.sector_id == sector_id).all()
    
    def update_carrier(self, carrier_id: int, name: str, sector_id: int) -> Optional[Carrier]:
        """Update carrier"""
        try:
            carrier = self.get_carrier(carrier_id)
            if carrier:
                carrier.name = name
                carrier.sector_id = sector_id
                self.db.commit()
                self.db.refresh(carrier)
                logger.info(f"Updated carrier: {carrier}")
            return carrier
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating carrier {carrier_id}: {e}")
            raise
    
    def delete_carrier(self, carrier_id: int) -> bool:
        """Delete carrier"""
        try:
            carrier = self.get_carrier(carrier_id)
            if carrier:
                self.db.delete(carrier)
                self.db.commit()
                logger.info(f"Deleted carrier: {carrier}")
                return True
            return False
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting carrier {carrier_id}: {e}")
            raise
    
    # Audit Log operations
    def create_audit_log(self, file_name: str, file_path: str, file_hash: str, 
                        records_added: Dict[str, int], status: str = 'SUCCESS', 
                        error_message: str = None, processing_time: float = None) -> AuditLog:
        """Create audit log entry"""
        try:
            audit_log = AuditLog(
                file_name=file_name,
                file_path=file_path,
                file_hash=file_hash,
                records_added=json.dumps(records_added) if records_added else None,
                status=status,
                error_message=error_message,
                processing_time_seconds=str(processing_time) if processing_time else None
            )
            self.db.add(audit_log)
            self.db.commit()
            self.db.refresh(audit_log)
            logger.info(f"Created audit log: {audit_log}")
            return audit_log
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating audit log: {e}")
            raise
    
    def get_audit_log_by_hash(self, file_hash: str) -> Optional[AuditLog]:
        """Get audit log by file hash"""
        return self.db.query(AuditLog).filter(AuditLog.file_hash == file_hash).first()
    
    def get_all_audit_logs(self) -> List[AuditLog]:
        """Get all audit logs"""
        return self.db.query(AuditLog).order_by(AuditLog.processed_at.desc()).all()
    
    def file_already_processed(self, file_hash: str) -> bool:
        """Check if file has already been processed"""
        return self.get_audit_log_by_hash(file_hash) is not None
