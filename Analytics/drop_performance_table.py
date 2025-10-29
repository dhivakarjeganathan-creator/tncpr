from database_config import DatabaseConfig

db = DatabaseConfig()
try:
    db.execute_query('DROP TABLE IF EXISTS "PerformanceRules"')
    db.execute_query('DROP TABLE IF EXISTS "mkt_samsung"')
    db.execute_query('DROP TABLE IF EXISTS "gnb_samsung"')
    db.execute_query('DROP TABLE IF EXISTS "du_samsung"')
    db.execute_query('DROP TABLE IF EXISTS "sector_samsung"')
    db.execute_query('DROP TABLE IF EXISTS "carrier_samsung"')
    db.execute_query('DROP TABLE IF EXISTS "mkt_corning"')
    db.execute_query('DROP TABLE IF EXISTS "gnb_corning"')
    db.execute_query('DROP TABLE IF EXISTS "du_corning"')
    db.execute_query('DROP TABLE IF EXISTS "sector_corning"')
    db.execute_query('DROP TABLE IF EXISTS "carrier_corning"')
    db.execute_query('DROP TABLE IF EXISTS "mkt_ericsson"')
    db.execute_query('DROP TABLE IF EXISTS "gnb_ericsson"')
    db.execute_query('DROP TABLE IF EXISTS "sector_ericsson"')
    db.execute_query('DROP TABLE IF EXISTS "carrier_ericsson"')
    db.execute_query('DROP TABLE IF EXISTS "acpf_samsung"')
    db.execute_query('DROP TABLE IF EXISTS "aupf_samsung"')
    print("Table dropped successfully")
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close_connection()
