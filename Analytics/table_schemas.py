"""
Table schema definitions for analytics data loading.
"""
from typing import Dict, List, Tuple

class TableSchemas:
    """Table schema definitions for all 15 analytics tables."""
    
    # Common columns for all tables
    COMMON_COLUMNS = [
        "market VARCHAR(50)",
        "region VARCHAR(100)", 
        "vcptype VARCHAR(50)",
        "technology VARCHAR(20)",
        "datacenter VARCHAR(100)",
        "site VARCHAR(100)",
        "freq INTEGER"
    ]
    
    # Primary key definitions
    PRIMARY_KEYS = {
        'MKT_Samsung': ['id', 'timestamp'],
        'GNB_Samsung': ['id', 'timestamp'],
        'DU_Samsung': ['id', 'timestamp'],
        'SECTOR_Samsung': ['id', 'timestamp'],
        'CARRIER_Samsung': ['id', 'timestamp'],
        'MKT_Corning': ['id', 'timestamp'],
        'GNB_Corning': ['id', 'timestamp'],
        'DU_Corning': ['id', 'timestamp'],
        'SECTOR_Corning': ['id', 'timestamp'],
        'CARRIER_Corning': ['id', 'timestamp'],
        'MKT_Ericsson': ['id', 'timestamp'],
        'GNB_Ericsson': ['id', 'timestamp'],
        'SECTOR_Ericsson': ['id', 'timestamp'],
        'CARRIER_Ericsson': ['id', 'timestamp'],
        'ACPF_Samsung': ['id', 'timestamp'],
        'AUPF_Samsung': ['id', 'timestamp']
    }
    
    @staticmethod
    def get_table_creation_sql(table_name: str) -> str:
        """Generate CREATE TABLE SQL for a specific table."""
        base_columns = TableSchemas.COMMON_COLUMNS.copy()
        
        # Add specific columns based on table type
        if 'MKT' in table_name:
            base_columns.extend([
                "id VARCHAR(50)",
                "timestamp TIMESTAMP",
                "ran_market_5gnr_rlf_gnb_percent DECIMAL(10,4)",
                "ran_market_5gnr_rlf_gnb_den_number BIGINT",
                "ran_market_5gnr_rlf_gnb_num_number BIGINT",
                "ran_market_conn_no_max_number BIGINT",
                "ran_market_dl_trnsmssn_nacked_retrans_max_number BIGINT",
                "ran_market_dl_trnsmssn_retrans0_number BIGINT",
                "ran_market_dl_trnsmssn_retrans1_number BIGINT",
                "ran_market_endc_add_att_du_per_gnb_number BIGINT",
                "ran_market_erab_add_att_per_gnb_number BIGINT"
            ])
        elif 'GNB' in table_name:
            base_columns.extend([
                "id VARCHAR(50)",
                "timestamp TIMESTAMP",
                "gnb_5gnr_rlf_gnb_percent DECIMAL(10,4)",
                "gnb_5gnr_rlf_gnb_den_number BIGINT",
                "gnb_5gnr_rlf_gnb_num_number BIGINT",
                "gnb_conn_no_max_number BIGINT",
                "gnb_dl_trnsmssn_nacked_retrans_max_number BIGINT",
                "gnb_dl_trnsmssn_retrans0_number BIGINT",
                "gnb_dl_trnsmssn_retrans1_number BIGINT",
                "gnb_endc_add_att_du_per_gnb_number BIGINT",
                "gnb_erab_add_att_per_gnb_number BIGINT"
            ])
        elif 'DU' in table_name:
            base_columns.extend([
                "id VARCHAR(50)",
                "timestamp TIMESTAMP",
                "du_5gnr_rlf_gnb_percent DECIMAL(10,4)",
                "du_5gnr_rlf_gnb_den_number BIGINT",
                "du_5gnr_rlf_gnb_num_number BIGINT",
                "du_conn_no_max_number BIGINT",
                "du_dl_trnsmssn_nacked_retrans_max_number BIGINT",
                "du_dl_trnsmssn_retrans0_number BIGINT",
                "du_dl_trnsmssn_retrans1_number BIGINT",
                "du_endc_add_att_du_per_gnb_number BIGINT",
                "du_erab_add_att_per_gnb_number BIGINT"
            ])
        elif 'SECTOR' in table_name:
            base_columns.extend([
                "id VARCHAR(50)",
                "timestamp TIMESTAMP",
                "sector_5gnr_rlf_gnb_percent DECIMAL(10,4)",
                "sector_5gnr_rlf_gnb_den_number BIGINT",
                "sector_5gnr_rlf_gnb_num_number BIGINT",
                "sector_conn_no_max_number BIGINT",
                "sector_dl_trnsmssn_nacked_retrans_max_number BIGINT",
                "sector_dl_trnsmssn_retrans0_number BIGINT",
                "sector_dl_trnsmssn_retrans1_number BIGINT",
                "sector_endc_add_att_du_per_gnb_number BIGINT",
                "sector_erab_add_att_per_gnb_number BIGINT"
            ])
        elif 'CARRIER' in table_name:
            base_columns.extend([
                "id VARCHAR(50)",
                "timestamp TIMESTAMP",
                "ran_market_dl_gtp_data_volume_rn_mb DECIMAL(15,2)",
                "ran_market_endc_sessions_rn BIGINT",
                "ran_market_intra_cu_ho_attempts_number BIGINT",
                "ran_market_max_endc_sessions_rn_number BIGINT",
                "ran_market_intra_cu_ho_attempts_rn BIGINT",
                "ran_market_intra_cu_ho_success_rn BIGINT",
                "ran_market_intra_cu_ho_success_rate_rn_pct DECIMAL(10,4)",
                "ran_market_sgnb_modification_attempts_rn BIGINT",
                "ran_market_sgnb_modification_success_rn BIGINT",
                "ran_market_sgnb_modification_success_rate_rn_pct DECIMAL(10,4)",
                "ran_market_ul_gtp_data_volume_rn_mb DECIMAL(15,2)"
            ])
        elif 'ACPF' in table_name or 'AUPF' in table_name:
            base_columns.extend([
                "id VARCHAR(50)",
                "timestamp TIMESTAMP",
                "vcu_5gnr_rlf_gnb_percent DECIMAL(10,4)",
                "vcu_5gnr_rlf_gnb_den_number BIGINT",
                "vcu_5gnr_rlf_gnb_num_number BIGINT",
                "vcu_conn_no_max_number BIGINT",
                "vcu_dl_trnsmssn_nacked_retrans_max_number BIGINT",
                "vcu_dl_trnsmssn_retrans0_number BIGINT",
                "vcu_dl_trnsmssn_retrans1_number BIGINT",
                "vcu_endc_add_att_du_per_gnb_number BIGINT",
                "vcu_erab_add_att_per_gnb_number BIGINT"
            ])
        
        # Add common performance metrics columns
        base_columns.extend([
            "pm_radio_sinr_pucch_distr_gt0_lte3_number BIGINT",
            "pm_radio_sinr_pucch_distr_gt3_number BIGINT",
            "pm_radio_sinr_pucch_distr_gtneg12_lteneg9_number BIGINT",
            "pm_radio_sinr_pucch_distr_gtneg15_lteneg12_number BIGINT",
            "pm_radio_sinr_pucch_distr_gtneg3_lte0_number BIGINT",
            "pm_radio_sinr_pucch_distr_gtneg6_lteneg3_number BIGINT",
            "pm_radio_sinr_pucch_distr_gtneg9_lteneg6_number BIGINT",
            "pm_radio_sinr_pucch_distr_ltneg15_number BIGINT",
            "pm_radio_sinr_pusch_distr_1_number BIGINT",
            "pm_radio_sinr_pusch_distr_2_number BIGINT",
            "pm_radio_sinr_pusch_distr_3_number BIGINT",
            "pm_radio_sinr_pusch_distr_4_number BIGINT",
            "pm_radio_sinr_pusch_distr_5_number BIGINT",
            "pm_radio_sinr_pusch_distr_6_number BIGINT",
            "pm_radio_sinr_pusch_distr_7_number BIGINT",
            "pm_radio_sinr_pusch_distr_8_number BIGINT",
            "pm_radio_sinr_pusch_distr_9_number BIGINT",
            "pm_radio_sinr_pusch_distr_10_number BIGINT",
            "pm_radio_sinr_pusch_distr_11_number BIGINT",
            "pm_radio_sinr_pusch_distr_12_number BIGINT"
        ])
        
        # Get primary key columns
        primary_key_cols = TableSchemas.PRIMARY_KEYS.get(table_name, ['id', 'timestamp'])
        
        # Create the SQL
        columns_sql = ",\n    ".join(base_columns)
        primary_key_sql = ", ".join(primary_key_cols)
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql},
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY ({primary_key_sql})
        );
        """
        
        return sql
    
    @staticmethod
    def get_all_table_names() -> List[str]:
        """Get list of all table names."""
        return list(TableSchemas.PRIMARY_KEYS.keys())
    
    @staticmethod
    def get_table_mapping() -> Dict[str, str]:
        """Get mapping of file patterns to table names."""
        return {
            '_MKT_SAMSUNG_': 'MKT_Samsung',
            '_GNB_SAMSUNG_': 'GNB_Samsung', 
            '_DU_SAMSUNG_': 'DU_Samsung',
            '_SECTOR_SAMSUNG_': 'SECTOR_Samsung',
            '_CARRIER_SAMSUNG_': 'CARRIER_Samsung',
            '_MKT_CORNING_': 'MKT_Corning',
            '_GNB_CORNING_': 'GNB_Corning',
            '_DU_CORNING_': 'DU_Corning',
            '_SECTOR_CORNING_': 'SECTOR_Corning',
            '_CARRIER_CORNING_': 'CARRIER_Corning',
            '_MKT_ERICSSON_': 'MKT_Ericsson',
            '_GNB_ERICSSON_': 'GNB_Ericsson',
            '_SECTOR_ERICSSON_': 'SECTOR_Ericsson',
            '_CARRIER_ERICSSON_': 'CARRIER_Ericsson',
            '_VCU_SAMSUNG_': 'ACPF_Samsung'  # Both ACPF and AUPF use same pattern
        }
