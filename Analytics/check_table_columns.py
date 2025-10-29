from database_config import DatabaseConfig

db = DatabaseConfig()
try:
    # Check acpf_samsung table columns
    result = db.execute_query("SELECT column_name FROM information_schema.columns WHERE table_name = 'acpf_samsung' ORDER BY ordinal_position")
    print("Columns in acpf_samsung:")
    for row in result:
        print(f"- {row['column_name']}")
    
    print("\n" + "="*50)
    
    # Check aupf_samsung table columns
    result = db.execute_query("SELECT column_name FROM information_schema.columns WHERE table_name = 'aupf_samsung' ORDER BY ordinal_position")
    print("Columns in aupf_samsung:")
    for row in result:
        print(f"- {row['column_name']}")
        
    print("\n" + "="*50)
    
    # Check du_samsung table columns
    result = db.execute_query("SELECT column_name FROM information_schema.columns WHERE table_name = 'du_samsung' ORDER BY ordinal_position")
    print("Columns in du_samsung:")
    for row in result:
        print(f"- {row['column_name']}")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close_connection()
