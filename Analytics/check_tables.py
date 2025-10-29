from database_config import DatabaseConfig

db = DatabaseConfig()
try:
    result = db.execute_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
    print("Available tables:")
    for row in result:
        print(f"- {row['table_name']}")
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close_connection()
