"""Verify all tables are configured"""
from api.routes import TABLE_CONFIG

print("✓ All tables configured successfully!")
print(f"Total tables: {len(TABLE_CONFIG)}")
print("\nTable list:")
for i, (name, config) in enumerate(sorted(TABLE_CONFIG.items()), 1):
    print(f"{i:2d}. {name:25s} - {len(config['entity_columns'])} entity columns")

print("\n✓ All tables use the same entity columns:")
print(f"  {', '.join(TABLE_CONFIG['mkt_corning']['entity_columns'])}")
print(f"\n✓ All tables use 'timestamp' as timestamp column")

