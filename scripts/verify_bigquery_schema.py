#!/usr/bin/env python3
"""
🔍 BigQuery Schema Verification Script
Verify actual column names and data types in BI_USA dataset

Run this BEFORE running any ETL to verify our assumptions are correct.
"""

import asyncio
from google.cloud import bigquery
from google.auth import default

async def verify_schema():
    """Verify schema for all tables used in ETL config"""
    
    # Initialize BigQuery client
    credentials, _ = default()
    client = bigquery.Client(project="mibot-222814", credentials=credentials)
    
    # Tables to verify
    tables_to_check = [
        "bi_P3fV4dWNeMkN5RJMhV8e_dash_calendario_v5",
        "batch_P3fV4dWNeMkN5RJMhV8e_asignacion", 
        "batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda",
        "voicebot_P3fV4dWNeMkN5RJMhV8e",
        "mibotair_P3fV4dWNeMkN5RJMhV8e",
        "batch_P3fV4dWNeMkN5RJMhV8e_pagos"
    ]
    
    print("🔍 BIGQUERY SCHEMA VERIFICATION")
    print("=" * 50)
    
    for table_name in tables_to_check:
        print(f"\n📋 TABLE: {table_name}")
        print("-" * 40)
        
        try:
            # Get table schema
            table_ref = client.dataset("BI_USA").table(table_name)
            table = client.get_table(table_ref)
            
            print(f"✅ EXISTS: {table.num_rows:,} rows")
            print("📊 SCHEMA:")
            
            for field in table.schema[:10]:  # First 10 columns
                print(f"  - {field.name} ({field.field_type})")
            
            if len(table.schema) > 10:
                print(f"  ... and {len(table.schema) - 10} more columns")
                
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
    
    print("\n🎯 CRITICAL COLUMNS TO VERIFY:")
    print("-" * 40)
    
    # Check critical columns exist
    critical_checks = {
        "voicebot_P3fV4dWNeMkN5RJMhV8e": ["document", "date", "campaign_name", "management"],
        "mibotair_P3fV4dWNeMkN5RJMhV8e": ["document", "date", "campaign_name", "n1", "n2", "n3"],
        "batch_P3fV4dWNeMkN5RJMhV8e_asignacion": ["archivo", "cod_luna", "cuenta", "creado_el"],
        "batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda": ["cod_cuenta", "monto_exigible", "creado_el"]
    }
    
    for table_name, required_cols in critical_checks.items():
        print(f"\n🔍 {table_name}:")
        try:
            table_ref = client.dataset("BI_USA").table(table_name)
            table = client.get_table(table_ref)
            existing_cols = [field.name for field in table.schema]
            
            for col in required_cols:
                if col in existing_cols:
                    print(f"  ✅ {col}")
                else:
                    print(f"  ❌ {col} - NOT FOUND!")
                    # Suggest similar columns
                    similar = [c for c in existing_cols if col.lower() in c.lower() or c.lower() in col.lower()]
                    if similar:
                        print(f"     💡 Similar: {similar}")
                        
        except Exception as e:
            print(f"  ❌ TABLE ERROR: {str(e)}")

if __name__ == "__main__":
    asyncio.run(verify_schema())
