# Sequence Diagram - CSV to Iceberg Pipeline

This document contains sequence diagrams showing the flow of operations in the CSV to Iceberg pipeline.

## Main Processing Flow

```mermaid
sequenceDiagram
    participant Main as main.py
    participant Orchestrator as PipelineOrchestrator
    participant FileProcessor as FileProcessor
    participant MinIO as MinIOClient
    participant SchemaMgr as SchemaManager
    participant IcebergMgr as IcebergManager
    participant Spark as SparkSession

    Main->>Orchestrator: Initialize (load config)
    Orchestrator->>Spark: Create SparkSession
    Orchestrator->>MinIO: Initialize MinIOClient
    Orchestrator->>SchemaMgr: Initialize SchemaManager
    Orchestrator->>IcebergMgr: Initialize IcebergManager
    Orchestrator->>FileProcessor: Initialize FileProcessor
    
    Main->>Orchestrator: run_single_cycle() or run_continuous()
    
    loop Continuous Mode (if enabled)
        Orchestrator->>FileProcessor: detect_new_files(landing_prefix)
        FileProcessor->>MinIO: list_files(prefix, recursive=True)
        MinIO-->>FileProcessor: List of CSV files
        FileProcessor-->>Orchestrator: new_files[]
        
        alt New files found
            Orchestrator->>FileProcessor: process_batch(files, configs)
            
            loop For each file in batch
                FileProcessor->>FileProcessor: identify_format(file_path)
                FileProcessor->>SchemaMgr: identify_format(file_path)
                SchemaMgr-->>FileProcessor: format_type (carrier/du)
                
                FileProcessor->>FileProcessor: process_file(file_path, format_type)
                FileProcessor->>SchemaMgr: get_spark_schema(format_type)
                SchemaMgr->>SchemaMgr: load_schema(format_type)
                SchemaMgr-->>FileProcessor: PySpark StructType schema
                
                FileProcessor->>IcebergMgr: table_exists(table_name)
                IcebergMgr->>Spark: SHOW TABLES query
                Spark-->>IcebergMgr: Table list
                IcebergMgr-->>FileProcessor: exists (true/false)
                
                alt Table does not exist
                    FileProcessor->>IcebergMgr: create_table(table_name, schema)
                    IcebergMgr->>Spark: Create empty DataFrame with schema
                    IcebergMgr->>Spark: Write as Iceberg table
                    Spark-->>IcebergMgr: Table created
                end
                
                FileProcessor->>IcebergMgr: load_csv_to_table(csv_path, table_name, schema)
                IcebergMgr->>Spark: Read CSV with schema
                Spark->>MinIO: Read CSV file (s3a://)
                MinIO-->>Spark: CSV data
                Spark-->>IcebergMgr: DataFrame
                IcebergMgr->>Spark: Append DataFrame to Iceberg table
                Spark-->>IcebergMgr: Data appended
                IcebergMgr-->>FileProcessor: Success
                
                FileProcessor->>FileProcessor: move_to_processed(file_path)
                FileProcessor->>MinIO: move_file(source, dest)
                MinIO->>MinIO: Copy object to processed/
                MinIO->>MinIO: Delete source object
                MinIO-->>FileProcessor: File moved
            end
            
            alt Compaction enabled
                Orchestrator->>FileProcessor: should_compact(table_name)
                FileProcessor->>IcebergMgr: compact_table(table_name)
                IcebergMgr->>Spark: CALL system.rewrite_data_files()
                Spark-->>IcebergMgr: Compaction complete
            end
        end
        
        alt Continuous mode
            Orchestrator->>Orchestrator: sleep(check_interval)
        end
    end
    
    Main->>Orchestrator: cleanup()
    Orchestrator->>Spark: stop()
```

## Initialization Sequence

```mermaid
sequenceDiagram
    participant Main as main.py
    participant Orchestrator as PipelineOrchestrator
    participant Config as config.yaml
    participant Spark as SparkSession
    participant MinIO as MinIOClient
    participant SchemaMgr as SchemaManager
    participant IcebergMgr as IcebergManager

    Main->>Orchestrator: __init__(config_path)
    Orchestrator->>Config: Load YAML configuration
    Config-->>Orchestrator: Configuration dict
    
    Orchestrator->>Orchestrator: _setup_logging()
    
    Orchestrator->>Orchestrator: _create_spark_session()
    Orchestrator->>Spark: SparkSession.builder.appName().master()
    loop For each Spark config
        Orchestrator->>Spark: config(key, value)
    end
    Orchestrator->>Spark: getOrCreate()
    Spark-->>Orchestrator: SparkSession instance
    
    Orchestrator->>Orchestrator: _create_minio_client()
    Orchestrator->>MinIO: MinIOClient(endpoint, keys, bucket)
    MinIO->>MinIO: _ensure_bucket_exists()
    MinIO->>MinIO: bucket_exists() or make_bucket()
    MinIO-->>Orchestrator: MinIOClient instance
    
    Orchestrator->>SchemaMgr: SchemaManager()
    SchemaMgr->>SchemaMgr: _initialize_default_schemas()
    SchemaMgr->>SchemaMgr: Create carrier_schema.json if missing
    SchemaMgr->>SchemaMgr: Create du_schema.json if missing
    SchemaMgr-->>Orchestrator: SchemaManager instance
    
    Orchestrator->>Orchestrator: _create_iceberg_manager()
    Orchestrator->>IcebergMgr: IcebergManager(spark, catalog, database)
    IcebergMgr->>IcebergMgr: _ensure_database_exists()
    IcebergMgr->>Spark: CREATE DATABASE IF NOT EXISTS
    Spark-->>IcebergMgr: Database ready
    IcebergMgr-->>Orchestrator: IcebergManager instance
    
    Orchestrator->>Orchestrator: FileProcessor(minio, schema, iceberg)
    Orchestrator-->>Main: Orchestrator ready
```

## File Processing Detail

```mermaid
sequenceDiagram
    participant FP as FileProcessor
    participant MinIO as MinIOClient
    participant SchemaMgr as SchemaManager
    participant IcebergMgr as IcebergManager
    participant Spark as SparkSession

    FP->>MinIO: list_files("landing/Samsung", recursive=True)
    MinIO-->>FP: ["landing/Samsung/carrier/file1.csv", ...]
    
    FP->>FP: Filter CSV files not in processed_files set
    FP-->>FP: new_files[]
    
    loop For each file
        FP->>SchemaMgr: identify_format(file_path)
        SchemaMgr->>SchemaMgr: Check if "carrier" or "du" in path
        SchemaMgr-->>FP: "carrier" or "du"
        
        FP->>SchemaMgr: get_spark_schema(format_type)
        SchemaMgr->>SchemaMgr: load_schema(format_type)
        SchemaMgr->>SchemaMgr: Convert JSON to PySpark StructType
        SchemaMgr-->>FP: StructType schema
        
        FP->>IcebergMgr: table_exists(table_name)
        IcebergMgr->>Spark: SHOW TABLES IN catalog.database
        Spark-->>IcebergMgr: Table list
        IcebergMgr-->>FP: exists boolean
        
        alt Table doesn't exist
            FP->>IcebergMgr: create_table(name, schema, partitions)
            IcebergMgr->>Spark: createDataFrame([], schema)
            IcebergMgr->>Spark: write.format("iceberg").saveAsTable()
            Spark-->>IcebergMgr: Table created
        end
        
        FP->>IcebergMgr: load_csv_to_table(s3_path, table, schema)
        IcebergMgr->>Spark: read.schema(schema).csv(s3_path)
        Spark->>MinIO: Read CSV via s3a://
        MinIO-->>Spark: CSV content
        Spark-->>IcebergMgr: DataFrame
        
        IcebergMgr->>Spark: df.withColumn("_ingestion_timestamp")
        IcebergMgr->>Spark: write.format("iceberg").mode("append")
        Spark-->>IcebergMgr: Data appended
        
        FP->>FP: move_to_processed(file_path)
        FP->>MinIO: move_file(source, dest)
        Note over MinIO: Copy to processed/ + timestamp<br/>Delete source
        MinIO-->>FP: Success
        
        FP->>FP: Add file to processed_files set
    end
```

## Error Handling Flow

```mermaid
sequenceDiagram
    participant FP as FileProcessor
    participant MinIO as MinIOClient
    participant IcebergMgr as IcebergManager
    participant Logger as Logging

    FP->>MinIO: list_files()
    alt MinIO connection error
        MinIO->>Logger: ERROR: Connection failed
        MinIO-->>FP: Empty list []
        FP->>Logger: WARNING: No files detected
    end
    
    FP->>IcebergMgr: load_csv_to_table()
    alt Schema mismatch
        IcebergMgr->>Spark: Read CSV with schema
        Spark->>Logger: ERROR: Column type mismatch
        Spark-->>IcebergMgr: Exception
        IcebergMgr->>Logger: ERROR: Failed to load CSV
        IcebergMgr-->>FP: Exception raised
        FP->>Logger: ERROR: Processing failed
        FP-->>FP: Increment failed counter
    end
    
    alt File move fails
        FP->>MinIO: move_file()
        MinIO->>Logger: ERROR: Move operation failed
        MinIO-->>FP: False
        FP->>Logger: WARNING: File processed but not moved
        Note over FP: Still counts as successful<br/>(data is in table)
    end
```

## Component Interactions Summary

| Component | Responsibilities | Key Methods |
|-----------|-----------------|-------------|
| **main.py** | Entry point, CLI argument parsing | `main()` |
| **PipelineOrchestrator** | Coordinates all components, manages lifecycle | `run_single_cycle()`, `run_continuous()`, `cleanup()` |
| **FileProcessor** | File detection, format identification, batch processing | `detect_new_files()`, `process_batch()`, `move_to_processed()` |
| **MinIOClient** | S3-compatible storage operations | `list_files()`, `move_file()`, `upload_file()`, `download_file()` |
| **SchemaManager** | Schema loading, validation, format identification | `load_schema()`, `get_spark_schema()`, `identify_format()` |
| **IcebergManager** | Iceberg table operations via Spark | `create_table()`, `append_data()`, `load_csv_to_table()`, `compact_table()` |
| **SparkSession** | Spark execution engine, SQL queries | Spark SQL operations, DataFrame operations |

## Data Flow

```
CSV Files (MinIO)
    ↓
File Detection (FileProcessor → MinIOClient)
    ↓
Format Identification (FileProcessor → SchemaManager)
    ↓
Schema Loading (FileProcessor → SchemaManager)
    ↓
Table Creation (if needed) (FileProcessor → IcebergManager → Spark)
    ↓
CSV Reading (IcebergManager → Spark → MinIO via s3a://)
    ↓
Data Transformation (Spark: add ingestion timestamp)
    ↓
Iceberg Table Append (IcebergManager → Spark)
    ↓
File Movement (FileProcessor → MinIOClient)
    ↓
Processed Files (MinIO)
```




