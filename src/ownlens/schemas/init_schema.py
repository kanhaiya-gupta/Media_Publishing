"""
Initialize Database Schema
==========================

Utility script to create all database tables from SQL schema files.
This must be run before loading seed data or sample data.

Usage:
    python -m src.ownlens.schemas.init_schema
    python src/ownlens/schemas/init_schema.py
"""

import sys
import os
import logging
import asyncio
from pathlib import Path
from typing import List, Tuple
from dotenv import load_dotenv
import asyncpg

# Find project root (where development.env is located)
# This script is in src/ownlens/schemas/, so go up 4 levels
script_dir = Path(__file__).parent
project_root = script_dir.parent.parent.parent

# Load environment variables from development.env
env_file = project_root / "development.env"
if env_file.exists():
    load_dotenv(env_file)
else:
    print(f"Warning: development.env not found at {env_file}. Using system environment variables.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if env_file.exists():
    logger.info(f"Loaded environment variables from {env_file}")


# Schema files in execution order
SCHEMA_FILES = [
    "base.sql",              # Foundation - must be first
    "security.sql",          # Security & Access Control
    "audit.sql",             # Audit & Logging
    "compliance.sql",        # Compliance & Data Privacy
    "ml_models.sql",         # ML Model Management
    "data_quality.sql",      # Data Quality
    "configuration.sql",     # Configuration Management
    "customer.sql",          # Customer domain
    "editorial_core.sql",    # Core editorial
    "editorial_content.sql", # Editorial content
    "editorial_media.sql",   # Editorial media
    "company.sql",           # Company domain
]


async def drop_all_tables(conn: asyncpg.Connection) -> bool:
    """
    Drop all tables in reverse dependency order.
    
    This will delete all data! Use with caution.
    
    Args:
        conn: Database connection
        
    Returns:
        True if successful, False otherwise
    """
    logger.warning("=" * 80)
    logger.warning("⚠️  DROPPING ALL TABLES - THIS WILL DELETE ALL DATA!")
    logger.warning("=" * 80)
    
    try:
        # Get all table names from the database
        tables_query = """
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """
        tables = await conn.fetch(tables_query)
        table_names = [row['tablename'] for row in tables]
        
        if not table_names:
            logger.info("No tables found to drop.")
            return True
        
        logger.info(f"Found {len(table_names)} tables to drop.")
        logger.info("")
        
        # Drop all tables (CASCADE will handle dependencies automatically)
        # We use CASCADE so order doesn't matter - PostgreSQL will handle it
        dropped_count = 0
        for table_name in table_names:
            try:
                logger.info(f"Dropping table: {table_name}")
                await conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
                logger.info(f"✅ Dropped: {table_name}")
                dropped_count += 1
            except Exception as e:
                logger.warning(f"⚠️  Could not drop {table_name}: {e}")
        
        logger.info("")
        logger.info(f"Dropped {dropped_count} out of {len(table_names)} tables.")
        
        # Also drop any remaining sequences, types, etc.
        logger.info("")
        logger.info("Cleaning up sequences and types...")
        
        # Drop sequences
        sequences_query = """
            SELECT sequence_name 
            FROM information_schema.sequences 
            WHERE sequence_schema = 'public';
        """
        sequences = await conn.fetch(sequences_query)
        for seq in sequences:
            try:
                await conn.execute(f'DROP SEQUENCE IF EXISTS "{seq["sequence_name"]}" CASCADE;')
            except Exception as e:
                logger.warning(f"⚠️  Could not drop sequence {seq['sequence_name']}: {e}")
        
        logger.info("✅ All tables dropped successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error dropping tables: {e}", exc_info=True)
        return False


async def execute_sql_file(conn: asyncpg.Connection, file_path: Path) -> Tuple[bool, str]:
    """
    Execute a SQL file.
    
    Args:
        conn: Database connection
        file_path: Path to SQL file
        
    Returns:
        Tuple of (success, message)
    """
    try:
        logger.info(f"Executing: {file_path.name}")
        
        # Read SQL file
        sql_content = file_path.read_text(encoding='utf-8')
        
        # Execute SQL (asyncpg supports multi-statement execution)
        # Use execute_many or execute to handle errors gracefully
        try:
            await conn.execute(sql_content)
        except asyncpg.exceptions.DuplicateObjectError as e:
            # Object already exists - this is OK for idempotent schema creation
            logger.warning(f"⚠️  Object already exists (skipping): {str(e)}")
            return True, f"Skipped (already exists): {file_path.name}"
        
        logger.info(f"✅ Successfully executed: {file_path.name}")
        return True, f"Successfully executed {file_path.name}"
        
    except Exception as e:
        error_msg = f"Error executing {file_path.name}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg


async def init_schema(drop_existing: bool = False) -> bool:
    """
    Initialize database schema by executing all SQL schema files.
    
    Args:
        drop_existing: If True, drop all existing tables before creating schema.
                      WARNING: This will delete all data!
    
    Returns:
        True if all schemas were created successfully, False otherwise
    """
    # Get database connection parameters
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "ownlens")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "password")
    
    logger.info("=" * 80)
    logger.info("Database Schema Initialization")
    logger.info("=" * 80)
    logger.info(f"Database: {db_name}")
    logger.info(f"Host: {db_host}:{db_port}")
    logger.info(f"User: {db_user}")
    logger.info("=" * 80)
    
    # Get schema directory (where this script is located)
    schema_dir = script_dir
    
    if not schema_dir.exists():
        logger.error(f"Schema directory not found: {schema_dir}")
        return False
    
    logger.info(f"Schema directory: {schema_dir}")
    logger.info("")
    
    try:
        # Connect to database
        logger.info("Connecting to PostgreSQL...")
        conn = await asyncpg.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        logger.info("✅ Connected to PostgreSQL")
        logger.info("")
        
        # Drop existing tables if requested
        if drop_existing:
            success = await drop_all_tables(conn)
            if not success:
                logger.error("❌ Failed to drop existing tables. Aborting.")
                await conn.close()
                return False
            logger.info("")
        
        # Execute schema files in order
        results = []
        for schema_file in SCHEMA_FILES:
            file_path = schema_dir / schema_file
            
            if not file_path.exists():
                logger.warning(f"⚠️  Schema file not found: {file_path}")
                results.append((False, f"File not found: {schema_file}"))
                continue
            
            success, message = await execute_sql_file(conn, file_path)
            results.append((success, message))
            logger.info("")
        
        # Close connection
        await conn.close()
        logger.info("Disconnected from PostgreSQL")
        
        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("Schema Initialization Summary")
        logger.info("=" * 80)
        
        success_count = sum(1 for success, _ in results if success)
        failure_count = len(results) - success_count
        
        for i, (success, message) in enumerate(results, 1):
            status = "✅" if success else "❌"
            logger.info(f"{status} [{i}/{len(results)}] {message}")
        
        logger.info("")
        logger.info(f"Total: {success_count} succeeded, {failure_count} failed")
        logger.info("=" * 80)
        
        if failure_count > 0:
            logger.error("⚠️  Some schema files failed to execute. Please check the errors above.")
            return False
        else:
            logger.info("✅ All schema files executed successfully!")
            return True
        
    except asyncpg.exceptions.InvalidPasswordError:
        logger.error("❌ Invalid database password")
        return False
    except asyncpg.exceptions.InvalidCatalogNameError:
        logger.error(f"❌ Database '{db_name}' does not exist. Please create it first.")
        logger.info(f"   Run: CREATE DATABASE {db_name};")
        return False
    except asyncpg.exceptions.ConnectionDoesNotExistError:
        logger.error(f"❌ Cannot connect to PostgreSQL at {db_host}:{db_port}")
        logger.info("   Please ensure PostgreSQL is running and accessible.")
        return False
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}", exc_info=True)
        return False


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Initialize database schema from SQL files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize schema (skip if tables exist)
  python -m src.ownlens.schemas.init_schema
  
  # Drop all tables and recreate schema (WARNING: deletes all data!)
  python -m src.ownlens.schemas.init_schema --drop-existing
        """
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop all existing tables before creating schema. WARNING: This will delete all data!"
    )
    
    args = parser.parse_args()
    
    # Check if drop_existing is set via environment variable
    drop_existing = args.drop_existing or os.getenv("DROP_EXISTING_TABLES", "").lower() in ("true", "1", "yes")
    
    if drop_existing:
        logger.warning("=" * 80)
        logger.warning("⚠️  WARNING: --drop-existing flag is set!")
        logger.warning("⚠️  This will DELETE ALL DATA in the database!")
        logger.warning("=" * 80)
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() not in ("yes", "y"):
            logger.info("Aborted by user.")
            sys.exit(0)
    
    try:
        success = asyncio.run(init_schema(drop_existing=drop_existing))
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Script failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

