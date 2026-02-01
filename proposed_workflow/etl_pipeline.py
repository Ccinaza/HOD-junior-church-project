"""
Weekly Junior Church Attendance ETL (CSV-based)

Description:
- Extracts weekly attendance data from a public Google Sheets CSV export
- Cleans and normalizes parent, child, and attendance records
- Loads data into a PostgreSQL database (Supabase)

Design Notes:
- Uses CSV export to avoid Google OAuth complexity
- Intended for scheduled, non-interactive execution (cron / CI / server)
"""

import pandas as pd
import psycopg2
import logging
from datetime import date

# CONFIGURATION
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID_HERE"
CSV_EXPORT_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv"

DB_CONFIG = {
    "host": "db.jpzgfrmthhillxoncmgs.supabase.co",
    "database": "postgres",
    "user": "postgres",
    "password": "HODTech2026",  # move to env in real setup
    "port": 5432
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# EXTRACT
def extract_attendance():
    """Download attendance data from public Google Sheets CSV"""
    logger.info("Extracting attendance CSV from Google Sheets...")
    try:
        df = pd.read_csv(CSV_EXPORT_URL)
        logger.info(f"Extracted {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to extract CSV: {e}")
        raise

# TRANSFORM + LOAD
def process_attendance(df, conn):
    """
    Process each form submission and update database
    
    Logic:
    1. For each submission, find or create parent by phone
    2. For each child in submission, find or create child
    3. Record attendance for each child
    """
    cursor = conn.cursor()
    stats = {
        'total_submissions': len(df),
        'new_parents': 0,
        'existing_parents': 0,
        'new_children': 0,
        'existing_children': 0,
        'attendance_recorded': 0,
        'errors': 0
    }

    for idx, row in df.iterrows():
        try:
            parent_name = str(row.get("Your Name", "")).strip()
            parent_phone = str(row.get("Your Phone", "")).strip()
            parent_gender = str(row.get("Your Gender", "Male")).strip()

            # Validate required fields
            if not parent_phone or not parent_name:
                logger.warning(f"Row {idx}: Missing parent name or phone, skipping")
                stats['errors'] += 1
                continue

            # Validate gender value
            if parent_gender not in ['Male', 'Female']:
                logger.warning(f"Row {idx}: Invalid gender '{parent_gender}', defaulting to 'Male'")
                parent_gender = 'Male'

            # Extract attendance date from form timestamp
            timestamp = row.get('Timestamp', '')
            if timestamp:
                attendance_date = pd.to_datetime(timestamp).date()
            else:
                attendance_date = date.today()

            logger.info(f"Processing submission {idx+1}/{len(df)}: {parent_name}")

            # PARENT: Find or create
            cursor.execute(
                "SELECT parent_id FROM parents WHERE phone_number = %s",
                (parent_phone,)
            )
            result = cursor.fetchone()

            if result:
                # EXISTING PARENT
                parent_id = result[0]
                stats['existing_parents'] += 1
                logger.info(f" Found existing parent (ID={parent_id})")
            else:
                # NEW PARENT - Create record
                cursor.execute(
                    """
                    INSERT INTO parents (full_name, phone_number, gender)
                    VALUES (%s, %s, %s)
                    RETURNING parent_id
                    """,
                    (parent_name, parent_phone, parent_gender)
                )
                parent_id = cursor.fetchone()[0]
                stats['new_parents'] += 1
                logger.info(f" Created new parent (ID={parent_id}): {parent_name}, {parent_gender}")

            # CHILDREN + ATTENDANCE
            for child_num in range(1, 4):
                child_name = str(row.get(f"Child {child_num} Name", "")).strip()
                
                # Skip if no child name provided
                if not child_name:
                    continue

                # Extract child details
                child_age = row.get(f"Child {child_num} Age", 0)
                child_gender = str(row.get(f"Child {child_num} Gender", "Male")).strip()
                
                # Validate and clean
                try:
                    child_age = int(child_age) if not pd.isna(child_age) else 0
                except:
                    child_age = 0
                
                if child_gender not in ['Male', 'Female']:
                    child_gender = 'Male'
                
                # Extract service name
                service_name = str(row.get("Which Service", "First Service")).strip()

                # CHILD: Find or create
                cursor.execute(
                    """
                    SELECT child_id FROM children
                    WHERE parent_id = %s
                      AND UPPER(full_name) = UPPER(%s)
                      AND age = %s
                    """,
                    (parent_id, child_name, child_age)
                )
                result = cursor.fetchone()

                if result:
                    # EXISTING CHILD
                    child_id = result[0]
                    stats['existing_children'] += 1
                    logger.info(f" Found existing child (ID={child_id}): {child_name}")
                else:
                # NEW CHILD - Create record
                    cursor.execute(
                        """
                        INSERT INTO children (parent_id, full_name, age, gender)
                        VALUES (%s, %s, %s, %s)
                        RETURNING child_id
                        """,
                        (parent_id, child_name, child_age, child_gender)
                    )
                    child_id = cursor.fetchone()[0]
                    stats['new_children'] += 1
                    logger.info(f" Created new child (ID={child_id}): {child_name}, age {child_age}, {child_gender}")


                # ATTENDANCE: Record (idempotent)
                cursor.execute(
                    """
                    INSERT INTO attendance (child_id, service_name, attendance_date, was_present)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (child_id, service_name, attendance_date) DO NOTHING
                    """,
                    (child_id, service_name, attendance_date)
                )
                
                # Check if row was actually inserted (not a duplicate)
                if cursor.rowcount > 0:
                    stats['attendance_recorded'] += 1
                    logger.info(f"  Recorded attendance: {service_name} on {attendance_date}")
                else:
                    logger.info(f"  Attendance already recorded (duplicate)")


        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            stats['errors'] += 1
            continue

    conn.commit()
    cursor.close()
    
    # Log summary
    logger.info("="*60)
    logger.info("ETL Summary:")
    logger.info(f"  New parents: {stats['new_parents']}")
    logger.info(f"  New children: {stats['new_children']}")
    logger.info(f"  Attendance recorded: {stats['attendance_recorded']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("="*60)

# MAIN
def main():
    """Main ETL execution"""
    logger.info("="*60)
    logger.info("Weekly Attendance ETL Started (CSV mode)")
    logger.info("="*60)
    
    try:
        # Extract
        df = extract_attendance()
        
        if len(df) == 0:
            logger.info("No submissions found. Exiting.")
            return

        # Connect to database
        logger.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Transform & Load
        process_attendance(df, conn)
        
        # Cleanup
        conn.close()
        logger.info("ETL completed successfully")
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise

if __name__ == "__main__":
    main()