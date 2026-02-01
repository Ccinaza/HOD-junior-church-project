"""
Junior Church Attendance ETL (Initial Data Load)

Purpose:
- Normalize raw multi-sheet attendance data into relational tables
- Deduplicate parents and children across services
- Generate one attendance record per child per service

Scope:
- One-time historical data cleanup and export
- Outputs clean CSVs for database import
"""


import pandas as pd
from datetime import datetime

# Load Excel file
excel_file = 'junior_church_dummy_data.xlsx'
sheets = pd.read_excel(excel_file, sheet_name=None)

# For this example, assuming all sheets(services) are from the same date
# In production, we'd have actual dates
ATTENDANCE_DATE = '2026-01-26' # YYYY-MM-DD

print(f"\nProcessing attendance for date: {ATTENDANCE_DATE}")

# STEP 1: CREATE UNIQUE PARENTS (dedupe across sheets)
print("\n1. Creating unique parents...")

all_data = []
for sheet_name, df in sheets.items():
    all_data.append(df)
combined_df = pd.concat(all_data, ignore_index=True)

parent_cols = ['ID', 'Full Name', 'Email', 'Gender', 'Role In Church',
               'Department In Church', 'Phone Number', 'Secondary Phone Number', 'Address']

parents_df = combined_df[parent_cols].copy()
unique_parents = parents_df.drop_duplicates(subset=['ID'], keep='first')

unique_parents['Email'] = unique_parents['Email'].fillna('')
unique_parents['Phone Number'] = unique_parents['Phone Number'].astype(str).replace('nan', '')
unique_parents['Secondary Phone Number'] = unique_parents['Secondary Phone Number'].astype(str).replace('nan', '')

unique_parents.columns = ['original_id', 'full_name', 'email', 'gender', 'role_in_church',
                          'department_in_church', 'phone_number', 'secondary_phone_number', 'address']

unique_parents = unique_parents.sort_values('original_id').reset_index(drop=True)
unique_parents.insert(0, 'parent_id', range(1, len(unique_parents) + 1))

parent_mapping = dict(zip(unique_parents['original_id'], unique_parents['parent_id']))

print(f"  done creating {len(unique_parents)} unique parents")

# STEP 2: CREATE UNIQUE CHILDREN (dedupe across sheets)
print("\n2. Creating unique children...")

all_children = []

for sheet_name, df in sheets.items():
    for _, row in df.iterrows():
        parent_original_id = row['ID']
        parent_id = parent_mapping.get(parent_original_id)
        
        if parent_id is None:
            continue
        
        for child_num in range(1, 4):
            child_name = row.get(f'Full Name of Child {child_num}')
            
            if pd.isna(child_name) or str(child_name).strip() == '':
                continue
            
            child_age = int(row.get(f'Age of Child {child_num}', 0)) if not pd.isna(row.get(f'Age of Child {child_num}')) else 0
            
            child_record = {
                'parent_id': parent_id,
                'full_name': str(child_name).strip(),
                'age': child_age,
                'gender': str(row.get(f'Gender of Child {child_num}', 'Unknown')).strip(),
                'special_needs': None if pd.isna(row.get(f'Special Needs of Child {child_num}')) else str(row.get(f'Special Needs of Child {child_num}')).strip(),
                'relationship_to_parent': 'Child' if pd.isna(row.get(f'Relationship With Child {child_num}')) else str(row.get(f'Relationship With Child {child_num}')).strip(),
                'dedupe_key': f"{parent_id}_{str(child_name).strip().upper()}_{child_age}"
            }
            
            all_children.append(child_record)

children_df = pd.DataFrame(all_children)
print(f"   Before deduplication: {len(children_df)} child records")

# Deduplicate children
children_unique = children_df.drop_duplicates(subset=['dedupe_key'], keep='first').copy()
children_unique = children_unique.drop(columns=['dedupe_key']).reset_index(drop=True)
children_unique.insert(0, 'child_id', range(1, len(children_unique) + 1))

print(f"   After deduplication: {len(children_unique)} unique children")

# Create reverse mapping: (parent_id + name + age) → child_id
child_lookup = {}
for _, child in children_unique.iterrows():
    key = f"{child['parent_id']}_{child['full_name'].upper()}_{child['age']}"
    child_lookup[key] = child['child_id']


# STEP 3: CREATE ATTENDANCE RECORDS (one per service per child)
print("\n3. Creating attendance records...")

all_attendance = []

for sheet_name, df in sheets.items():
    service_name = sheet_name  # "First Service", "Second Service", etc.
    
    for _, row in df.iterrows():
        parent_original_id = row['ID']
        parent_id = parent_mapping.get(parent_original_id)
        
        if parent_id is None:
            continue
        
        for child_num in range(1, 4):
            child_name = row.get(f'Full Name of Child {child_num}')
            
            if pd.isna(child_name) or str(child_name).strip() == '':
                continue
            
            child_age = int(row.get(f'Age of Child {child_num}', 0)) if not pd.isna(row.get(f'Age of Child {child_num}')) else 0
            
            # Find the child_id from our lookup
            lookup_key = f"{parent_id}_{str(child_name).strip().upper()}_{child_age}"
            child_id = child_lookup.get(lookup_key)
            
            if child_id is None:
                print(f"Warning: Could not find child_id for {lookup_key}")
                continue
            
            # Check if child was present (check-in column has value)
            checkin_col = f'Child {child_num} (check-in)'
            checkout_col = f'Child {child_num} (check-out)'
            
            was_present = False
            if checkin_col in row and not pd.isna(row[checkin_col]) and row[checkin_col] == 1:
                was_present = True
            
            # Only create attendance record if child was present
            if was_present:
                attendance_record = {
                    'child_id': child_id,
                    'service_name': service_name,
                    'attendance_date': ATTENDANCE_DATE,
                    'check_in_time': None,  # Time not available in current data
                    'check_out_time': None,
                    'was_present': True
                }
                
                all_attendance.append(attendance_record)

attendance_df = pd.DataFrame(all_attendance)

# Add attendance_id
if len(attendance_df) > 0:
    attendance_df.insert(0, 'attendance_id', range(1, len(attendance_df) + 1))
    print(f"Done creating {len(attendance_df)} attendance records")
else:
    print(f" No attendance records (check-in data may be missing)")


# STEP 4: EXPORT FILES
print("\n4. Exporting files...")

parents_export = unique_parents.drop(columns=['original_id'])
parents_export.to_csv('parents_final.csv', index=False)
print(f" Exported parents_final.csv ({len(parents_export)} records)")

children_unique.to_csv('children_final.csv', index=False)
print(f" Exported children_final.csv ({len(children_unique)} records)")

if len(attendance_df) > 0:
    attendance_df.to_csv('attendance_final.csv', index=False)
    print(f" Exported attendance_final.csv ({len(attendance_df)} records)")

# VALIDATION
print("\n5. Validation:")

if len(attendance_df) > 0:
    # Check all child_ids in attendance exist in children
    attendance_child_ids = set(attendance_df['child_id'])
    children_ids = set(children_unique['child_id'])
    orphaned = attendance_child_ids - children_ids
    
    if len(orphaned) == 0:
        print(f"All attendance records link to valid children")
    else:
        print(f"ERROR: {len(orphaned)} attendance records reference non-existent children")

# Check all parent_ids in children exist in parents
children_parent_ids = set(children_unique['parent_id'])
parent_ids = set(parents_export['parent_id'])
orphaned_children = children_parent_ids - parent_ids

if len(orphaned_children) == 0:
    print(f"All children link to valid parents")
else:
    print(f"ERROR: {len(orphaned_children)} children reference non-existent parents")
