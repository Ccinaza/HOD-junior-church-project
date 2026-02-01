-- Household of David Junior Church Database Schema

-- Drop existing tables if they exist (in correct order due to foreign keys)
DROP TABLE IF EXISTS attendance CASCADE;
DROP TABLE IF EXISTS children CASCADE;
DROP TABLE IF EXISTS parents CASCADE;

-- TABLE: parents
-- Purpose: Store unique parent/guardian information
CREATE TABLE parents (
    parent_id SERIAL PRIMARY KEY,
    
    -- Basic Info
    full_name VARCHAR(200) NOT NULL,
    email VARCHAR(255),
    gender VARCHAR(10) NOT NULL CHECK (gender IN ('Male', 'Female')),  -- Required
    
    -- Contact Info (at least one required)
    phone_number VARCHAR(20),
    secondary_phone_number VARCHAR(20),
    
    -- Church Info
    role_in_church VARCHAR(100),
    department_in_church VARCHAR(100),
    means_of_identification VARCHAR(100),
    address TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- At least one contact method required
    CONSTRAINT check_contact_info CHECK (
        (email IS NOT NULL AND email != '') OR 
        (phone_number IS NOT NULL AND phone_number != '') OR 
        (secondary_phone_number IS NOT NULL AND secondary_phone_number != '')
    )
);

CREATE INDEX idx_parents_name ON parents(full_name);
CREATE INDEX idx_parents_email ON parents(email) WHERE email IS NOT NULL;
CREATE INDEX idx_parents_phone ON parents(phone_number) WHERE phone_number IS NOT NULL;
CREATE INDEX idx_parents_secondary_phone ON parents(secondary_phone_number) WHERE secondary_phone_number IS NOT NULL;
CREATE INDEX idx_parents_active ON parents(is_active);

-- TABLE 2: children
-- Purpose: Store individual child information
CREATE TABLE children (
    child_id SERIAL PRIMARY KEY,
    
    -- Parent Link
    parent_id INTEGER NOT NULL REFERENCES parents(parent_id) ON DELETE CASCADE,
    
    -- Basic Info
    full_name VARCHAR(200) NOT NULL,
    
    -- Age Information (at least ONE must be provided)
    date_of_birth DATE,           -- Optional - if provided, age is calculated
    age INTEGER,                  -- Required if DOB not provided
    age_group VARCHAR(50),        -- Auto-calculated from age
    
    gender VARCHAR(10) NOT NULL CHECK (gender IN ('Male', 'Female')),  -- Required
    
    -- Special Info
    special_needs TEXT,
    allergies TEXT,
    
    -- Relationship (auto-filled if blank based on gender)
    relationship_to_parent VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Must have either DOB or age
    CONSTRAINT check_age_info CHECK (
        date_of_birth IS NOT NULL OR age IS NOT NULL
    )
);

-- Function to calculate age from DOB
CREATE OR REPLACE FUNCTION calculate_age_from_dob(dob DATE)
RETURNS INTEGER AS $$
BEGIN
    RETURN DATE_PART('year', AGE(dob));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to calculate age group
CREATE OR REPLACE FUNCTION calculate_age_group(child_age INTEGER)
RETURNS VARCHAR(50) AS $$
BEGIN
    RETURN CASE
        WHEN child_age BETWEEN 0 AND 2 THEN 'Nursery (0-2 years)'
        WHEN child_age BETWEEN 3 AND 5 THEN 'Kindergarten (3-5 years)'
        WHEN child_age BETWEEN 6 AND 9 THEN 'Primary (6-9 years)'
        WHEN child_age BETWEEN 10 AND 12 THEN 'Juniors (10-12 years)'
        WHEN child_age >= 13 THEN 'Teens (13+ years)'
        ELSE 'Unknown'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Trigger to auto-update age, age_group, and relationship
CREATE OR REPLACE FUNCTION update_child_fields()
RETURNS TRIGGER AS $$
BEGIN
    -- If DOB is provided, calculate age from it
    IF NEW.date_of_birth IS NOT NULL THEN
        NEW.age := calculate_age_from_dob(NEW.date_of_birth);
    END IF;
    
    -- Calculate age_group from age
    IF NEW.age IS NOT NULL THEN
        NEW.age_group := calculate_age_group(NEW.age);
    END IF;
    
    -- Auto-fill relationship if blank based on gender
    IF NEW.relationship_to_parent IS NULL OR NEW.relationship_to_parent = '' THEN
        NEW.relationship_to_parent := CASE 
            WHEN NEW.gender = 'Male' THEN 'Son'
            WHEN NEW.gender = 'Female' THEN 'Daughter'
            ELSE 'Child'
        END;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_child_fields
    BEFORE INSERT OR UPDATE ON children
    FOR EACH ROW
    EXECUTE FUNCTION update_child_fields();

CREATE INDEX idx_children_parent ON children(parent_id);
CREATE INDEX idx_children_name ON children(full_name);
CREATE INDEX idx_children_age_group ON children(age_group);
CREATE INDEX idx_children_active ON children(is_active);


-- TABLE 3: attendance
CREATE TABLE attendance (
    attendance_id SERIAL PRIMARY KEY,
    
    child_id INTEGER NOT NULL REFERENCES children(child_id) ON DELETE CASCADE,
    
    service_name VARCHAR(50) NOT NULL CHECK (
        service_name IN ('First Service', 'Second Service', 'Third Service')
    ),
    attendance_date DATE NOT NULL,
    
    check_in_time TIME,
    check_out_time TIME,
    was_present BOOLEAN DEFAULT TRUE,
    
    checked_in_by VARCHAR(200),
    notes TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_attendance UNIQUE(child_id, service_name, attendance_date),
    CONSTRAINT check_times CHECK (
        check_out_time IS NULL OR check_in_time IS NULL OR check_out_time >= check_in_time
    )
);

CREATE INDEX idx_attendance_child ON attendance(child_id);
CREATE INDEX idx_attendance_date ON attendance(attendance_date);
CREATE INDEX idx_attendance_service ON attendance(service_name);


-- VIEWS
CREATE OR REPLACE VIEW vw_children_with_parents AS
SELECT 
    c.child_id,
    c.full_name AS child_name,
    c.age,
    c.age_group,
    c.gender AS child_gender,
    c.date_of_birth,
    c.special_needs,
    c.relationship_to_parent,
    p.parent_id,
    p.full_name AS parent_name,
    p.email AS parent_email,
    p.phone_number AS parent_phone,
    p.role_in_church,
    p.department_in_church
FROM children c
JOIN parents p ON c.parent_id = p.parent_id
WHERE c.is_active = TRUE AND p.is_active = TRUE;

CREATE OR REPLACE VIEW vw_attendance_detail AS
SELECT 
    a.attendance_id,
    a.attendance_date,
    a.service_name,
    c.child_id,
    c.full_name AS child_name,
    c.age,
    c.age_group,
    p.parent_id,
    p.full_name AS parent_name,
    p.phone_number AS parent_phone
FROM attendance a
JOIN children c ON a.child_id = c.child_id
JOIN parents p ON c.parent_id = p.parent_id;

CREATE OR REPLACE VIEW vw_age_group_stats AS
SELECT 
    age_group,
    COUNT(*) AS total_children,
    ROUND(AVG(age)::numeric, 1) AS avg_age,
    MIN(age) AS min_age,
    MAX(age) AS max_age
FROM children
WHERE is_active = TRUE
GROUP BY age_group
ORDER BY MIN(age);


-- HELPER FUNCTIONS
-- Function: Find or create parent (prevents duplicates)
CREATE OR REPLACE FUNCTION find_or_create_parent(
    p_full_name VARCHAR,
    p_phone VARCHAR,
    p_email VARCHAR DEFAULT NULL,
    p_gender VARCHAR DEFAULT 'Male'
)
RETURNS INTEGER AS $$
DECLARE
    existing_parent_id INTEGER;
BEGIN
    -- Try to find existing parent by phone or email
    SELECT parent_id INTO existing_parent_id
    FROM parents
    WHERE (phone_number = p_phone AND p_phone IS NOT NULL)
       OR (email = p_email AND p_email IS NOT NULL AND p_email != '')
    LIMIT 1;
    
    IF existing_parent_id IS NOT NULL THEN
        RETURN existing_parent_id;
    ELSE
        -- Create new parent
        INSERT INTO parents (full_name, phone_number, email, gender)
        VALUES (p_full_name, p_phone, p_email, p_gender)
        RETURNING parent_id INTO existing_parent_id;
        
        RETURN existing_parent_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function: Find or create child
CREATE OR REPLACE FUNCTION find_or_create_child(
    p_parent_id INTEGER,
    p_full_name VARCHAR,
    p_age INTEGER,
    p_gender VARCHAR
)
RETURNS INTEGER AS $$
DECLARE
    existing_child_id INTEGER;
BEGIN
    -- Try to find existing child by parent_id + name + age
    SELECT child_id INTO existing_child_id
    FROM children
    WHERE parent_id = p_parent_id
      AND UPPER(full_name) = UPPER(p_full_name)
      AND age = p_age
    LIMIT 1;
    
    IF existing_child_id IS NOT NULL THEN
        RETURN existing_child_id;
    ELSE
        -- Create new child
        INSERT INTO children (parent_id, full_name, age, gender)
        VALUES (p_parent_id, p_full_name, p_age, p_gender)
        RETURNING child_id INTO existing_child_id;
        
        RETURN existing_child_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function: Record attendance (idempotent - can run multiple times safely)
CREATE OR REPLACE FUNCTION record_attendance(
    p_child_id INTEGER,
    p_service_name VARCHAR,
    p_attendance_date DATE
)
RETURNS INTEGER AS $$
DECLARE
    new_attendance_id INTEGER;
BEGIN
    INSERT INTO attendance (child_id, service_name, attendance_date, was_present)
    VALUES (p_child_id, p_service_name, p_attendance_date, TRUE)
    ON CONFLICT (child_id, service_name, attendance_date) 
    DO UPDATE SET was_present = TRUE
    RETURNING attendance_id INTO new_attendance_id;
    
    RETURN new_attendance_id;
END;
$$ LANGUAGE plpgsql;


-- UPDATE TIMESTAMP TRIGGER
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_parents_timestamp
    BEFORE UPDATE ON parents FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_children_timestamp
    BEFORE UPDATE ON children FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- SAMPLE DATA VALIDATION QUERIES

-- Check for children without valid age info
SELECT * FROM children WHERE age IS NULL AND date_of_birth IS NULL;

-- Check for parents without contact info
SELECT * FROM parents 
WHERE (email IS NULL OR email = '') 
  AND (phone_number IS NULL OR phone_number = '');

-- Show relationship auto-fill working
SELECT full_name, gender, relationship_to_parent FROM children LIMIT 10;