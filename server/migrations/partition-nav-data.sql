-- NAV Data Partitioning Migration
-- This script partitions the nav_data table by month for improved performance

-- Step 1: Create the partitioned table structure
CREATE TABLE IF NOT EXISTS nav_data_partitioned (
    fund_id INTEGER REFERENCES funds(id),
    nav_date DATE NOT NULL,
    nav_value DECIMAL(12,4) NOT NULL,
    nav_change DECIMAL(12,4),
    nav_change_pct DECIMAL(8,4),
    aum_cr DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT NOW()
) PARTITION BY RANGE (nav_date);

-- Step 2: Create indexes on the partitioned table
CREATE INDEX IF NOT EXISTS idx_nav_data_partitioned_fund_date 
    ON nav_data_partitioned (fund_id, nav_date);

CREATE INDEX IF NOT EXISTS idx_nav_data_partitioned_date 
    ON nav_data_partitioned (nav_date);

-- Step 3: Create monthly partitions from 2006 to 2025
DO $$
DECLARE
    start_date DATE := '2006-01-01';
    end_date DATE := '2025-12-01';
    partition_date DATE;
    partition_name TEXT;
    next_month DATE;
BEGIN
    partition_date := start_date;
    
    WHILE partition_date <= end_date LOOP
        partition_name := 'nav_data_y' || EXTRACT(YEAR FROM partition_date)::TEXT || 
                         'm' || LPAD(EXTRACT(MONTH FROM partition_date)::TEXT, 2, '0');
        next_month := partition_date + INTERVAL '1 month';
        
        -- Check if partition already exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = partition_name
            AND n.nspname = 'public'
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF nav_data_partitioned 
                FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                partition_date,
                next_month
            );
            
            -- Create unique constraint on each partition
            EXECUTE format(
                'CREATE UNIQUE INDEX %I ON %I (fund_id, nav_date)',
                partition_name || '_unique_idx',
                partition_name
            );
        END IF;
        
        partition_date := next_month;
    END LOOP;
END $$;

-- Step 4: Function to automatically create new partitions
CREATE OR REPLACE FUNCTION create_nav_partition_if_not_exists(partition_date DATE)
RETURNS void AS $$
DECLARE
    partition_name TEXT;
    start_of_month DATE;
    start_of_next_month DATE;
BEGIN
    start_of_month := DATE_TRUNC('month', partition_date);
    start_of_next_month := start_of_month + INTERVAL '1 month';
    partition_name := 'nav_data_y' || EXTRACT(YEAR FROM start_of_month)::TEXT || 
                     'm' || LPAD(EXTRACT(MONTH FROM start_of_month)::TEXT, 2, '0');
    
    -- Check if partition exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = partition_name
        AND n.nspname = 'public'
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF nav_data_partitioned 
            FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_of_month,
            start_of_next_month
        );
        
        -- Create unique constraint
        EXECUTE format(
            'CREATE UNIQUE INDEX %I ON %I (fund_id, nav_date)',
            partition_name || '_unique_idx',
            partition_name
        );
        
        RAISE NOTICE 'Created partition % for dates % to %', 
            partition_name, start_of_month, start_of_next_month;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Step 5: Create trigger to auto-create partitions for new data
CREATE OR REPLACE FUNCTION nav_data_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- Ensure partition exists for the insert date
    PERFORM create_nav_partition_if_not_exists(NEW.nav_date);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ensure_nav_partition_exists
    BEFORE INSERT ON nav_data_partitioned
    FOR EACH ROW
    EXECUTE FUNCTION nav_data_insert_trigger();

-- Step 6: Migrate data from old table to new partitioned table
-- This is done in batches to avoid locking issues
DO $$
DECLARE
    batch_size INTEGER := 500000;
    total_rows INTEGER;
    migrated_rows INTEGER := 0;
    current_date DATE;
BEGIN
    -- Get total rows
    SELECT COUNT(*) INTO total_rows FROM nav_data;
    RAISE NOTICE 'Starting migration of % rows', total_rows;
    
    -- Migrate data month by month to minimize memory usage
    FOR current_date IN 
        SELECT DISTINCT DATE_TRUNC('month', nav_date)::DATE 
        FROM nav_data 
        ORDER BY 1
    LOOP
        RAISE NOTICE 'Migrating data for %', current_date;
        
        INSERT INTO nav_data_partitioned 
        SELECT fund_id, nav_date, nav_value, nav_change, nav_change_pct, aum_cr, created_at
        FROM nav_data
        WHERE nav_date >= current_date 
        AND nav_date < current_date + INTERVAL '1 month'
        ON CONFLICT (fund_id, nav_date) DO NOTHING;
        
        GET DIAGNOSTICS migrated_rows = ROW_COUNT;
        RAISE NOTICE 'Migrated % rows for month %', migrated_rows, current_date;
    END LOOP;
    
    RAISE NOTICE 'Migration completed';
END $$;

-- Step 7: Verify migration
SELECT 
    'Original table' as table_name,
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('nav_data')) as size
FROM nav_data
UNION ALL
SELECT 
    'Partitioned table' as table_name,
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('nav_data_partitioned')) as size
FROM nav_data_partitioned;

-- Step 8: After verification, rename tables
-- IMPORTANT: Only run these after confirming migration success
-- ALTER TABLE nav_data RENAME TO nav_data_old;
-- ALTER TABLE nav_data_partitioned RENAME TO nav_data;

-- Step 9: Update statistics for query planner
ANALYZE nav_data_partitioned;