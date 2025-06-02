-- Database Integrity Fixes and Authentic Data Validation
-- Comprehensive solution for logical gaps and inconsistencies

-- 1. Add missing foreign key constraints
ALTER TABLE fund_scores 
ADD CONSTRAINT fk_fund_scores_fund_id 
FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE;

ALTER TABLE nav_data 
ADD CONSTRAINT fk_nav_data_fund_id 
FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE;

ALTER TABLE risk_analytics 
ADD CONSTRAINT fk_risk_analytics_fund_id 
FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE;

ALTER TABLE portfolio_holdings 
ADD CONSTRAINT fk_portfolio_holdings_fund_id 
FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE;

-- 2. Add unique constraints for data integrity
ALTER TABLE fund_scores 
ADD CONSTRAINT uk_fund_scores_fund_date 
UNIQUE (fund_id, score_date);

ALTER TABLE nav_data 
ADD CONSTRAINT uk_nav_data_fund_date 
UNIQUE (fund_id, nav_date);

ALTER TABLE risk_analytics 
ADD CONSTRAINT uk_risk_analytics_fund_date 
UNIQUE (fund_id, calculation_date);

-- 3. Add data validation constraints
ALTER TABLE funds 
ADD CONSTRAINT chk_expense_ratio_range 
CHECK (expense_ratio IS NULL OR (expense_ratio >= 0 AND expense_ratio <= 10));

ALTER TABLE funds 
ADD CONSTRAINT chk_minimum_investment_positive 
CHECK (minimum_investment IS NULL OR minimum_investment > 0);

ALTER TABLE nav_data 
ADD CONSTRAINT chk_nav_value_positive 
CHECK (nav_value > 0);

ALTER TABLE nav_data 
ADD CONSTRAINT chk_nav_date_not_future 
CHECK (nav_date <= CURRENT_DATE);

ALTER TABLE fund_scores 
ADD CONSTRAINT chk_total_score_range 
CHECK (total_score >= 0 AND total_score <= 100);

ALTER TABLE fund_scores 
ADD CONSTRAINT chk_quartile_range 
CHECK (quartile >= 1 AND quartile <= 4);

-- 4. Add NOT NULL constraints for essential fields
ALTER TABLE funds 
ALTER COLUMN scheme_code SET NOT NULL;

ALTER TABLE funds 
ALTER COLUMN fund_name SET NOT NULL;

ALTER TABLE funds 
ALTER COLUMN amc_name SET NOT NULL;

ALTER TABLE funds 
ALTER COLUMN category SET NOT NULL;

-- 5. Create data quality monitoring table
CREATE TABLE IF NOT EXISTS data_quality_audit (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    check_type TEXT NOT NULL,
    issue_count INTEGER NOT NULL,
    issue_description TEXT,
    check_date DATE NOT NULL DEFAULT CURRENT_DATE,
    severity TEXT CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 6. Create data lineage tracking
CREATE TABLE IF NOT EXISTS data_lineage (
    id SERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    transformation_type TEXT NOT NULL,
    data_source TEXT NOT NULL, -- AMFI, NSE, BSE, etc.
    last_update TIMESTAMP NOT NULL,
    record_count INTEGER,
    data_freshness_hours INTEGER,
    validation_status TEXT CHECK (validation_status IN ('PASSED', 'FAILED', 'PENDING')),
    created_at TIMESTAMP DEFAULT NOW()
);

-- 7. Populate initial data quality audit
INSERT INTO data_quality_audit (table_name, check_type, issue_count, issue_description, severity)
SELECT 
    'funds' as table_name,
    'MISSING_FUND_MANAGERS' as check_type,
    COUNT(*) as issue_count,
    'Funds requiring authentic fund manager data from AMC sources' as issue_description,
    'HIGH' as severity
FROM funds 
WHERE fund_manager IS NULL OR fund_manager LIKE '%Requires%' OR fund_manager LIKE '%Required%'
HAVING COUNT(*) > 0;

INSERT INTO data_quality_audit (table_name, check_type, issue_count, issue_description, severity)
SELECT 
    'funds' as table_name,
    'MISSING_EXPENSE_RATIOS' as check_type,
    COUNT(*) as issue_count,
    'Funds missing expense ratio data - requires collection from authentic AMC sources' as issue_description,
    'MEDIUM' as severity
FROM funds 
WHERE expense_ratio IS NULL
HAVING COUNT(*) > 0;

-- 8. Create authentic data validation functions
CREATE OR REPLACE FUNCTION validate_nav_authenticity()
RETURNS TABLE (
    fund_id INTEGER,
    nav_date DATE,
    nav_value NUMERIC,
    validation_status TEXT,
    issue_description TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        n.fund_id,
        n.nav_date,
        n.nav_value,
        CASE 
            WHEN n.nav_value <= 0 THEN 'FAILED'
            WHEN n.nav_date > CURRENT_DATE THEN 'FAILED'
            WHEN n.nav_date < f.inception_date THEN 'FAILED'
            ELSE 'PASSED'
        END as validation_status,
        CASE 
            WHEN n.nav_value <= 0 THEN 'Invalid NAV value - must be positive'
            WHEN n.nav_date > CURRENT_DATE THEN 'Future-dated NAV not allowed'
            WHEN n.nav_date < f.inception_date THEN 'NAV date before fund inception'
            ELSE 'Valid authentic NAV data'
        END as issue_description
    FROM nav_data n
    JOIN funds f ON n.fund_id = f.id
    WHERE n.nav_value <= 0 
       OR n.nav_date > CURRENT_DATE 
       OR (f.inception_date IS NOT NULL AND n.nav_date < f.inception_date);
END;
$$ LANGUAGE plpgsql;

-- 9. Create market data validation function
CREATE OR REPLACE FUNCTION validate_market_indices_authenticity()
RETURNS TABLE (
    index_name TEXT,
    index_date DATE,
    close_value NUMERIC,
    validation_status TEXT,
    data_source_required TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.index_name,
        m.index_date,
        m.close_value,
        CASE 
            WHEN m.close_value <= 0 THEN 'FAILED'
            WHEN m.index_date > CURRENT_DATE THEN 'FAILED'
            WHEN m.index_date < CURRENT_DATE - INTERVAL '10 days' THEN 'STALE'
            ELSE 'PASSED'
        END as validation_status,
        CASE 
            WHEN m.index_name LIKE 'NIFTY%' THEN 'NSE Official API Required'
            WHEN m.index_name LIKE 'SENSEX%' OR m.index_name LIKE 'BSE%' THEN 'BSE Official API Required'
            ELSE 'Authorized Market Data Provider Required'
        END as data_source_required
    FROM market_indices m
    WHERE m.close_value <= 0 
       OR m.index_date > CURRENT_DATE
       OR m.index_date < CURRENT_DATE - INTERVAL '10 days';
END;
$$ LANGUAGE plpgsql;

-- 10. Create comprehensive data freshness monitoring
CREATE OR REPLACE FUNCTION monitor_data_freshness()
RETURNS TABLE (
    data_category TEXT,
    last_update DATE,
    days_since_update INTEGER,
    status TEXT,
    action_required TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'NAV_DATA' as data_category,
        MAX(nav_date) as last_update,
        CURRENT_DATE - MAX(nav_date) as days_since_update,
        CASE 
            WHEN CURRENT_DATE - MAX(nav_date) <= 2 THEN 'FRESH'
            WHEN CURRENT_DATE - MAX(nav_date) <= 7 THEN 'STALE'
            ELSE 'CRITICAL'
        END as status,
        CASE 
            WHEN CURRENT_DATE - MAX(nav_date) > 2 THEN 'Implement AMFI daily NAV feed'
            ELSE 'Data current'
        END as action_required
    FROM nav_data
    
    UNION ALL
    
    SELECT 
        'MARKET_INDICES',
        MAX(index_date),
        CURRENT_DATE - MAX(index_date),
        CASE 
            WHEN CURRENT_DATE - MAX(index_date) <= 1 THEN 'FRESH'
            WHEN CURRENT_DATE - MAX(index_date) <= 3 THEN 'STALE'
            ELSE 'CRITICAL'
        END,
        CASE 
            WHEN CURRENT_DATE - MAX(index_date) > 1 THEN 'Setup NSE/BSE real-time feeds'
            ELSE 'Data current'
        END
    FROM market_indices
    
    UNION ALL
    
    SELECT 
        'FUND_SCORES',
        MAX(score_date),
        CURRENT_DATE - MAX(score_date),
        CASE 
            WHEN CURRENT_DATE - MAX(score_date) <= 7 THEN 'FRESH'
            WHEN CURRENT_DATE - MAX(score_date) <= 30 THEN 'STALE'
            ELSE 'CRITICAL'
        END,
        CASE 
            WHEN CURRENT_DATE - MAX(score_date) > 7 THEN 'Run authentic scoring update'
            ELSE 'Scoring current'
        END
    FROM fund_scores;
END;
$$ LANGUAGE plpgsql;

-- 11. Create indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_nav_data_fund_date ON nav_data(fund_id, nav_date DESC);
CREATE INDEX IF NOT EXISTS idx_fund_scores_score_date ON fund_scores(score_date DESC);
CREATE INDEX IF NOT EXISTS idx_risk_analytics_calc_date ON risk_analytics(calculation_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_indices_date ON market_indices(index_date DESC);
CREATE INDEX IF NOT EXISTS idx_funds_category ON funds(category);
CREATE INDEX IF NOT EXISTS idx_funds_amc_name ON funds(amc_name);

-- 12. Cleanup orphaned records
DELETE FROM fund_scores WHERE fund_id NOT IN (SELECT id FROM funds);
DELETE FROM nav_data WHERE fund_id NOT IN (SELECT id FROM funds);
DELETE FROM risk_analytics WHERE fund_id NOT IN (SELECT id FROM funds);
DELETE FROM portfolio_holdings WHERE fund_id NOT IN (SELECT id FROM funds);

COMMIT;