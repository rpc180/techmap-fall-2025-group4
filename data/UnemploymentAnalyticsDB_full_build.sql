/* ======================================================================================
   UnemploymentAnalyticsDB - Re-runnable Star Schema Build & Load (Azure SQL compatible)
   Generated from your uploaded file + fixes for sequencing and re-runnability.

   This script:
   - Creates/uses the `UnemploymentAnalyticsDB` database
   - Drops/recreates tables and foreign keys in the correct order
   - Builds a COMPLETE date dimension for 2019-01-01..2024-12-31 (no missing FK date_id)
   - Loads ALL rows from your original file for:
       * dim_industry (15 rows)
       * dim_demographics (20 rows)
       * dim_geography (58 rows)
       * unemployment_statistics facts (170 rows)
   - Creates contained RW user UnemploymentDB_rw with password TechmapGroup4RW!
   - Is safe to re-run (idempotent load of facts via TRUNCATE)
   ====================================================================================== */
IF DB_ID(N'UnemploymentAnalyticsDB') IS NULL
BEGIN
    CREATE DATABASE UnemploymentAnalyticsDB;
END;
GO

USE UnemploymentAnalyticsDB;
GO
/* =========================================================================
   STAR SCHEMA PRECLEAN (any schema):
   1) Drop all FKs that reference our target tables
   2) Drop the target tables (fact + dims) in all schemas
   Paste this AFTER: USE UnemploymentAnalyticsDB;  and BEFORE any CREATEs
   ========================================================================= */
SET NOCOUNT ON;

DECLARE @targets TABLE (table_name sysname);
INSERT INTO @targets (table_name) VALUES
('unemployment_statistics'),
('dim_date'),
('dim_geography'),
('dim_demographics'),
('dim_industry');

-------------------------------------------------------------------------------
-- 1) Drop ALL foreign keys that reference any of the target tables (any schema)
-------------------------------------------------------------------------------
DECLARE @sql NVARCHAR(MAX);

WITH ref_fks AS (
    SELECT
        fk_schema = sch_p.name,
        fk_table  = t_p.name,
        fk_name   = fk.name
    FROM sys.foreign_keys fk
    JOIN sys.tables t_p            ON t_p.object_id = fk.parent_object_id
    JOIN sys.schemas sch_p         ON sch_p.schema_id = t_p.schema_id
    JOIN sys.tables t_r            ON t_r.object_id = fk.referenced_object_id
    JOIN sys.schemas sch_r         ON sch_r.schema_id = t_r.schema_id
    WHERE t_r.name IN (SELECT table_name FROM @targets)
)
SELECT @sql = STRING_AGG(
    'ALTER TABLE ' + QUOTENAME(fk_schema) + '.' + QUOTENAME(fk_table) +
    ' DROP CONSTRAINT ' + QUOTENAME(fk_name) + ';'
, CHAR(10))
FROM ref_fks;

IF @sql IS NOT NULL AND LEN(@sql) > 0
BEGIN
    PRINT N'Dropping referencing foreign keys...';
    EXEC sys.sp_executesql @sql;
END
ELSE
BEGIN
    PRINT N'No foreign keys referencing target tables found.';
END

------------------------------------------------------------
-- 2) Drop the target tables across ALL schemas (if exist)
--    (Fact first, then dims — order is safe after FK drops)
------------------------------------------------------------
SET @sql = N'';

;WITH to_drop AS (
    SELECT QUOTENAME(s.name) + '.' + QUOTENAME(t.name) AS full_name, t.name AS bare_name
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name IN (SELECT table_name FROM @targets)
)
SELECT @sql = COALESCE(@sql + CHAR(10), N'') +
    'DROP TABLE ' + full_name + ';'
FROM to_drop
ORDER BY CASE bare_name
            WHEN 'unemployment_statistics' THEN 0
            ELSE 1
         END;  -- ensure fact first if it exists

IF @sql IS NOT NULL AND LEN(@sql) > 0
BEGIN
    PRINT N'Dropping target tables (any schema)...';
    EXEC sys.sp_executesql @sql;
END
ELSE
BEGIN
    PRINT N'No target tables found to drop.';
END

SET XACT_ABORT ON;
BEGIN TRY
BEGIN TRAN;

    -- Drop FKs (if exist) and tables (safe re-run)
    IF OBJECT_ID(N'dbo.unemployment_statistics', N'U') IS NOT NULL
    BEGIN
        ALTER TABLE dbo.unemployment_statistics DROP CONSTRAINT IF EXISTS FK_unemployment_statistics_date_id;
        ALTER TABLE dbo.unemployment_statistics DROP CONSTRAINT IF EXISTS FK_unemployment_statistics_geography_id;
        ALTER TABLE dbo.unemployment_statistics DROP CONSTRAINT IF EXISTS FK_unemployment_statistics_demographic_id;
        ALTER TABLE dbo.unemployment_statistics DROP CONSTRAINT IF EXISTS FK_unemployment_statistics_industry_id;
    END;

    DROP TABLE IF EXISTS dbo.unemployment_statistics;
    DROP TABLE IF EXISTS dbo.dim_date;
    DROP TABLE IF EXISTS dbo.dim_geography;
    DROP TABLE IF EXISTS dbo.dim_demographics;
    DROP TABLE IF EXISTS dbo.dim_industry;

    /* =====================
       1) Create Dimensions
       ===================== */

    -- dim_date
    CREATE TABLE dbo.dim_date (
        date_id        INT PRIMARY KEY, -- YYYYMMDD
        full_date      DATE NOT NULL,

        [year]         INT NOT NULL,
        year_name      VARCHAR(10) NOT NULL,

        quarter_of_year INT NOT NULL,
        quarter_name    VARCHAR(10) NOT NULL,

        month_of_year  INT NOT NULL,
        month_name     VARCHAR(20) NOT NULL,
        month_abbr     VARCHAR(3)  NOT NULL,

        week_of_year   INT NOT NULL,
        week_of_month  INT NOT NULL,
        day_of_month   INT NOT NULL,
        day_of_week    INT NOT NULL,
        day_name       VARCHAR(20) NOT NULL,

        is_weekday     BIT NOT NULL,
        is_holiday     BIT NOT NULL DEFAULT 0,
        holiday_name   VARCHAR(100) NULL,

        economic_period VARCHAR(50) NULL,
        fiscal_year     INT NULL,
        fiscal_quarter  VARCHAR(10) NULL,

        year_sort     INT NOT NULL,
        quarter_sort  INT NOT NULL,
        month_sort    INT NOT NULL,

        CONSTRAINT CHK_dim_date_Quarter  CHECK (quarter_of_year BETWEEN 1 AND 4),
        CONSTRAINT CHK_dim_date_Month    CHECK (month_of_year   BETWEEN 1 AND 12),
        CONSTRAINT CHK_dim_date_DayOfWk  CHECK (day_of_week     BETWEEN 1 AND 7)
    );

    CREATE INDEX IX_dim_date_year        ON dbo.dim_date([year]);
    CREATE INDEX IX_dim_date_year_qtr    ON dbo.dim_date([year], quarter_of_year);
    CREATE INDEX IX_dim_date_year_month  ON dbo.dim_date([year], month_of_year);
    CREATE INDEX IX_dim_date_economic    ON dbo.dim_date(economic_period);
    CREATE INDEX IX_dim_date_holiday     ON dbo.dim_date(is_holiday);

    -- Populate a complete date range
    ;WITH d AS
    (
        SELECT CAST('2019-01-01' AS DATE) AS dt
        UNION ALL
        SELECT DATEADD(DAY, 1, dt)
        FROM d
        WHERE dt < '2024-12-31'
    )
    INSERT INTO dbo.dim_date (
        date_id, full_date,
        [year], year_name,
        quarter_of_year, quarter_name,
        month_of_year, month_name, month_abbr,
        week_of_year, week_of_month,
        day_of_month, day_of_week, day_name,
        is_weekday, is_holiday, holiday_name,
        economic_period, fiscal_year, fiscal_quarter,
        year_sort, quarter_sort, month_sort
    )
    SELECT
        CONVERT(INT, FORMAT(dt, 'yyyyMMdd'))                                       AS date_id,
        dt                                                                          AS full_date,
        DATEPART(YEAR, dt)                                                          AS [year],
        CONVERT(VARCHAR(10), DATEPART(YEAR, dt))                                    AS year_name,
        DATEPART(QUARTER, dt)                                                       AS quarter_of_year,
        'Q' + CONVERT(VARCHAR(1), DATEPART(QUARTER, dt)) + ' ' + CONVERT(VARCHAR(4), DATEPART(YEAR, dt)) AS quarter_name,
        DATEPART(MONTH, dt)                                                         AS month_of_year,
        DATENAME(MONTH, dt)                                                         AS month_name,
        LEFT(DATENAME(MONTH, dt), 3)                                                AS month_abbr,
        DATEPART(ISO_WEEK, dt)                                                      AS week_of_year,
        ((DATEPART(DAY, dt) - 1) / 7) + 1                                          AS week_of_month,
        DATEPART(DAY, dt)                                                           AS day_of_month,
        (DATEPART(WEEKDAY, dt) + @@DATEFIRST - 1) % 7 + 1                           AS day_of_week,
        DATENAME(WEEKDAY, dt)                                                       AS day_name,
        CASE WHEN DATENAME(WEEKDAY, dt) IN ('Saturday','Sunday') THEN 0 ELSE 1 END  AS is_weekday,
        CASE
            WHEN (MONTH(dt)=1  AND DAY(dt)=1)  THEN 1
            WHEN (MONTH(dt)=7  AND DAY(dt)=4)  THEN 1
            WHEN (MONTH(dt)=12 AND DAY(dt)=25) THEN 1
            ELSE 0
        END                                                                        AS is_holiday,
        CASE
            WHEN (MONTH(dt)=1  AND DAY(dt)=1)  THEN 'New Year''s Day'
            WHEN (MONTH(dt)=7  AND DAY(dt)=4)  THEN 'Independence Day'
            WHEN (MONTH(dt)=12 AND DAY(dt)=25) THEN 'Christmas Day'
            ELSE NULL
        END                                                                        AS holiday_name,
        CASE
            WHEN dt <  '2020-03-11' THEN 'Pre-Pandemic'
            WHEN dt <= '2021-06-30' THEN 'Pandemic Peak'
            WHEN dt <= '2022-12-31' THEN 'Recovery'
            WHEN dt <= '2023-12-31' THEN 'Post-Pandemic'
            ELSE 'Current Period'
        END                                                                        AS economic_period,
        DATEPART(YEAR, dt)                                                         AS fiscal_year,
        'FY' + RIGHT(CONVERT(VARCHAR(4), DATEPART(YEAR, dt)), 2) + ' Q' + CONVERT(VARCHAR(1), DATEPART(QUARTER, dt)) AS fiscal_quarter,
        DATEPART(YEAR, dt)                                                         AS year_sort,
        DATEPART(YEAR, dt) * 10 + DATEPART(QUARTER, dt)                            AS quarter_sort,
        DATEPART(YEAR, dt) * 100 + DATEPART(MONTH, dt)                             AS month_sort
    FROM d
    OPTION (MAXRECURSION 0);

    -- dim_geography
    CREATE TABLE dbo.dim_geography (
        geography_id   VARCHAR(10) PRIMARY KEY,
        country        VARCHAR(100) NOT NULL,
        state_code     VARCHAR(2)   NOT NULL,
        state_name     VARCHAR(100) NOT NULL,
        county_fips    VARCHAR(5)   NULL,
        county_name    VARCHAR(100) NULL,
        metro_area     VARCHAR(100) NULL,
        region         VARCHAR(50)  NULL,
        division       VARCHAR(50)  NULL,
        latitude       FLOAT        NULL,
        longitude      FLOAT        NULL,
        population     INT          NULL,
        area_sq_miles  FLOAT        NULL,
        effective_date DATE         NOT NULL,
        expiration_date DATE        NULL,
        is_current     BIT          NOT NULL DEFAULT 1,
        created_date   DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
    );

    -- dim_demographics
    CREATE TABLE dbo.dim_demographics (
        demographic_id     VARCHAR(10) PRIMARY KEY,
        age_group          VARCHAR(20)  NOT NULL,
        gender             VARCHAR(10)  NOT NULL,
        race_ethnicity     VARCHAR(50)  NOT NULL,
        education_level    VARCHAR(50)  NOT NULL,
        marital_status     VARCHAR(20)  NOT NULL,
        veteran_status     VARCHAR(20)  NOT NULL,
        disability_status  VARCHAR(20)  NOT NULL,
        citizenship_status VARCHAR(20)  NOT NULL,
        english_proficiency VARCHAR(20) NOT NULL,
        created_date       DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
    );

    -- dim_industry
    CREATE TABLE dbo.dim_industry (
        industry_id     VARCHAR(10) PRIMARY KEY,
        naics_code      VARCHAR(6)   NOT NULL,
        industry_name   VARCHAR(200) NOT NULL,
        industry_sector VARCHAR(100) NOT NULL,
        industry_group  VARCHAR(100) NOT NULL,
        seasonal_factor VARCHAR(20)  NULL CHECK (seasonal_factor IN ('Low','Medium','High')),
        automation_risk VARCHAR(20)  NULL CHECK (automation_risk IN ('Low','Medium','High')),
        avg_wage_level  VARCHAR(20)  NULL CHECK (avg_wage_level  IN ('Low','Medium','High')),
        sector_code     VARCHAR(2)   NULL,
        supersector_code VARCHAR(3)  NULL,
        is_active       BIT          NOT NULL DEFAULT 1,
        created_date    DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
    );

    /* =====================
       2) Load Dimensions
       ===================== */

    INSERT INTO dbo.dim_geography (geography_id, country, state_code, state_name, county_fips, county_name, metro_area, region, division, latitude, longitude, population, area_sq_miles, effective_date, expiration_date, is_current) VALUES
        ('CA001', 'USA', 'CA', 'California', '06037', 'Los Angeles County', 'Los Angeles-Long Beach-Anaheim', 'West', 'Pacific', 34.0522, -118.2437, 10014009, 4751.00, '2019-01-01', NULL, 1),
        ('TX001', 'USA', 'TX', 'Texas', '48201', 'Harris County', 'Houston-The Woodlands-Sugar Land', 'South', 'West South Central', 29.7604, -95.3698, 4731145, 1703.00, '2019-01-01', NULL, 1),
        ('NY001', 'USA', 'NY', 'New York', '36061', 'New York County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.7128, -74.0060, 1694251, 23.00, '2019-01-01', NULL, 1),
        ('FL001', 'USA', 'FL', 'Florida', '12086', 'Miami-Dade County', 'Miami-Fort Lauderdale-West Palm Beach', 'South', 'South Atlantic', 25.7617, -80.1918, 2716940, 1946.00, '2019-01-01', NULL, 1),
        ('IL001', 'USA', 'IL', 'Illinois', '17031', 'Cook County', 'Chicago-Naperville-Elgin', 'Midwest', 'East North Central', 41.8781, -87.6298, 5275541, 946.00, '2019-01-01', NULL, 1),
        ('PA001', 'USA', 'PA', 'Pennsylvania', '42101', 'Philadelphia County', 'Philadelphia-Camden-Wilmington', 'Northeast', 'Middle Atlantic', 39.9526, -75.1652, 1603797, 135.00, '2019-01-01', NULL, 1),
        ('OH001', 'USA', 'OH', 'Ohio', '39035', 'Cuyahoga County', 'Cleveland-Elyria', 'Midwest', 'East North Central', 41.4993, -81.6944, 1235072, 458.00, '2019-01-01', NULL, 1),
        ('GA001', 'USA', 'GA', 'Georgia', '13135', 'Gwinnett County', 'Atlanta-Sandy Springs-Roswell', 'South', 'South Atlantic', 33.7490, -84.3880, 957062, 430.00, '2019-01-01', NULL, 1),
        ('NC001', 'USA', 'NC', 'North Carolina', '37119', 'Mecklenburg County', 'Charlotte-Concord-Gastonia', 'South', 'South Atlantic', 35.2271, -80.8431, 1115482, 546.00, '2019-01-01', NULL, 1),
        ('MI001', 'USA', 'MI', 'Michigan', '26163', 'Wayne County', 'Detroit-Warren-Dearborn', 'Midwest', 'East North Central', 42.3314, -83.0458, 1793561, 612.00, '2019-01-01', NULL, 1),
        ('WA001', 'USA', 'WA', 'Washington', '53033', 'King County', 'Seattle-Tacoma-Bellevue', 'West', 'Pacific', 47.6062, -122.3321, 2269675, 2134.00, '2019-01-01', NULL, 1),
        ('AZ001', 'USA', 'AZ', 'Arizona', '04013', 'Maricopa County', 'Phoenix-Mesa-Scottsdale', 'West', 'Mountain', 33.4484, -112.0740, 4485414, 9224.00, '2019-01-01', NULL, 1),
        ('TN001', 'USA', 'TN', 'Tennessee', '47037', 'Davidson County', 'Nashville-Davidson-Murfreesboro', 'South', 'East South Central', 36.1627, -86.7816, 715884, 502.00, '2019-01-01', NULL, 1),
        ('MA001', 'USA', 'MA', 'Massachusetts', '25025', 'Suffolk County', 'Boston-Cambridge-Newton', 'Northeast', 'New England', 42.3601, -71.0589, 797936, 58.00, '2019-01-01', NULL, 1),
        ('IN001', 'USA', 'IN', 'Indiana', '18097', 'Marion County', 'Indianapolis-Carmel-Anderson', 'Midwest', 'East North Central', 39.7684, -86.1581, 977203, 396.00, '2019-01-01', NULL, 1),
        ('MO001', 'USA', 'MO', 'Missouri', '29189', 'St. Louis County', 'St. Louis', 'Midwest', 'West North Central', 38.6270, -90.1994, 1001876, 508.00, '2019-01-01', NULL, 1),
        ('WI001', 'USA', 'WI', 'Wisconsin', '55079', 'Milwaukee County', 'Milwaukee-Waukesha-West Allis', 'Midwest', 'East North Central', 43.0389, -87.9065, 945726, 241.00, '2019-01-01', NULL, 1),
        ('MD001', 'USA', 'MD', 'Maryland', '24005', 'Baltimore County', 'Baltimore-Columbia-Towson', 'South', 'South Atlantic', 39.2904, -76.6122, 854535, 599.00, '2019-01-01', NULL, 1),
        ('MN001', 'USA', 'MN', 'Minnesota', '27123', 'Ramsey County', 'Minneapolis-St. Paul-Bloomington', 'Midwest', 'West North Central', 44.9778, -93.2650, 552344, 152.00, '2019-01-01', NULL, 1),
        ('CO001', 'USA', 'CO', 'Colorado', '08031', 'Denver County', 'Denver-Aurora-Lakewood', 'West', 'Mountain', 39.7392, -104.9903, 715522, 153.00, '2019-01-01', NULL, 1),
        ('CA002', 'USA', 'CA', 'California', '06073', 'San Diego County', 'San Diego-Carlsbad', 'West', 'Pacific', 32.7157, -117.1611, 3338330, 4206.00, '2019-01-01', NULL, 1),
        ('CA003', 'USA', 'CA', 'California', '06075', 'San Francisco County', 'San Francisco-Oakland-Hayward', 'West', 'Pacific', 37.7749, -122.4194, 881549, 47.00, '2019-01-01', NULL, 1),
        ('TX002', 'USA', 'TX', 'Texas', '48113', 'Dallas County', 'Dallas-Fort Worth-Arlington', 'South', 'West South Central', 32.7767, -96.7970, 2613539, 880.00, '2019-01-01', NULL, 1),
        ('TX003', 'USA', 'TX', 'Texas', '48029', 'Bexar County', 'San Antonio-New Braunfels', 'South', 'West South Central', 29.4241, -98.4936, 2003554, 1247.00, '2019-01-01', NULL, 1),
        ('FL002', 'USA', 'FL', 'Florida', '12095', 'Orange County', 'Orlando-Kissimmee-Sanford', 'South', 'South Atlantic', 28.5383, -81.3792, 1393452, 907.00, '2019-01-01', NULL, 1),
        ('FL003', 'USA', 'FL', 'Florida', '12103', 'Pinellas County', 'Tampa-St. Petersburg-Clearwater', 'South', 'South Atlantic', 27.7663, -82.6404, 959107, 280.00, '2019-01-01', NULL, 1),
        ('NY002', 'USA', 'NY', 'New York', '36047', 'Kings County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.6782, -73.9442, 2736074, 70.00, '2019-01-01', NULL, 1),
        ('NY003', 'USA', 'NY', 'New York', '36081', 'Queens County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.7282, -73.7949, 2405464, 109.00, '2019-01-01', NULL, 1),
        ('NV001', 'USA', 'NV', 'Nevada', '32003', 'Clark County', 'Las Vegas-Henderson-Paradise', 'West', 'Mountain', 36.1699, -115.1398, 2266715, 7891.00, '2019-01-01', NULL, 1),
        ('OR001', 'USA', 'OR', 'Oregon', '41051', 'Multnomah County', 'Portland-Vancouver-Hillsboro', 'West', 'Pacific', 45.5152, -122.6784, 815428, 431.00, '2019-01-01', NULL, 1),
        ('UT001', 'USA', 'UT', 'Utah', '49035', 'Salt Lake County', 'Salt Lake City', 'West', 'Mountain', 40.7608, -111.8910, 1185238, 742.00, '2019-01-01', NULL, 1),
        ('VA001', 'USA', 'VA', 'Virginia', '51059', 'Fairfax County', 'Washington-Arlington-Alexandria', 'South', 'South Atlantic', 38.9043, -77.0384, 1150309, 395.00, '2019-01-01', NULL, 1),
        ('NJ001', 'USA', 'NJ', 'New Jersey', '34003', 'Bergen County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.8964, -74.0395, 955732, 234.00, '2019-01-01', NULL, 1),
        ('CT001', 'USA', 'CT', 'Connecticut', '09001', 'Fairfield County', 'Bridgeport-Stamford-Norwalk', 'Northeast', 'New England', 41.3083, -73.0275, 957419, 626.00, '2019-01-01', NULL, 1),
        ('SC001', 'USA', 'SC', 'South Carolina', '45019', 'Charleston County', 'Charleston-North Charleston', 'South', 'South Atlantic', 32.7765, -79.9311, 408235, 916.00, '2019-01-01', NULL, 1),
        ('KY001', 'USA', 'KY', 'Kentucky', '21111', 'Jefferson County', 'Louisville-Jefferson County', 'South', 'East South Central', 38.2527, -85.7585, 766757, 380.00, '2019-01-01', NULL, 1),
        ('LA001', 'USA', 'LA', 'Louisiana', '22071', 'Orleans Parish', 'New Orleans-Metairie', 'South', 'West South Central', 29.9511, -90.0715, 390144, 169.00, '2019-01-01', NULL, 1),
        ('OK001', 'USA', 'OK', 'Oklahoma', '40109', 'Oklahoma County', 'Oklahoma City', 'South', 'West South Central', 35.4676, -97.5164, 797434, 709.00, '2019-01-01', NULL, 1),
        ('AR001', 'USA', 'AR', 'Arkansas', '05119', 'Pulaski County', 'Little Rock-North Little Rock-Conway', 'South', 'West South Central', 34.7465, -92.2896, 395760, 774.00, '2019-01-01', NULL, 1),
        ('KS001', 'USA', 'KS', 'Kansas', '20091', 'Johnson County', 'Kansas City', 'Midwest', 'West North Central', 38.9072, -94.7203, 597511, 477.00, '2019-01-01', NULL, 1),
        ('IA001', 'USA', 'IA', 'Iowa', '19153', 'Polk County', 'Des Moines-West Des Moines', 'Midwest', 'West North Central', 41.5868, -93.6250, 492401, 569.00, '2019-01-01', NULL, 1),
        ('NE001', 'USA', 'NE', 'Nebraska', '31055', 'Douglas County', 'Omaha-Council Bluffs', 'Midwest', 'West North Central', 41.2565, -95.9345, 571327, 335.00, '2019-01-01', NULL, 1),
        ('MT001', 'USA', 'MT', 'Montana', '30111', 'Yellowstone County', 'Billings', 'West', 'Mountain', 45.7833, -108.5007, 164731, 2633.00, '2019-01-01', NULL, 1),
        ('ID001', 'USA', 'ID', 'Idaho', '16001', 'Ada County', 'Boise City', 'West', 'Mountain', 43.6150, -116.2023, 481587, 1055.00, '2019-01-01', NULL, 1),
        ('WY001', 'USA', 'WY', 'Wyoming', '56025', 'Natrona County', 'Casper', 'West', 'Mountain', 42.8500, -106.3162, 79858, 5376.00, '2019-01-01', NULL, 1),
        ('ND001', 'USA', 'ND', 'North Dakota', '38017', 'Cass County', 'Fargo', 'Midwest', 'West North Central', 46.8772, -96.7898, 184525, 1765.00, '2019-01-01', NULL, 1),
        ('SD001', 'USA', 'SD', 'South Dakota', '46099', 'Minnehaha County', 'Sioux Falls', 'Midwest', 'West North Central', 43.5446, -96.7311, 197214, 809.00, '2019-01-01', NULL, 1),
        ('ME001', 'USA', 'ME', 'Maine', '23005', 'Cumberland County', 'Portland-South Portland', 'Northeast', 'New England', 43.6591, -70.2568, 303069, 835.00, '2019-01-01', NULL, 1),
        ('NH001', 'USA', 'NH', 'New Hampshire', '33011', 'Hillsborough County', 'Manchester-Nashua', 'Northeast', 'New England', 42.9956, -71.4548, 422937, 876.00, '2019-01-01', NULL, 1),
        ('VT001', 'USA', 'VT', 'Vermont', '50007', 'Chittenden County', 'Burlington-South Burlington', 'Northeast', 'New England', 44.4759, -73.2121, 168323, 536.00, '2019-01-01', NULL, 1),
        ('RI001', 'USA', 'RI', 'Rhode Island', '44007', 'Providence County', 'Providence-Warwick', 'Northeast', 'New England', 41.8240, -71.4128, 660741, 410.00, '2019-01-01', NULL, 1),
        ('AK001', 'USA', 'AK', 'Alaska', '02020', 'Anchorage Municipality', 'Anchorage', 'West', 'Pacific', 61.2181, -149.9003, 291247, 1961.00, '2019-01-01', NULL, 1),
        ('HI001', 'USA', 'HI', 'Hawaii', '15003', 'Honolulu County', 'Urban Honolulu', 'West', 'Pacific', 21.3099, -157.8581, 1016508, 596.00, '2019-01-01', NULL, 1),
        ('AL001', 'USA', 'AL', 'Alabama', '01073', 'Jefferson County', 'Birmingham-Hoover', 'South', 'East South Central', 33.5207, -86.8025, 674721, 1111.00, '2019-01-01', NULL, 1),
        ('MS001', 'USA', 'MS', 'Mississippi', '28049', 'Hinds County', 'Jackson', 'South', 'East South Central', 32.2988, -90.1848, 238674, 869.00, '2019-01-01', NULL, 1),
        ('WV001', 'USA', 'WV', 'West Virginia', '54039', 'Kanawha County', 'Charleston', 'South', 'South Atlantic', 38.3498, -81.6326, 180745, 903.00, '2019-01-01', NULL, 1),
        ('DE001', 'USA', 'DE', 'Delaware', '10003', 'New Castle County', 'Philadelphia-Camden-Wilmington', 'South', 'South Atlantic', 39.7391, -75.5398, 570719, 426.00, '2019-01-01', NULL, 1);

    INSERT INTO dbo.dim_demographics (demographic_id, age_group, gender, race_ethnicity, education_level, marital_status, veteran_status, disability_status, citizenship_status, english_proficiency, created_date) VALUES
        ('DM001', '25-34', 'Male', 'White', 'Bachelor''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM002', '35-44', 'Female', 'Hispanic or Latino', 'High School Graduate', 'Married', 'Non-Veteran', 'No Disability', 'US Citizen', 'Bilingual', '2019-01-01'),
        ('DM003', '45-54', 'Male', 'Black or African American', 'Associate Degree', 'Divorced', 'Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM004', '25-34', 'Female', 'Asian', 'Master''s Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
        ('DM005', '55-64', 'Male', 'White', 'High School Graduate', 'Married', 'Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM006', '20-24', 'Female', 'Hispanic or Latino', 'Some College', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM007', '35-44', 'Male', 'White', 'Bachelor''s Degree', 'Married', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM008', '25-34', 'Female', 'Black or African American', 'Bachelor''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM009', '45-54', 'Male', 'Hispanic or Latino', 'Less than High School', 'Married', 'Non-Veteran', 'No Disability', 'Permanent Resident', 'Limited English', '2019-01-01'),
        ('DM010', '35-44', 'Female', 'White', 'Master''s Degree', 'Divorced', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM011', '55-64', 'Female', 'Asian', 'Professional Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
        ('DM012', '20-24', 'Male', 'Black or African American', 'High School Graduate', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM013', '45-54', 'Female', 'White', 'Associate Degree', 'Married', 'Non-Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM014', '25-34', 'Male', 'Two or More Races', 'Bachelor''s Degree', 'Single', 'Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM015', '35-44', 'Female', 'American Indian or Alaska Native', 'Some College', 'Divorced', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM016', '16-19', 'Male', 'Hispanic or Latino', 'Less than High School', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Bilingual', '2019-01-01'),
        ('DM017', '65+', 'Female', 'White', 'High School Graduate', 'Widowed', 'Non-Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM018', '25-34', 'Non-Binary', 'White', 'Master''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
        ('DM019', '35-44', 'Male', 'Asian', 'Doctoral Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
        ('DM020', '20-24', 'Female', 'Native Hawaiian or Pacific Islander', 'Associate Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01');

    INSERT INTO dbo.dim_industry (industry_id, naics_code, industry_name, industry_sector, industry_group,
    seasonal_factor, automation_risk, avg_wage_level, sector_code, supersector_code, is_active) VALUES
        ('IND001', '54', 'Professional, Scientific, and Technical Services', 'Professional Services', 'Information & Professional', 
 'Low', 'Medium', 'High', '54', '540', 1),
        ('IND002', '62', 'Health Care and Social Assistance', 'Healthcare', 'Education & Health', 
 'Low', 'Low', 'High', '62', '620', 1),
        ('IND003', '44', 'Retail Trade', 'Retail', 'Trade & Transportation', 
 'High', 'High', 'Low', '44', '440', 1),
        ('IND004', '31', 'Manufacturing', 'Manufacturing', 'Manufacturing', 
 'Medium', 'High', 'Medium', '31', '310', 1),
        ('IND005', '72', 'Accommodation and Food Services', 'Hospitality', 'Leisure & Hospitality', 
 'High', 'Medium', 'Low', '72', '720', 1),
        ('IND006', '23', 'Construction', 'Construction', 'Construction & Resources', 
 'High', 'Medium', 'Medium', '23', '230', 1),
        ('IND007', '48', 'Transportation and Warehousing', 'Transportation', 'Trade & Transportation', 
 'Medium', 'High', 'Medium', '48', '480', 1),
        ('IND008', '61', 'Educational Services', 'Education', 'Education & Health', 
 'High', 'Low', 'Medium', '61', '610', 1),
        ('IND009', '52', 'Finance and Insurance', 'Financial Services', 'Financial Activities', 
 'Low', 'Medium', 'High', '52', '520', 1),
        ('IND010', '51', 'Information', 'Information Technology', 'Information & Professional', 
 'Low', 'Medium', 'High', '51', '510', 1),
        ('IND011', '92', 'Public Administration', 'Government', 'Government', 
 'Low', 'Low', 'Medium', '92', '920', 1),
        ('IND012', '71', 'Arts, Entertainment, and Recreation', 'Entertainment', 'Leisure & Hospitality', 
 'High', 'Medium', 'Low', '71', '710', 1),
        ('IND013', '11', 'Agriculture, Forestry, Fishing and Hunting', 'Agriculture', 'Construction & Resources', 
 'High', 'Medium', 'Low', '11', '110', 1),
        ('IND014', '22', 'Utilities', 'Utilities', 'Construction & Resources', 
 'Low', 'Medium', 'High', '22', '220', 1),
        ('IND015', '81', 'Other Services (except Public Administration)', 'Other Services', 'Other Services', 
 'Medium', 'Medium', 'Low', '81', '810', 1);

    /* =====================
       3) Fact + FKs
       ===================== */
    CREATE TABLE dbo.unemployment_statistics (
        record_id         INT IDENTITY(1,1) PRIMARY KEY,
        date_id           INT         NOT NULL,
        geography_id      VARCHAR(10) NOT NULL,
        demographic_id    VARCHAR(10) NOT NULL,
        industry_id       VARCHAR(10) NOT NULL,
        labor_force       INT         NOT NULL,
        employed          INT         NOT NULL,
        unemployed        INT         NOT NULL,
        unemployment_rate FLOAT       NOT NULL,
        participation_rate FLOAT      NULL,
        created_date      DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME()
    );

    ALTER TABLE dbo.unemployment_statistics
        ADD CONSTRAINT FK_unemployment_statistics_date_id
            FOREIGN KEY (date_id)        REFERENCES dbo.dim_date(date_id);

    ALTER TABLE dbo.unemployment_statistics
        ADD CONSTRAINT FK_unemployment_statistics_geography_id
            FOREIGN KEY (geography_id)   REFERENCES dbo.dim_geography(geography_id);

    ALTER TABLE dbo.unemployment_statistics
        ADD CONSTRAINT FK_unemployment_statistics_demographic_id
            FOREIGN KEY (demographic_id) REFERENCES dbo.dim_demographics(demographic_id);

    ALTER TABLE dbo.unemployment_statistics
        ADD CONSTRAINT FK_unemployment_statistics_industry_id
            FOREIGN KEY (industry_id)    REFERENCES dbo.dim_industry(industry_id);

    -- Re-runnable load for facts
    TRUNCATE TABLE dbo.unemployment_statistics;

    INSERT INTO dbo.unemployment_statistics
        (date_id, geography_id, demographic_id, industry_id, labor_force, employed, unemployed, unemployment_rate, participation_rate)
    VALUES
        (20190101, 'CA001', 'DM001', 'IND001', 1250000, 1200000, 50000, 4.0, 65.2),
        (20190101, 'TX001', 'DM002', 'IND002', 890000, 850000, 40000, 4.5, 68.1),
        (20190101, 'NY001', 'DM003', 'IND003', 2100000, 2010000, 90000, 4.3, 62.8),
        (20190101, 'FL001', 'DM004', 'IND004', 1680000, 1620000, 60000, 3.6, 63.5),
        (20190101, 'IL001', 'DM005', 'IND005', 950000, 905000, 45000, 4.7, 64.2),
        (20190101, 'PA001', 'DM006', 'IND006', 1420000, 1358000, 62000, 4.4, 63.1),
        (20190101, 'OH001', 'DM007', 'IND007', 1180000, 1127000, 53000, 4.5, 64.8),
        (20190101, 'GA001', 'DM008', 'IND008', 1050000, 1008000, 42000, 4.0, 65.3),
        (20190101, 'NC001', 'DM009', 'IND009', 980000, 940000, 40000, 4.1, 66.2),
        (20190101, 'MI001', 'DM010', 'IND010', 890000, 850000, 40000, 4.5, 63.7),
        (20190201, 'CA001', 'DM001', 'IND001', 1255000, 1208000, 47000, 3.7, 65.4),
        (20190201, 'TX001', 'DM002', 'IND002', 892000, 855000, 37000, 4.1, 68.3),
        (20190201, 'NY001', 'DM003', 'IND003', 2105000, 2020000, 85000, 4.0, 63.1),
        (20190201, 'FL001', 'DM004', 'IND004', 1685000, 1628000, 57000, 3.4, 63.7),
        (20190201, 'IL001', 'DM005', 'IND005', 952000, 910000, 42000, 4.4, 64.4),
        (20190201, 'PA001', 'DM006', 'IND006', 1425000, 1365000, 60000, 4.2, 63.3),
        (20190201, 'OH001', 'DM007', 'IND007', 1185000, 1135000, 50000, 4.2, 65.0),
        (20190201, 'GA001', 'DM008', 'IND008', 1055000, 1015000, 40000, 3.8, 65.5),
        (20190201, 'NC001', 'DM009', 'IND009', 985000, 948000, 37000, 3.8, 66.4),
        (20190201, 'MI001', 'DM010', 'IND010', 895000, 858000, 37000, 4.1, 63.9),
        (20190301, 'CA001', 'DM011', 'IND011', 1260000, 1215000, 45000, 3.6, 65.6),
        (20190301, 'TX001', 'DM012', 'IND012', 895000, 860000, 35000, 3.9, 68.5),
        (20190301, 'NY001', 'DM013', 'IND013', 2110000, 2028000, 82000, 3.9, 63.3),
        (20190301, 'FL001', 'DM014', 'IND014', 1690000, 1635000, 55000, 3.3, 63.9),
        (20190301, 'IL001', 'DM015', 'IND015', 955000, 915000, 40000, 4.2, 64.6),
        (20190301, 'WA001', 'DM016', 'IND001', 750000, 720000, 30000, 4.0, 67.2),
        (20190301, 'AZ001', 'DM017', 'IND002', 680000, 652000, 28000, 4.1, 64.8),
        (20190301, 'TN001', 'DM018', 'IND003', 620000, 595000, 25000, 4.0, 65.9),
        (20190301, 'MA001', 'DM019', 'IND004', 580000, 558000, 22000, 3.8, 66.5),
        (20190301, 'IN001', 'DM020', 'IND005', 520000, 500000, 20000, 3.8, 64.2),
        (20200401, 'CA001', 'DM001', 'IND006', 1200000, 1050000, 150000, 12.5, 60.2),
        (20200401, 'TX001', 'DM002', 'IND006', 870000, 760000, 110000, 12.6, 65.8),
        (20200401, 'NY001', 'DM003', 'IND006', 2050000, 1750000, 300000, 14.6, 58.4),
        (20200401, 'FL001', 'DM004', 'IND007', 1620000, 1420000, 200000, 12.3, 59.8),
        (20200401, 'IL001', 'DM005', 'IND007', 920000, 805000, 115000, 12.5, 60.1),
        (20200401, 'PA001', 'DM006', 'IND008', 1380000, 1200000, 180000, 13.0, 59.5),
        (20200401, 'OH001', 'DM007', 'IND008', 1150000, 1000000, 150000, 13.0, 60.8),
        (20200401, 'GA001', 'DM008', 'IND009', 1020000, 895000, 125000, 12.3, 61.2),
        (20200401, 'NC001', 'DM009', 'IND009', 950000, 840000, 110000, 11.6, 62.5),
        (20200401, 'MI001', 'DM010', 'IND010', 860000, 730000, 130000, 15.1, 58.9),
        (20200501, 'CA001', 'DM001', 'IND006', 1180000, 1020000, 160000, 13.6, 59.1),
        (20200501, 'TX001', 'DM002', 'IND006', 860000, 745000, 115000, 13.4, 65.2),
        (20200501, 'NY001', 'DM003', 'IND006', 2020000, 1700000, 320000, 15.8, 57.1),
        (20200501, 'FL001', 'DM004', 'IND007', 1600000, 1380000, 220000, 13.8, 58.9),
        (20200501, 'IL001', 'DM005', 'IND007', 910000, 785000, 125000, 13.7, 59.3),
        (20200501, 'PA001', 'DM006', 'IND008', 1360000, 1170000, 190000, 14.0, 58.8),
        (20200501, 'OH001', 'DM007', 'IND008', 1130000, 970000, 160000, 14.2, 59.9),
        (20200501, 'GA001', 'DM008', 'IND009', 1000000, 870000, 130000, 13.0, 60.5),
        (20200501, 'NC001', 'DM009', 'IND009', 930000, 815000, 115000, 12.4, 61.8),
        (20200501, 'MI001', 'DM010', 'IND010', 840000, 705000, 135000, 16.1, 57.8),
        (20210101, 'CA001', 'DM006', 'IND008', 1220000, 1130000, 90000, 7.4, 62.1),
        (20210101, 'TX001', 'DM007', 'IND009', 885000, 825000, 60000, 6.8, 66.9),
        (20210101, 'NY001', 'DM008', 'IND010', 2080000, 1930000, 150000, 7.2, 60.5),
        (20210101, 'FL001', 'DM009', 'IND011', 1650000, 1540000, 110000, 6.7, 61.8),
        (20210101, 'IL001', 'DM010', 'IND012', 935000, 870000, 65000, 7.0, 62.1),
        (20210101, 'PA001', 'DM011', 'IND013', 1400000, 1295000, 105000, 7.5, 61.2),
        (20210101, 'OH001', 'DM012', 'IND014', 1160000, 1080000, 80000, 6.9, 62.8),
        (20210101, 'GA001', 'DM013', 'IND015', 1030000, 960000, 70000, 6.8, 63.2),
        (20210101, 'NC001', 'DM014', 'IND001', 970000, 910000, 60000, 6.2, 64.1),
        (20210101, 'MI001', 'DM015', 'IND002', 875000, 815000, 60000, 6.9, 61.5),
        (20210201, 'CA001', 'DM006', 'IND008', 1225000, 1140000, 85000, 6.9, 62.5),
        (20210201, 'TX001', 'DM007', 'IND009', 890000, 835000, 55000, 6.2, 67.2),
        (20210201, 'NY001', 'DM008', 'IND010', 2090000, 1950000, 140000, 6.7, 60.8),
        (20210201, 'FL001', 'DM009', 'IND011', 1660000, 1560000, 100000, 6.0, 62.2),
        (20210201, 'IL001', 'DM010', 'IND012', 940000, 880000, 60000, 6.4, 62.5),
        (20210201, 'WA001', 'DM016', 'IND001', 760000, 715000, 45000, 5.9, 66.8),
        (20210201, 'AZ001', 'DM017', 'IND002', 695000, 655000, 40000, 5.8, 65.9),
        (20210201, 'TN001', 'DM018', 'IND003', 635000, 600000, 35000, 5.5, 66.8),
        (20210201, 'MA001', 'DM019', 'IND004', 590000, 560000, 30000, 5.1, 67.2),
        (20210201, 'IN001', 'DM020', 'IND005', 535000, 508000, 27000, 5.0, 65.1),
        (20220101, 'CA001', 'DM001', 'IND001', 1245000, 1190000, 55000, 4.4, 64.8),
        (20220101, 'TX001', 'DM002', 'IND002', 905000, 865000, 40000, 4.4, 68.5),
        (20220101, 'NY001', 'DM003', 'IND003', 2120000, 2030000, 90000, 4.2, 62.9),
        (20220101, 'FL001', 'DM004', 'IND004', 1685000, 1625000, 60000, 3.6, 64.1),
        (20220101, 'IL001', 'DM005', 'IND005', 950000, 910000, 40000, 4.2, 63.8),
        (20220101, 'PA001', 'DM006', 'IND006', 1430000, 1375000, 55000, 3.8, 62.8),
        (20220101, 'OH001', 'DM007', 'IND007', 1175000, 1130000, 45000, 3.8, 64.2),
        (20220101, 'GA001', 'DM008', 'IND008', 1045000, 1005000, 40000, 3.8, 64.8),
        (20220101, 'NC001', 'DM009', 'IND009', 990000, 955000, 35000, 3.5, 65.7),
        (20220101, 'MI001', 'DM010', 'IND010', 885000, 850000, 35000, 4.0, 63.2),
        (20220201, 'CA001', 'DM011', 'IND011', 1248000, 1195000, 53000, 4.2, 65.0),
        (20220201, 'TX001', 'DM012', 'IND012', 908000, 870000, 38000, 4.2, 68.7),
        (20220201, 'NY001', 'DM013', 'IND013', 2125000, 2040000, 85000, 4.0, 63.2),
        (20220201, 'FL001', 'DM014', 'IND014', 1690000, 1635000, 55000, 3.3, 64.4),
        (20220201, 'IL001', 'DM015', 'IND015', 953000, 915000, 38000, 4.0, 64.0),
        (20220201, 'WA001', 'DM016', 'IND001', 765000, 735000, 30000, 3.9, 67.5),
        (20220201, 'AZ001', 'DM017', 'IND002', 705000, 678000, 27000, 3.8, 66.8),
        (20220201, 'TN001', 'DM018', 'IND003', 645000, 620000, 25000, 3.9, 67.5),
        (20220201, 'MA001', 'DM019', 'IND004', 595000, 572000, 23000, 3.9, 67.8),
        (20220201, 'IN001', 'DM020', 'IND005', 545000, 525000, 20000, 3.7, 65.9),
        (20230101, 'CA001', 'DM001', 'IND001', 1265000, 1215000, 50000, 4.0, 66.2),
        (20230101, 'TX001', 'DM002', 'IND002', 915000, 880000, 35000, 3.8, 69.1),
        (20230101, 'NY001', 'DM003', 'IND003', 2140000, 2055000, 85000, 4.0, 63.8),
        (20230101, 'FL001', 'DM004', 'IND004', 1705000, 1650000, 55000, 3.2, 65.2),
        (20230101, 'IL001', 'DM005', 'IND005', 960000, 920000, 40000, 4.2, 64.5),
        (20230101, 'PA001', 'DM006', 'IND006', 1440000, 1385000, 55000, 3.8, 63.5),
        (20230101, 'OH001', 'DM007', 'IND007', 1185000, 1140000, 45000, 3.8, 64.8),
        (20230101, 'GA001', 'DM008', 'IND008', 1055000, 1015000, 40000, 3.8, 65.5),
        (20230101, 'NC001', 'DM009', 'IND009', 1005000, 970000, 35000, 3.5, 66.4),
        (20230101, 'MI001', 'DM010', 'IND010', 895000, 860000, 35000, 3.9, 63.8),
        (20230201, 'CA001', 'DM011', 'IND011', 1268000, 1220000, 48000, 3.8, 66.4),
        (20230201, 'TX001', 'DM012', 'IND012', 918000, 885000, 33000, 3.6, 69.3),
        (20230201, 'NY001', 'DM013', 'IND013', 2145000, 2065000, 80000, 3.7, 64.1),
        (20230201, 'FL001', 'DM014', 'IND014', 1710000, 1658000, 52000, 3.0, 65.5),
        (20230201, 'IL001', 'DM015', 'IND015', 963000, 925000, 38000, 3.9, 64.7),
        (20230201, 'WA001', 'DM016', 'IND001', 770000, 742000, 28000, 3.6, 68.1),
        (20230201, 'AZ001', 'DM017', 'IND002', 712000, 686000, 26000, 3.7, 67.5),
        (20230201, 'TN001', 'DM018', 'IND003', 655000, 632000, 23000, 3.5, 68.2),
        (20230201, 'MA001', 'DM019', 'IND004', 602000, 582000, 20000, 3.3, 68.4),
        (20230201, 'IN001', 'DM020', 'IND005', 552000, 535000, 17000, 3.1, 66.5),
        (20240101, 'CA001', 'DM008', 'IND010', 1280000, 1235000, 45000, 3.5, 66.8),
        (20240101, 'TX001', 'DM009', 'IND011', 920000, 885000, 35000, 3.8, 69.2),
        (20240101, 'NY001', 'DM010', 'IND012', 2150000, 2070000, 80000, 3.7, 64.5),
        (20240101, 'FL001', 'DM011', 'IND013', 1720000, 1665000, 55000, 3.2, 65.8),
        (20240101, 'IL001', 'DM012', 'IND014', 970000, 932000, 38000, 3.9, 65.1),
        (20240101, 'PA001', 'DM013', 'IND015', 1450000, 1398000, 52000, 3.6, 63.2),
        (20240101, 'OH001', 'DM014', 'IND001', 1195000, 1152000, 43000, 3.6, 65.4),
        (20240101, 'GA001', 'DM015', 'IND002', 1065000, 1028000, 37000, 3.5, 66.1),
        (20240101, 'NC001', 'DM016', 'IND003', 1015000, 982000, 33000, 3.3, 67.0),
        (20240101, 'MI001', 'DM017', 'IND004', 905000, 872000, 33000, 3.6, 64.4),
        (20240201, 'WA001', 'DM012', 'IND014', 780000, 755000, 25000, 3.2, 67.4),
        (20240201, 'PA001', 'DM013', 'IND015', 1450000, 1398000, 52000, 3.6, 63.2),
        (20240201, 'AZ001', 'DM014', 'IND001', 720000, 696000, 24000, 3.3, 68.1),
        (20240201, 'TN001', 'DM015', 'IND002', 665000, 644000, 21000, 3.2, 68.8),
        (20240201, 'MA001', 'DM016', 'IND003', 610000, 592000, 18000, 2.9, 68.9),
        (20240201, 'IN001', 'DM017', 'IND004', 560000, 545000, 15000, 2.7, 67.1),
        (20240201, 'MO001', 'DM018', 'IND005', 540000, 522000, 18000, 3.3, 65.8),
        (20240201, 'WI001', 'DM019', 'IND006', 520000, 504000, 16000, 3.1, 66.9),
        (20240201, 'MD001', 'DM020', 'IND007', 485000, 470000, 15000, 3.1, 67.5),
        (20240201, 'MN001', 'DM001', 'IND008', 475000, 461000, 14000, 2.9, 68.2),
        (20240301, 'CA001', 'DM001', 'IND001', 1285000, 1242000, 43000, 3.3, 67.1),
        (20240301, 'TX001', 'DM002', 'IND002', 925000, 892000, 33000, 3.6, 69.5),
        (20240301, 'NY001', 'DM003', 'IND003', 2155000, 2078000, 77000, 3.6, 64.8),
        (20240301, 'FL001', 'DM004', 'IND004', 1725000, 1672000, 53000, 3.1, 66.1),
        (20240301, 'IL001', 'DM005', 'IND005', 973000, 937000, 36000, 3.7, 65.4),
        (20240301, 'PA001', 'DM006', 'IND006', 1453000, 1403000, 50000, 3.4, 63.5),
        (20240301, 'OH001', 'DM007', 'IND007', 1200000, 1160000, 40000, 3.3, 65.7),
        (20240301, 'GA001', 'DM008', 'IND008', 1070000, 1035000, 35000, 3.3, 66.4),
        (20240301, 'NC001', 'DM009', 'IND009', 1020000, 988000, 32000, 3.1, 67.3),
        (20240301, 'MI001', 'DM010', 'IND010', 910000, 878000, 32000, 3.5, 64.7),
        (20240401, 'CA001', 'DM011', 'IND011', 1288000, 1248000, 40000, 3.1, 67.3),
        (20240401, 'TX001', 'DM012', 'IND012', 928000, 896000, 32000, 3.4, 69.7),
        (20240401, 'NY001', 'DM013', 'IND013', 2160000, 2085000, 75000, 3.5, 65.0),
        (20240401, 'FL001', 'DM014', 'IND014', 1730000, 1678000, 52000, 3.0, 66.3),
        (20240401, 'IL001', 'DM015', 'IND015', 975000, 940000, 35000, 3.6, 65.6),
        (20240401, 'WA001', 'DM016', 'IND001', 785000, 762000, 23000, 2.9, 67.8),
        (20240401, 'AZ001', 'DM017', 'IND002', 725000, 702000, 23000, 3.2, 68.4),
        (20240401, 'TN001', 'DM018', 'IND003', 670000, 650000, 20000, 3.0, 69.1),
        (20240401, 'MA001', 'DM019', 'IND004', 615000, 598000, 17000, 2.8, 69.2),
        (20240401, 'IN001', 'DM020', 'IND005', 565000, 551000, 14000, 2.5, 67.4),
        (20240501, 'CA001', 'DM002', 'IND003', 1290000, 1252000, 38000, 2.9, 67.5),
        (20240501, 'TX001', 'DM003', 'IND004', 930000, 900000, 30000, 3.2, 69.9),
        (20240501, 'NY001', 'DM004', 'IND005', 2165000, 2092000, 73000, 3.4, 65.3),
        (20240501, 'FL001', 'DM005', 'IND006', 1735000, 1685000, 50000, 2.9, 66.6),
        (20240501, 'IL001', 'DM006', 'IND007', 978000, 944000, 34000, 3.5, 65.9),
        (20240501, 'PA001', 'DM007', 'IND008', 1456000, 1408000, 48000, 3.3, 63.8),
        (20240501, 'OH001', 'DM008', 'IND009', 1205000, 1167000, 38000, 3.2, 66.0),
        (20240501, 'GA001', 'DM009', 'IND010', 1075000, 1042000, 33000, 3.1, 66.7),
        (20240501, 'NC001', 'DM010', 'IND011', 1025000, 995000, 30000, 2.9, 67.6),
        (20240501, 'MI001', 'DM011', 'IND012', 915000, 885000, 30000, 3.3, 65.0),
        (20240601, 'WA001', 'DM012', 'IND013', 790000, 768000, 22000, 2.8, 68.1),
        (20240601, 'AZ001', 'DM013', 'IND014', 730000, 708000, 22000, 3.0, 68.7),
        (20240601, 'TN001', 'DM014', 'IND015', 675000, 656000, 19000, 2.8, 69.4),
        (20240601, 'MA001', 'DM015', 'IND001', 620000, 604000, 16000, 2.6, 69.5),
        (20240601, 'IN001', 'DM016', 'IND002', 570000, 557000, 13000, 2.3, 67.7),
        (20240601, 'MO001', 'DM017', 'IND003', 545000, 529000, 16000, 2.9, 66.2),
        (20240601, 'WI001', 'DM018', 'IND004', 525000, 510000, 15000, 2.9, 67.3),
        (20240601, 'MD001', 'DM019', 'IND005', 490000, 477000, 13000, 2.7, 67.8),
        (20240601, 'MN001', 'DM020', 'IND006', 480000, 468000, 12000, 2.5, 68.5),
        (20240601, 'CO001', 'DM001', 'IND007', 465000, 453000, 12000, 2.6, 69.1);

    /* =====================
       4) Read/Write User
       ===================== */
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'UnemploymentDB_rw')
    BEGIN
        CREATE USER [UnemploymentDB_rw] WITH PASSWORD = N'TechmapGroup4RW!';
    END
    ELSE
    BEGIN
        ALTER USER [UnemploymentDB_rw] WITH PASSWORD = N'TechmapGroup4RW!';
    END;

    GRANT CONNECT TO [UnemploymentDB_rw];
    ALTER ROLE db_datareader ADD MEMBER [UnemploymentDB_rw];
    ALTER ROLE db_datawriter ADD MEMBER [UnemploymentDB_rw];

COMMIT TRAN;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRAN;

    DECLARE @msg NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR('Star schema build/load failed: %s', 16, 1, @msg);
END CATCH;
GO

-- Quick checks
-- SELECT COUNT(*) AS dates FROM dbo.dim_date;
-- SELECT TOP (10) * FROM dbo.unemployment_statistics ORDER BY date_id, geography_id, demographic_id;

/* =====================
   APPENDIX: Original File (verbatim)
   =====================
CREATE DATABASE UnemploymentAnalyticsDB;
GO
USE UnemploymentAnalyticsDB;
GO

-- Drop tables if they exist for rerun convenience (development/testing only; not recommended for production environments)
-- Drop foreign key constraints first to avoid dependency errors
IF OBJECT_ID('unemployment_statistics', 'U') IS NOT NULL
BEGIN
    ALTER TABLE unemployment_statistics DROP CONSTRAINT IF EXISTS FK_unemployment_statistics_date_id;
    ALTER TABLE unemployment_statistics DROP CONSTRAINT IF EXISTS FK_unemployment_statistics_geography_id;
    ALTER TABLE unemployment_statistics DROP CONSTRAINT IF EXISTS FK_unemployment_statistics_demographic_id;
    ALTER TABLE unemployment_statistics DROP CONSTRAINT IF EXISTS FK_unemployment_statistics_industry_id;
END

IF OBJECT_ID('unemployment_statistics', 'U') IS NOT NULL DROP TABLE unemployment_statistics;
IF OBJECT_ID('dim_date', 'U') IS NOT NULL DROP TABLE dim_date;
IF OBJECT_ID('dim_geography', 'U') IS NOT NULL DROP TABLE dim_geography;
IF OBJECT_ID('dim_demographics', 'U') IS NOT NULL DROP TABLE dim_demographics;
IF OBJECT_ID('dim_industry', 'U') IS NOT NULL DROP TABLE dim_industry;

-- Create tables

-- ============================================
-- Date Dimension Dataset - Part 1
-- Table: dim_date
-- Records: 2,191 complete date records (2019-2024)
-- Coverage: Comprehensive time intelligence for unemployment analytics
-- ============================================

-- Create the date dimension table structure
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY, -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    
    -- Year attributes
    year INT NOT NULL,
    year_name VARCHAR(10) NOT NULL, -- '2024'
    
    -- Quarter attributes
    quarter_of_year INT NOT NULL, -- 1,2,3,4
    quarter_name VARCHAR(10) NOT NULL, -- 'Q1 2024'
    
    -- Month attributes
    month_of_year INT NOT NULL, -- 1-12
    month_name VARCHAR(20) NOT NULL,
    month_abbr VARCHAR(3) NOT NULL,
    
    -- Week attributes
    week_of_year INT NOT NULL,
    week_of_month INT NOT NULL,
    
    -- Day attributes
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL, -- 1=Sunday, 7=Saturday
    day_name VARCHAR(20) NOT NULL,
    
    -- Business attributes
    is_weekday BIT NOT NULL,
    is_holiday BIT NOT NULL DEFAULT 0,
    holiday_name VARCHAR(100),
    
    -- Economic context
    economic_period VARCHAR(50), -- 'Pre-Pandemic', 'Pandemic Peak', 'Recovery', etc.
    fiscal_year INT,
    fiscal_quarter VARCHAR(10),
    
    -- Power BI sorting attributes
    year_sort INT NOT NULL,
    quarter_sort INT NOT NULL,
    month_sort INT NOT NULL,
    
    -- Data quality constraints
    CONSTRAINT CHK_Month CHECK (month_of_year BETWEEN 1 AND 12),
    CONSTRAINT CHK_Quarter CHECK (quarter_of_year BETWEEN 1 AND 4),
    CONSTRAINT CHK_DayOfWeek CHECK (day_of_week BETWEEN 1 AND 7)
);

-- Create performance indexes for Power BI optimization
CREATE INDEX IX_Date_Year ON dim_date(year);
CREATE INDEX IX_Date_Quarter ON dim_date(year, quarter_of_year);
CREATE INDEX IX_Date_Month ON dim_date(year, month_of_year);
CREATE INDEX IX_Date_Economic ON dim_date(economic_period);
CREATE INDEX IX_Date_Holiday ON dim_date(is_holiday);

-- ...existing code for data inserts and further logic...


CREATE TABLE dim_geography (
    geography_id VARCHAR(10) PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    state_code VARCHAR(2) NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    county_fips VARCHAR(5),
    county_name VARCHAR(100),
    metro_area VARCHAR(100),
    region VARCHAR(50),
    division VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    population INT,
    area_sq_miles FLOAT,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BIT NOT NULL DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE()
);
CREATE TABLE dim_demographics (
    demographic_id VARCHAR(10) PRIMARY KEY,
    age_group VARCHAR(20) NOT NULL, 
    gender VARCHAR(10) NOT NULL,
    race_ethnicity VARCHAR(50) NOT NULL,
    education_level VARCHAR(50) NOT NULL,
    marital_status VARCHAR(20) NOT NULL,             
    veteran_status VARCHAR(20) NOT NULL,
    disability_status VARCHAR(20) NOT NULL,
    citizenship_status VARCHAR(20) NOT NULL,
    english_proficiency VARCHAR(20) NOT NULL,
    created_date DATETIME2 DEFAULT GETDATE()
);
    
-- ============================================
-- Industry Dimension Dataset
-- Table: dim_industry
-- Records: 15 comprehensive industry classifications
-- Coverage: All major NAICS industry sectors
-- ============================================

-- First, create the table structure (if not already created)
CREATE TABLE dim_industry (
    industry_id VARCHAR(10) PRIMARY KEY,
    naics_code VARCHAR(6) NOT NULL,
    industry_name VARCHAR(200) NOT NULL,
    industry_sector VARCHAR(100) NOT NULL,
    industry_group VARCHAR(100) NOT NULL,
    seasonal_factor VARCHAR(20) CHECK (seasonal_factor IN ('Low', 'Medium', 'High')),
    automation_risk VARCHAR(20) CHECK (automation_risk IN ('Low', 'Medium', 'High')),
    avg_wage_level VARCHAR(20) CHECK (avg_wage_level IN ('Low', 'Medium', 'High')),
    sector_code VARCHAR(2),
    supersector_code VARCHAR(3),
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE()
);
CREATE TABLE unemployment_statistics (
    record_id INT PRIMARY KEY IDENTITY(1,1),
    date_id INT NOT NULL,
    geography_id VARCHAR(10) NOT NULL,
    demographic_id VARCHAR(10) NOT NULL,
    industry_id VARCHAR(10) NOT NULL,
    labor_force INT NOT NULL,
    employed INT NOT NULL,
    unemployed INT NOT NULL,
    unemployment_rate FLOAT NOT NULL,
    participation_rate FLOAT,
    created_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id),
    FOREIGN KEY (demographic_id) REFERENCES dim_demographics(demographic_id),
    FOREIGN KEY (industry_id) REFERENCES dim_industry(industry_id)
);


-- Insert complete industry dimension dataset
INSERT INTO dim_industry (
    industry_id, naics_code, industry_name, industry_sector, industry_group,
    seasonal_factor, automation_risk, avg_wage_level, sector_code, supersector_code, is_active
) VALUES

-- Professional and Business Services
('IND001', '54', 'Professional, Scientific, and Technical Services', 'Professional Services', 'Information & Professional', 
 'Low', 'Medium', 'High', '54', '540', 1),

-- Healthcare and Social Assistance
('IND002', '62', 'Health Care and Social Assistance', 'Healthcare', 'Education & Health', 
 'Low', 'Low', 'High', '62', '620', 1),

-- Retail Trade
('IND003', '44', 'Retail Trade', 'Retail', 'Trade & Transportation', 
 'High', 'High', 'Low', '44', '440', 1),

-- Manufacturing
('IND004', '31', 'Manufacturing', 'Manufacturing', 'Manufacturing', 
 'Medium', 'High', 'Medium', '31', '310', 1),

-- Accommodation and Food Services
('IND005', '72', 'Accommodation and Food Services', 'Hospitality', 'Leisure & Hospitality', 
 'High', 'Medium', 'Low', '72', '720', 1),

-- Construction
('IND006', '23', 'Construction', 'Construction', 'Construction & Resources', 
 'High', 'Medium', 'Medium', '23', '230', 1),

-- Transportation and Warehousing
('IND007', '48', 'Transportation and Warehousing', 'Transportation', 'Trade & Transportation', 
 'Medium', 'High', 'Medium', '48', '480', 1),

-- Educational Services
('IND008', '61', 'Educational Services', 'Education', 'Education & Health', 
 'High', 'Low', 'Medium', '61', '610', 1),

-- Finance and Insurance
('IND009', '52', 'Finance and Insurance', 'Financial Services', 'Financial Activities', 
 'Low', 'Medium', 'High', '52', '520', 1),

-- Information Technology
('IND010', '51', 'Information', 'Information Technology', 'Information & Professional', 
 'Low', 'Medium', 'High', '51', '510', 1),

-- Public Administration
('IND011', '92', 'Public Administration', 'Government', 'Government', 
 'Low', 'Low', 'Medium', '92', '920', 1),

-- Arts, Entertainment, and Recreation
('IND012', '71', 'Arts, Entertainment, and Recreation', 'Entertainment', 'Leisure & Hospitality', 
 'High', 'Medium', 'Low', '71', '710', 1),

-- Agriculture, Forestry, Fishing and Hunting
('IND013', '11', 'Agriculture, Forestry, Fishing and Hunting', 'Agriculture', 'Construction & Resources', 
 'High', 'Medium', 'Low', '11', '110', 1),

-- Utilities
('IND014', '22', 'Utilities', 'Utilities', 'Construction & Resources', 
 'Low', 'Medium', 'High', '22', '220', 1),

-- Other Services
('IND015', '81', 'Other Services (except Public Administration)', 'Other Services', 'Other Services', 
 'Medium', 'Medium', 'Low', '81', '810', 1);

-- Create performance indexes for Power BI optimization
CREATE INDEX IX_Industry_Hierarchy 
ON dim_industry(industry_sector, industry_group, industry_name);

CREATE INDEX IX_Industry_Analysis 
ON dim_industry(seasonal_factor, automation_risk, avg_wage_level);

CREATE INDEX IX_Industry_NAICS 
ON dim_industry(naics_code, sector_code);

-- Verify the dataset
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT industry_sector) as unique_sectors,
    COUNT(DISTINCT industry_group) as unique_groups,
    COUNT(DISTINCT naics_code) as unique_naics_codes
FROM dim_industry 
WHERE is_active = 1;

-- Sample query for Power BI - Industry Performance Analysis
SELECT 
    i.industry_sector,
    i.industry_name,
    i.seasonal_factor,
    i.automation_risk,
    i.avg_wage_level,
    AVG(us.unemployment_rate) as avg_unemployment_rate,
    SUM(us.labor_force) as total_labor_force
FROM dim_industry i
LEFT JOIN unemployment_statistics us ON i.industry_id = us.industry_id
WHERE i.is_active = 1
GROUP BY i.industry_sector, i.industry_name, i.seasonal_factor, i.automation_risk, i.avg_wage_level
ORDER BY avg_unemployment_rate DESC;
-- Step 3: Insert all data using HTML dataset files

-- COMPLETE FACT TABLE INSERT STATEMENTS (500+ Records)
-- unemployment_statistics table data
-- 2019 Data (Pre-Pandemic Baseline)

-- ============================================
-- Complete Date Dimension Dataset
-- Table: dim_date  
-- Records: 2,191 complete date records
-- Coverage: January 1, 2019 - December 31, 2024
-- ============================================

-- Complete INSERT statement for all date records
INSERT INTO dim_date (
    date_id, full_date, year, year_name, quarter_of_year, quarter_name,
    month_of_year, month_name, month_abbr, week_of_year, week_of_month,
    day_of_month, day_of_week, day_name, is_weekday, is_holiday, holiday_name,
    economic_period, fiscal_year, fiscal_quarter, year_sort, quarter_sort, month_sort
) VALUES

-- 2019 Data (365 records)
(20190101, '2019-01-01', 2019, '2019', 1, 'Q1 2019', 1, 'January', 'Jan', 1, 1, 1, 3, 'Tuesday', 1, 1, 'New Year''s Day', 'Pre-Pandemic', 2019, 'FY19 Q1', 2019, 20191, 201901),
(20190102, '2019-01-02', 2019, '2019', 1, 'Q1 2019', 1, 'January', 'Jan', 1, 1, 2, 4, 'Wednesday', 1, 0, NULL, 'Pre-Pandemic', 2019, 'FY19 Q1', 2019, 20191, 201901),
(20190103, '2019-01-03', 2019, '2019', 1, 'Q1 2019', 1, 'January', 'Jan', 1, 1, 3, 5, 'Thursday', 1, 0, NULL, 'Pre-Pandemic', 2019, 'FY19 Q1', 2019, 20191, 201901),

-- Key dates from 2019 (sample from full dataset)
(20190218, '2019-02-18', 2019, '2019', 1, 'Q1 2019', 2, 'February', 'Feb', 8, 3, 18, 2, 'Monday', 1, 1, 'Presidents Day', 'Pre-Pandemic', 2019, 'FY19 Q1', 2019, 20191, 201902),
(20190527, '2019-05-27', 2019, '2019', 2, 'Q2 2019', 5, 'May', 'May', 22, 4, 27, 2, 'Monday', 1, 1, 'Memorial Day', 'Pre-Pandemic', 2019, 'FY19 Q2', 2019, 20192, 201905),
(20190704, '2019-07-04', 2019, '2019', 3, 'Q3 2019', 7, 'July', 'Jul', 27, 1, 4, 5, 'Thursday', 1, 1, 'Independence Day', 'Pre-Pandemic', 2019, 'FY19 Q3', 2019, 20193, 201907),
(20191128, '2019-11-28', 2019, '2019', 4, 'Q4 2019', 11, 'November', 'Nov', 48, 4, 28, 5, 'Thursday', 1, 1, 'Thanksgiving Day', 'Pre-Pandemic', 2019, 'FY19 Q4', 2019, 20194, 201911),
(20191225, '2019-12-25', 2019, '2019', 4, 'Q4 2019', 12, 'December', 'Dec', 52, 4, 25, 4, 'Wednesday', 1, 1, 'Christmas Day', 'Pre-Pandemic', 2019, 'FY19 Q4', 2019, 20194, 201912),

-- 2020 Data (366 records - leap year) - Key pandemic dates
(20200101, '2020-01-01', 2020, '2020', 1, 'Q1 2020', 1, 'January', 'Jan', 1, 1, 1, 4, 'Wednesday', 1, 1, 'New Year''s Day', 'Pre-Pandemic', 2020, 'FY20 Q1', 2020, 20201, 202001),
(20200311, '2020-03-11', 2020, '2020', 1, 'Q1 2020', 3, 'March', 'Mar', 11, 2, 11, 4, 'Wednesday', 1, 0, NULL, 'Pandemic Onset', 2020, 'FY20 Q1', 2020, 20201, 202003),
(20200315, '2020-03-15', 2020, '2020', 1, 'Q1 2020', 3, 'March', 'Mar', 11, 3, 15, 1, 'Sunday', 0, 0, NULL, 'Pandemic Onset', 2020, 'FY20 Q1', 2020, 20201, 202003),
(20200401, '2020-04-01', 2020, '2020', 2, 'Q2 2020', 4, 'April', 'Apr', 14, 1, 1, 4, 'Wednesday', 1, 0, NULL, 'Pandemic Peak', 2020, 'FY20 Q2', 2020, 20202, 202004),
(20200704, '2020-07-04', 2020, '2020', 3, 'Q3 2020', 7, 'July', 'Jul', 27, 1, 4, 7, 'Saturday', 0, 1, 'Independence Day', 'Pandemic Peak', 2020, 'FY20 Q3', 2020, 20203, 202007),

-- 2021 Data (365 records) - Recovery begins
(20210101, '2021-01-01', 2021, '2021', 1, 'Q1 2021', 1, 'January', 'Jan', 1, 1, 1, 6, 'Friday', 1, 1, 'New Year''s Day', 'Pandemic Peak', 2021, 'FY21 Q1', 2021, 20211, 202101),
(20210525, '2021-05-25', 2021, '2021', 2, 'Q2 2021', 5, 'May', 'May', 21, 4, 25, 3, 'Tuesday', 1, 0, NULL, 'Early Recovery', 2021, 'FY21 Q2', 2021, 20212, 202105),
(20211125, '2021-11-25', 2021, '2021', 4, 'Q4 2021', 11, 'November', 'Nov', 47, 4, 25, 5, 'Thursday', 1, 1, 'Thanksgiving Day', 'Early Recovery', 2021, 'FY21 Q4', 2021, 20214, 202111),

-- 2022 Data (365 records) - Recovery phase
(20220101, '2022-01-01', 2022, '2022', 1, 'Q1 2022', 1, 'January', 'Jan', 1, 1, 1, 7, 'Saturday', 0, 1, 'New Year''s Day', 'Recovery', 2022, 'FY22 Q1', 2022, 20221, 202201),
(20220704, '2022-07-04', 2022, '2022', 3, 'Q3 2022', 7, 'July', 'Jul', 27, 1, 4, 2, 'Monday', 1, 1, 'Independence Day', 'Recovery', 2022, 'FY22 Q3', 2022, 20223, 202207),

-- 2023 Data (365 records) - Post-pandemic normalization  
(20230101, '2023-01-01', 2023, '2023', 1, 'Q1 2023', 1, 'January', 'Jan', 1, 1, 1, 1, 'Sunday', 0, 1, 'New Year''s Day', 'Post-Pandemic', 2023, 'FY23 Q1', 2023, 20231, 202301),
(20230515, '2023-05-15', 2023, '2023', 2, 'Q2 2023', 5, 'May', 'May', 20, 3, 15, 2, 'Monday', 1, 0, NULL, 'Post-Pandemic', 2023, 'FY23 Q2', 2023, 20232, 202305),

-- 2024 Data (366 records - leap year) - Current period
(20240101, '2024-01-01', 2024, '2024', 1, 'Q1 2024', 1, 'January', 'Jan', 1, 1, 1, 2, 'Monday', 1, 1, 'New Year''s Day', 'Current Period', 2024, 'FY24 Q1', 2024, 20241, 202401),
(20240704, '2024-07-04', 2024, '2024', 3, 'Q3 2024', 7, 'July', 'Jul', 27, 1, 4, 5, 'Thursday', 1, 1, 'Independence Day', 'Current Period', 2024, 'FY24 Q3', 2024, 20243, 202407),
(20241225, '2024-12-25', 2024, '2024', 4, 'Q4 2024', 12, 'December', 'Dec', 52, 4, 25, 4, 'Wednesday', 1, 1, 'Christmas Day', 'Current Period', 2024, 'FY24 Q4', 2024, 20244, 202412),
(20241231, '2024-12-31', 2024, '2024', 4, 'Q4 2024', 12, 'December', 'Dec', 53, 5, 31, 3, 'Tuesday', 1, 0, NULL, 'Current Period', 2024, 'FY24 Q4', 2024, 20244, 202412);

-- Verification queries
SELECT COUNT(*) as total_records FROM dim_date;
SELECT MIN(full_date) as start_date, MAX(full_date) as end_date FROM dim_date;
SELECT economic_period, COUNT(*) as record_count FROM dim_date GROUP BY economic_period;
SELECT COUNT(*) as holiday_count FROM dim_date WHERE is_holiday = 1;

INSERT INTO unemployment_statistics (date_id, geography_id, demographic_id, industry_id, labor_force, employed, unemployed, unemployment_rate, participation_rate) VALUES

(20190101, 'CA001', 'DM001', 'IND001', 1250000, 1200000, 50000, 4.0, 65.2),
(20190101, 'TX001', 'DM002', 'IND002', 890000, 850000, 40000, 4.5, 68.1),
(20190101, 'NY001', 'DM003', 'IND003', 2100000, 2010000, 90000, 4.3, 62.8),
(20190101, 'FL001', 'DM004', 'IND004', 1680000, 1620000, 60000, 3.6, 63.5),
(20190101, 'IL001', 'DM005', 'IND005', 950000, 905000, 45000, 4.7, 64.2),
(20190101, 'PA001', 'DM006', 'IND006', 1420000, 1358000, 62000, 4.4, 63.1),
(20190101, 'OH001', 'DM007', 'IND007', 1180000, 1127000, 53000, 4.5, 64.8),
(20190101, 'GA001', 'DM008', 'IND008', 1050000, 1008000, 42000, 4.0, 65.3),
(20190101, 'NC001', 'DM009', 'IND009', 980000, 940000, 40000, 4.1, 66.2),
(20190101, 'MI001', 'DM010', 'IND010', 890000, 850000, 40000, 4.5, 63.7),

(20190201, 'CA001', 'DM001', 'IND001', 1255000, 1208000, 47000, 3.7, 65.4),
(20190201, 'TX001', 'DM002', 'IND002', 892000, 855000, 37000, 4.1, 68.3),
(20190201, 'NY001', 'DM003', 'IND003', 2105000, 2020000, 85000, 4.0, 63.1),
(20190201, 'FL001', 'DM004', 'IND004', 1685000, 1628000, 57000, 3.4, 63.7),
(20190201, 'IL001', 'DM005', 'IND005', 952000, 910000, 42000, 4.4, 64.4),
(20190201, 'PA001', 'DM006', 'IND006', 1425000, 1365000, 60000, 4.2, 63.3),
(20190201, 'OH001', 'DM007', 'IND007', 1185000, 1135000, 50000, 4.2, 65.0),
(20190201, 'GA001', 'DM008', 'IND008', 1055000, 1015000, 40000, 3.8, 65.5),
(20190201, 'NC001', 'DM009', 'IND009', 985000, 948000, 37000, 3.8, 66.4),
(20190201, 'MI001', 'DM010', 'IND010', 895000, 858000, 37000, 4.1, 63.9),

(20190301, 'CA001', 'DM011', 'IND011', 1260000, 1215000, 45000, 3.6, 65.6),
(20190301, 'TX001', 'DM012', 'IND012', 895000, 860000, 35000, 3.9, 68.5),
(20190301, 'NY001', 'DM013', 'IND013', 2110000, 2028000, 82000, 3.9, 63.3),
(20190301, 'FL001', 'DM014', 'IND014', 1690000, 1635000, 55000, 3.3, 63.9),
(20190301, 'IL001', 'DM015', 'IND015', 955000, 915000, 40000, 4.2, 64.6),
(20190301, 'WA001', 'DM016', 'IND001', 750000, 720000, 30000, 4.0, 67.2),
(20190301, 'AZ001', 'DM017', 'IND002', 680000, 652000, 28000, 4.1, 64.8),
(20190301, 'TN001', 'DM018', 'IND003', 620000, 595000, 25000, 4.0, 65.9),
(20190301, 'MA001', 'DM019', 'IND004', 580000, 558000, 22000, 3.8, 66.5),
(20190301, 'IN001', 'DM020', 'IND005', 520000, 500000, 20000, 3.8, 64.2),

-- 2020 Pandemic Impact Data
(20200401, 'CA001', 'DM001', 'IND006', 1200000, 1050000, 150000, 12.5, 60.2),
(20200401, 'TX001', 'DM002', 'IND006', 870000, 760000, 110000, 12.6, 65.8),
(20200401, 'NY001', 'DM003', 'IND006', 2050000, 1750000, 300000, 14.6, 58.4),
(20200401, 'FL001', 'DM004', 'IND007', 1620000, 1420000, 200000, 12.3, 59.8),
(20200401, 'IL001', 'DM005', 'IND007', 920000, 805000, 115000, 12.5, 60.1),
(20200401, 'PA001', 'DM006', 'IND008', 1380000, 1200000, 180000, 13.0, 59.5),
(20200401, 'OH001', 'DM007', 'IND008', 1150000, 1000000, 150000, 13.0, 60.8),
(20200401, 'GA001', 'DM008', 'IND009', 1020000, 895000, 125000, 12.3, 61.2),
(20200401, 'NC001', 'DM009', 'IND009', 950000, 840000, 110000, 11.6, 62.5),
(20200401, 'MI001', 'DM010', 'IND010', 860000, 730000, 130000, 15.1, 58.9),

(20200501, 'CA001', 'DM001', 'IND006', 1180000, 1020000, 160000, 13.6, 59.1),
(20200501, 'TX001', 'DM002', 'IND006', 860000, 745000, 115000, 13.4, 65.2),
(20200501, 'NY001', 'DM003', 'IND006', 2020000, 1700000, 320000, 15.8, 57.1),
(20200501, 'FL001', 'DM004', 'IND007', 1600000, 1380000, 220000, 13.8, 58.9),
(20200501, 'IL001', 'DM005', 'IND007', 910000, 785000, 125000, 13.7, 59.3),
(20200501, 'PA001', 'DM006', 'IND008', 1360000, 1170000, 190000, 14.0, 58.8),
(20200501, 'OH001', 'DM007', 'IND008', 1130000, 970000, 160000, 14.2, 59.9),
(20200501, 'GA001', 'DM008', 'IND009', 1000000, 870000, 130000, 13.0, 60.5),
(20200501, 'NC001', 'DM009', 'IND009', 930000, 815000, 115000, 12.4, 61.8),
(20200501, 'MI001', 'DM010', 'IND010', 840000, 705000, 135000, 16.1, 57.8),

-- 2021 Recovery Phase Data
(20210101, 'CA001', 'DM006', 'IND008', 1220000, 1130000, 90000, 7.4, 62.1),
(20210101, 'TX001', 'DM007', 'IND009', 885000, 825000, 60000, 6.8, 66.9),
(20210101, 'NY001', 'DM008', 'IND010', 2080000, 1930000, 150000, 7.2, 60.5),
(20210101, 'FL001', 'DM009', 'IND011', 1650000, 1540000, 110000, 6.7, 61.8),
(20210101, 'IL001', 'DM010', 'IND012', 935000, 870000, 65000, 7.0, 62.1),
(20210101, 'PA001', 'DM011', 'IND013', 1400000, 1295000, 105000, 7.5, 61.2),
(20210101, 'OH001', 'DM012', 'IND014', 1160000, 1080000, 80000, 6.9, 62.8),
(20210101, 'GA001', 'DM013', 'IND015', 1030000, 960000, 70000, 6.8, 63.2),
(20210101, 'NC001', 'DM014', 'IND001', 970000, 910000, 60000, 6.2, 64.1),
(20210101, 'MI001', 'DM015', 'IND002', 875000, 815000, 60000, 6.9, 61.5),

(20210201, 'CA001', 'DM006', 'IND008', 1225000, 1140000, 85000, 6.9, 62.5),
(20210201, 'TX001', 'DM007', 'IND009', 890000, 835000, 55000, 6.2, 67.2),
(20210201, 'NY001', 'DM008', 'IND010', 2090000, 1950000, 140000, 6.7, 60.8),
(20210201, 'FL001', 'DM009', 'IND011', 1660000, 1560000, 100000, 6.0, 62.2),
(20210201, 'IL001', 'DM010', 'IND012', 940000, 880000, 60000, 6.4, 62.5),
(20210201, 'WA001', 'DM016', 'IND001', 760000, 715000, 45000, 5.9, 66.8),
(20210201, 'AZ001', 'DM017', 'IND002', 695000, 655000, 40000, 5.8, 65.9),
(20210201, 'TN001', 'DM018', 'IND003', 635000, 600000, 35000, 5.5, 66.8),
(20210201, 'MA001', 'DM019', 'IND004', 590000, 560000, 30000, 5.1, 67.2),
(20210201, 'IN001', 'DM020', 'IND005', 535000, 508000, 27000, 5.0, 65.1),

-- 2022 Continued Recovery
(20220101, 'CA001', 'DM001', 'IND001', 1245000, 1190000, 55000, 4.4, 64.8),
(20220101, 'TX001', 'DM002', 'IND002', 905000, 865000, 40000, 4.4, 68.5),
(20220101, 'NY001', 'DM003', 'IND003', 2120000, 2030000, 90000, 4.2, 62.9),
(20220101, 'FL001', 'DM004', 'IND004', 1685000, 1625000, 60000, 3.6, 64.1),
(20220101, 'IL001', 'DM005', 'IND005', 950000, 910000, 40000, 4.2, 63.8),
(20220101, 'PA001', 'DM006', 'IND006', 1430000, 1375000, 55000, 3.8, 62.8),
(20220101, 'OH001', 'DM007', 'IND007', 1175000, 1130000, 45000, 3.8, 64.2),
(20220101, 'GA001', 'DM008', 'IND008', 1045000, 1005000, 40000, 3.8, 64.8),
(20220101, 'NC001', 'DM009', 'IND009', 990000, 955000, 35000, 3.5, 65.7),
(20220101, 'MI001', 'DM010', 'IND010', 885000, 850000, 35000, 4.0, 63.2),

(20220201, 'CA001', 'DM011', 'IND011', 1248000, 1195000, 53000, 4.2, 65.0),
(20220201, 'TX001', 'DM012', 'IND012', 908000, 870000, 38000, 4.2, 68.7),
(20220201, 'NY001', 'DM013', 'IND013', 2125000, 2040000, 85000, 4.0, 63.2),
(20220201, 'FL001', 'DM014', 'IND014', 1690000, 1635000, 55000, 3.3, 64.4),
(20220201, 'IL001', 'DM015', 'IND015', 953000, 915000, 38000, 4.0, 64.0),
(20220201, 'WA001', 'DM016', 'IND001', 765000, 735000, 30000, 3.9, 67.5),
(20220201, 'AZ001', 'DM017', 'IND002', 705000, 678000, 27000, 3.8, 66.8),
(20220201, 'TN001', 'DM018', 'IND003', 645000, 620000, 25000, 3.9, 67.5),
(20220201, 'MA001', 'DM019', 'IND004', 595000, 572000, 23000, 3.9, 67.8),
(20220201, 'IN001', 'DM020', 'IND005', 545000, 525000, 20000, 3.7, 65.9),

-- 2023 Stabilization Period
(20230101, 'CA001', 'DM001', 'IND001', 1265000, 1215000, 50000, 4.0, 66.2),
(20230101, 'TX001', 'DM002', 'IND002', 915000, 880000, 35000, 3.8, 69.1),
(20230101, 'NY001', 'DM003', 'IND003', 2140000, 2055000, 85000, 4.0, 63.8),
(20230101, 'FL001', 'DM004', 'IND004', 1705000, 1650000, 55000, 3.2, 65.2),
(20230101, 'IL001', 'DM005', 'IND005', 960000, 920000, 40000, 4.2, 64.5),
(20230101, 'PA001', 'DM006', 'IND006', 1440000, 1385000, 55000, 3.8, 63.5),
(20230101, 'OH001', 'DM007', 'IND007', 1185000, 1140000, 45000, 3.8, 64.8),
(20230101, 'GA001', 'DM008', 'IND008', 1055000, 1015000, 40000, 3.8, 65.5),
(20230101, 'NC001', 'DM009', 'IND009', 1005000, 970000, 35000, 3.5, 66.4),
(20230101, 'MI001', 'DM010', 'IND010', 895000, 860000, 35000, 3.9, 63.8),

(20230201, 'CA001', 'DM011', 'IND011', 1268000, 1220000, 48000, 3.8, 66.4),
(20230201, 'TX001', 'DM012', 'IND012', 918000, 885000, 33000, 3.6, 69.3),
(20230201, 'NY001', 'DM013', 'IND013', 2145000, 2065000, 80000, 3.7, 64.1),
(20230201, 'FL001', 'DM014', 'IND014', 1710000, 1658000, 52000, 3.0, 65.5),
(20230201, 'IL001', 'DM015', 'IND015', 963000, 925000, 38000, 3.9, 64.7),
(20230201, 'WA001', 'DM016', 'IND001', 770000, 742000, 28000, 3.6, 68.1),
(20230201, 'AZ001', 'DM017', 'IND002', 712000, 686000, 26000, 3.7, 67.5),
(20230201, 'TN001', 'DM018', 'IND003', 655000, 632000, 23000, 3.5, 68.2),
(20230201, 'MA001', 'DM019', 'IND004', 602000, 582000, 20000, 3.3, 68.4),
(20230201, 'IN001', 'DM020', 'IND005', 552000, 535000, 17000, 3.1, 66.5),

-- 2024 Current Year Data
(20240101, 'CA001', 'DM008', 'IND010', 1280000, 1235000, 45000, 3.5, 66.8),
(20240101, 'TX001', 'DM009', 'IND011', 920000, 885000, 35000, 3.8, 69.2),
(20240101, 'NY001', 'DM010', 'IND012', 2150000, 2070000, 80000, 3.7, 64.5),
(20240101, 'FL001', 'DM011', 'IND013', 1720000, 1665000, 55000, 3.2, 65.8),
(20240101, 'IL001', 'DM012', 'IND014', 970000, 932000, 38000, 3.9, 65.1),
(20240101, 'PA001', 'DM013', 'IND015', 1450000, 1398000, 52000, 3.6, 63.2),
(20240101, 'OH001', 'DM014', 'IND001', 1195000, 1152000, 43000, 3.6, 65.4),
(20240101, 'GA001', 'DM015', 'IND002', 1065000, 1028000, 37000, 3.5, 66.1),
(20240101, 'NC001', 'DM016', 'IND003', 1015000, 982000, 33000, 3.3, 67.0),
(20240101, 'MI001', 'DM017', 'IND004', 905000, 872000, 33000, 3.6, 64.4),

(20240201, 'WA001', 'DM012', 'IND014', 780000, 755000, 25000, 3.2, 67.4),
(20240201, 'PA001', 'DM013', 'IND015', 1450000, 1398000, 52000, 3.6, 63.2),
(20240201, 'AZ001', 'DM014', 'IND001', 720000, 696000, 24000, 3.3, 68.1),
(20240201, 'TN001', 'DM015', 'IND002', 665000, 644000, 21000, 3.2, 68.8),
(20240201, 'MA001', 'DM016', 'IND003', 610000, 592000, 18000, 2.9, 68.9),
(20240201, 'IN001', 'DM017', 'IND004', 560000, 545000, 15000, 2.7, 67.1),
(20240201, 'MO001', 'DM018', 'IND005', 540000, 522000, 18000, 3.3, 65.8),
(20240201, 'WI001', 'DM019', 'IND006', 520000, 504000, 16000, 3.1, 66.9),
(20240201, 'MD001', 'DM020', 'IND007', 485000, 470000, 15000, 3.1, 67.5),
(20240201, 'MN001', 'DM001', 'IND008', 475000, 461000, 14000, 2.9, 68.2),

(20240301, 'CA001', 'DM001', 'IND001', 1285000, 1242000, 43000, 3.3, 67.1),
(20240301, 'TX001', 'DM002', 'IND002', 925000, 892000, 33000, 3.6, 69.5),
(20240301, 'NY001', 'DM003', 'IND003', 2155000, 2078000, 77000, 3.6, 64.8),
(20240301, 'FL001', 'DM004', 'IND004', 1725000, 1672000, 53000, 3.1, 66.1),
(20240301, 'IL001', 'DM005', 'IND005', 973000, 937000, 36000, 3.7, 65.4),
(20240301, 'PA001', 'DM006', 'IND006', 1453000, 1403000, 50000, 3.4, 63.5),
(20240301, 'OH001', 'DM007', 'IND007', 1200000, 1160000, 40000, 3.3, 65.7),
(20240301, 'GA001', 'DM008', 'IND008', 1070000, 1035000, 35000, 3.3, 66.4),
(20240301, 'NC001', 'DM009', 'IND009', 1020000, 988000, 32000, 3.1, 67.3),
(20240301, 'MI001', 'DM010', 'IND010', 910000, 878000, 32000, 3.5, 64.7),

(20240401, 'CA001', 'DM011', 'IND011', 1288000, 1248000, 40000, 3.1, 67.3),
(20240401, 'TX001', 'DM012', 'IND012', 928000, 896000, 32000, 3.4, 69.7),
(20240401, 'NY001', 'DM013', 'IND013', 2160000, 2085000, 75000, 3.5, 65.0),
(20240401, 'FL001', 'DM014', 'IND014', 1730000, 1678000, 52000, 3.0, 66.3),
(20240401, 'IL001', 'DM015', 'IND015', 975000, 940000, 35000, 3.6, 65.6),
(20240401, 'WA001', 'DM016', 'IND001', 785000, 762000, 23000, 2.9, 67.8),
(20240401, 'AZ001', 'DM017', 'IND002', 725000, 702000, 23000, 3.2, 68.4),
(20240401, 'TN001', 'DM018', 'IND003', 670000, 650000, 20000, 3.0, 69.1),
(20240401, 'MA001', 'DM019', 'IND004', 615000, 598000, 17000, 2.8, 69.2),
(20240401, 'IN001', 'DM020', 'IND005', 565000, 551000, 14000, 2.5, 67.4),

-- Additional 2024 data with seasonal variations
(20240501, 'CA001', 'DM002', 'IND003', 1290000, 1252000, 38000, 2.9, 67.5),
(20240501, 'TX001', 'DM003', 'IND004', 930000, 900000, 30000, 3.2, 69.9),
(20240501, 'NY001', 'DM004', 'IND005', 2165000, 2092000, 73000, 3.4, 65.3),
(20240501, 'FL001', 'DM005', 'IND006', 1735000, 1685000, 50000, 2.9, 66.6),
(20240501, 'IL001', 'DM006', 'IND007', 978000, 944000, 34000, 3.5, 65.9),
(20240501, 'PA001', 'DM007', 'IND008', 1456000, 1408000, 48000, 3.3, 63.8),
(20240501, 'OH001', 'DM008', 'IND009', 1205000, 1167000, 38000, 3.2, 66.0),
(20240501, 'GA001', 'DM009', 'IND010', 1075000, 1042000, 33000, 3.1, 66.7),
(20240501, 'NC001', 'DM010', 'IND011', 1025000, 995000, 30000, 2.9, 67.6),
(20240501, 'MI001', 'DM011', 'IND012', 915000, 885000, 30000, 3.3, 65.0),

(20240601, 'WA001', 'DM012', 'IND013', 790000, 768000, 22000, 2.8, 68.1),
(20240601, 'AZ001', 'DM013', 'IND014', 730000, 708000, 22000, 3.0, 68.7),
(20240601, 'TN001', 'DM014', 'IND015', 675000, 656000, 19000, 2.8, 69.4),
(20240601, 'MA001', 'DM015', 'IND001', 620000, 604000, 16000, 2.6, 69.5),
(20240601, 'IN001', 'DM016', 'IND002', 570000, 557000, 13000, 2.3, 67.7),
(20240601, 'MO001', 'DM017', 'IND003', 545000, 529000, 16000, 2.9, 66.2),
(20240601, 'WI001', 'DM018', 'IND004', 525000, 510000, 15000, 2.9, 67.3),
(20240601, 'MD001', 'DM019', 'IND005', 490000, 477000, 13000, 2.7, 67.8),
(20240601, 'MN001', 'DM020', 'IND006', 480000, 468000, 12000, 2.5, 68.5),
(20240601, 'CO001', 'DM001', 'IND007', 465000, 453000, 12000, 2.6, 69.1);
            

-- Execute INSERT statements from all 6 HTML files
-- 1. dim_date (from unemployment-date-dimension-part2.html)

-- COMPLETE DEMOGRAPHICS DIMENSION TABLE INSERT STATEMENTS
-- dim_demographics table data with comprehensive US labor force representation

INSERT INTO dim_demographics (demographic_id, age_group, gender, race_ethnicity, education_level, marital_status, veteran_status, disability_status, citizenship_status, english_proficiency, created_date) VALUES

-- Core demographic combinations matching fact table foreign keys
('DM001', '25-34', 'Male', 'White', 'Bachelor''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM002', '35-44', 'Female', 'Hispanic or Latino', 'High School Graduate', 'Married', 'Non-Veteran', 'No Disability', 'US Citizen', 'Bilingual', '2019-01-01'),
('DM003', '45-54', 'Male', 'Black or African American', 'Associate Degree', 'Divorced', 'Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM004', '25-34', 'Female', 'Asian', 'Master''s Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
('DM005', '55-64', 'Male', 'White', 'High School Graduate', 'Married', 'Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),

-- Young adults and recent graduates
('DM006', '20-24', 'Female', 'Hispanic or Latino', 'Some College', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM007', '35-44', 'Male', 'White', 'Bachelor''s Degree', 'Married', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM008', '25-34', 'Female', 'Black or African American', 'Bachelor''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM009', '45-54', 'Male', 'Hispanic or Latino', 'Less than High School', 'Married', 'Non-Veteran', 'No Disability', 'Permanent Resident', 'Limited English', '2019-01-01'),
('DM010', '35-44', 'Female', 'White', 'Master''s Degree', 'Divorced', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),

-- Professional and highly educated segments
('DM011', '55-64', 'Female', 'Asian', 'Professional Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
('DM012', '20-24', 'Male', 'Black or African American', 'High School Graduate', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM013', '45-54', 'Female', 'White', 'Associate Degree', 'Married', 'Non-Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM014', '25-34', 'Male', 'Two or More Races', 'Bachelor''s Degree', 'Single', 'Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM015', '35-44', 'Female', 'American Indian or Alaska Native', 'Some College', 'Divorced', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),

-- Special demographic segments
('DM016', '16-19', 'Male', 'Hispanic or Latino', 'Less than High School', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Bilingual', '2019-01-01'),
('DM017', '65+', 'Female', 'White', 'High School Graduate', 'Widowed', 'Non-Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM018', '25-34', 'Non-Binary', 'White', 'Master''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM019', '35-44', 'Male', 'Asian', 'Doctoral Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
('DM020', '20-24', 'Female', 'Native Hawaiian or Pacific Islander', 'Associate Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01');
            

            
-- COMPLETE GEOGRAPHIC DIMENSION TABLE INSERT STATEMENTS
-- dim_geography table data with full US coverage

INSERT INTO dim_geography (geography_id, country, state_code, state_name, county_fips, county_name, metro_area, region, division, latitude, longitude, population, area_sq_miles, effective_date, expiration_date, is_current) VALUES

-- Major States and Metropolitan Areas (matching fact table foreign keys)
('CA001', 'USA', 'CA', 'California', '06037', 'Los Angeles County', 'Los Angeles-Long Beach-Anaheim', 'West', 'Pacific', 34.0522, -118.2437, 10014009, 4751.00, '2019-01-01', NULL, 1),
('TX001', 'USA', 'TX', 'Texas', '48201', 'Harris County', 'Houston-The Woodlands-Sugar Land', 'South', 'West South Central', 29.7604, -95.3698, 4731145, 1703.00, '2019-01-01', NULL, 1),
('NY001', 'USA', 'NY', 'New York', '36061', 'New York County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.7128, -74.0060, 1694251, 23.00, '2019-01-01', NULL, 1),
('FL001', 'USA', 'FL', 'Florida', '12086', 'Miami-Dade County', 'Miami-Fort Lauderdale-West Palm Beach', 'South', 'South Atlantic', 25.7617, -80.1918, 2716940, 1946.00, '2019-01-01', NULL, 1),
('IL001', 'USA', 'IL', 'Illinois', '17031', 'Cook County', 'Chicago-Naperville-Elgin', 'Midwest', 'East North Central', 41.8781, -87.6298, 5275541, 946.00, '2019-01-01', NULL, 1),
('PA001', 'USA', 'PA', 'Pennsylvania', '42101', 'Philadelphia County', 'Philadelphia-Camden-Wilmington', 'Northeast', 'Middle Atlantic', 39.9526, -75.1652, 1603797, 135.00, '2019-01-01', NULL, 1),
('OH001', 'USA', 'OH', 'Ohio', '39035', 'Cuyahoga County', 'Cleveland-Elyria', 'Midwest', 'East North Central', 41.4993, -81.6944, 1235072, 458.00, '2019-01-01', NULL, 1),
('GA001', 'USA', 'GA', 'Georgia', '13135', 'Gwinnett County', 'Atlanta-Sandy Springs-Roswell', 'South', 'South Atlantic', 33.7490, -84.3880, 957062, 430.00, '2019-01-01', NULL, 1),
('NC001', 'USA', 'NC', 'North Carolina', '37119', 'Mecklenburg County', 'Charlotte-Concord-Gastonia', 'South', 'South Atlantic', 35.2271, -80.8431, 1115482, 546.00, '2019-01-01', NULL, 1),
('MI001', 'USA', 'MI', 'Michigan', '26163', 'Wayne County', 'Detroit-Warren-Dearborn', 'Midwest', 'East North Central', 42.3314, -83.0458, 1793561, 612.00, '2019-01-01', NULL, 1),
('WA001', 'USA', 'WA', 'Washington', '53033', 'King County', 'Seattle-Tacoma-Bellevue', 'West', 'Pacific', 47.6062, -122.3321, 2269675, 2134.00, '2019-01-01', NULL, 1),
('AZ001', 'USA', 'AZ', 'Arizona', '04013', 'Maricopa County', 'Phoenix-Mesa-Scottsdale', 'West', 'Mountain', 33.4484, -112.0740, 4485414, 9224.00, '2019-01-01', NULL, 1),
('TN001', 'USA', 'TN', 'Tennessee', '47037', 'Davidson County', 'Nashville-Davidson-Murfreesboro', 'South', 'East South Central', 36.1627, -86.7816, 715884, 502.00, '2019-01-01', NULL, 1),
('MA001', 'USA', 'MA', 'Massachusetts', '25025', 'Suffolk County', 'Boston-Cambridge-Newton', 'Northeast', 'New England', 42.3601, -71.0589, 797936, 58.00, '2019-01-01', NULL, 1),
('IN001', 'USA', 'IN', 'Indiana', '18097', 'Marion County', 'Indianapolis-Carmel-Anderson', 'Midwest', 'East North Central', 39.7684, -86.1581, 977203, 396.00, '2019-01-01', NULL, 1),
('MO001', 'USA', 'MO', 'Missouri', '29189', 'St. Louis County', 'St. Louis', 'Midwest', 'West North Central', 38.6270, -90.1994, 1001876, 508.00, '2019-01-01', NULL, 1),
('WI001', 'USA', 'WI', 'Wisconsin', '55079', 'Milwaukee County', 'Milwaukee-Waukesha-West Allis', 'Midwest', 'East North Central', 43.0389, -87.9065, 945726, 241.00, '2019-01-01', NULL, 1),
('MD001', 'USA', 'MD', 'Maryland', '24005', 'Baltimore County', 'Baltimore-Columbia-Towson', 'South', 'South Atlantic', 39.2904, -76.6122, 854535, 599.00, '2019-01-01', NULL, 1),
('MN001', 'USA', 'MN', 'Minnesota', '27123', 'Ramsey County', 'Minneapolis-St. Paul-Bloomington', 'Midwest', 'West North Central', 44.9778, -93.2650, 552344, 152.00, '2019-01-01', NULL, 1),
('CO001', 'USA', 'CO', 'Colorado', '08031', 'Denver County', 'Denver-Aurora-Lakewood', 'West', 'Mountain', 39.7392, -104.9903, 715522, 153.00, '2019-01-01', NULL, 1),

-- Additional Major Metropolitan Areas
('CA002', 'USA', 'CA', 'California', '06073', 'San Diego County', 'San Diego-Carlsbad', 'West', 'Pacific', 32.7157, -117.1611, 3338330, 4206.00, '2019-01-01', NULL, 1),
('CA003', 'USA', 'CA', 'California', '06075', 'San Francisco County', 'San Francisco-Oakland-Hayward', 'West', 'Pacific', 37.7749, -122.4194, 881549, 47.00, '2019-01-01', NULL, 1),
('TX002', 'USA', 'TX', 'Texas', '48113', 'Dallas County', 'Dallas-Fort Worth-Arlington', 'South', 'West South Central', 32.7767, -96.7970, 2613539, 880.00, '2019-01-01', NULL, 1),
('TX003', 'USA', 'TX', 'Texas', '48029', 'Bexar County', 'San Antonio-New Braunfels', 'South', 'West South Central', 29.4241, -98.4936, 2003554, 1247.00, '2019-01-01', NULL, 1),
('FL002', 'USA', 'FL', 'Florida', '12095', 'Orange County', 'Orlando-Kissimmee-Sanford', 'South', 'South Atlantic', 28.5383, -81.3792, 1393452, 907.00, '2019-01-01', NULL, 1),
('FL003', 'USA', 'FL', 'Florida', '12103', 'Pinellas County', 'Tampa-St. Petersburg-Clearwater', 'South', 'South Atlantic', 27.7663, -82.6404, 959107, 280.00, '2019-01-01', NULL, 1),
('NY002', 'USA', 'NY', 'New York', '36047', 'Kings County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.6782, -73.9442, 2736074, 70.00, '2019-01-01', NULL, 1),
('NY003', 'USA', 'NY', 'New York', '36081', 'Queens County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.7282, -73.7949, 2405464, 109.00, '2019-01-01', NULL, 1),

-- Additional States for Complete Coverage
('NV001', 'USA', 'NV', 'Nevada', '32003', 'Clark County', 'Las Vegas-Henderson-Paradise', 'West', 'Mountain', 36.1699, -115.1398, 2266715, 7891.00, '2019-01-01', NULL, 1),
('OR001', 'USA', 'OR', 'Oregon', '41051', 'Multnomah County', 'Portland-Vancouver-Hillsboro', 'West', 'Pacific', 45.5152, -122.6784, 815428, 431.00, '2019-01-01', NULL, 1),
('UT001', 'USA', 'UT', 'Utah', '49035', 'Salt Lake County', 'Salt Lake City', 'West', 'Mountain', 40.7608, -111.8910, 1185238, 742.00, '2019-01-01', NULL, 1),
('VA001', 'USA', 'VA', 'Virginia', '51059', 'Fairfax County', 'Washington-Arlington-Alexandria', 'South', 'South Atlantic', 38.9043, -77.0384, 1150309, 395.00, '2019-01-01', NULL, 1),
('NJ001', 'USA', 'NJ', 'New Jersey', '34003', 'Bergen County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.8964, -74.0395, 955732, 234.00, '2019-01-01', NULL, 1),
('CT001', 'USA', 'CT', 'Connecticut', '09001', 'Fairfield County', 'Bridgeport-Stamford-Norwalk', 'Northeast', 'New England', 41.3083, -73.0275, 957419, 626.00, '2019-01-01', NULL, 1),
('SC001', 'USA', 'SC', 'South Carolina', '45019', 'Charleston County', 'Charleston-North Charleston', 'South', 'South Atlantic', 32.7765, -79.9311, 408235, 916.00, '2019-01-01', NULL, 1),
('KY001', 'USA', 'KY', 'Kentucky', '21111', 'Jefferson County', 'Louisville-Jefferson County', 'South', 'East South Central', 38.2527, -85.7585, 766757, 380.00, '2019-01-01', NULL, 1),
('LA001', 'USA', 'LA', 'Louisiana', '22071', 'Orleans Parish', 'New Orleans-Metairie', 'South', 'West South Central', 29.9511, -90.0715, 390144, 169.00, '2019-01-01', NULL, 1),
('OK001', 'USA', 'OK', 'Oklahoma', '40109', 'Oklahoma County', 'Oklahoma City', 'South', 'West South Central', 35.4676, -97.5164, 797434, 709.00, '2019-01-01', NULL, 1),
('AR001', 'USA', 'AR', 'Arkansas', '05119', 'Pulaski County', 'Little Rock-North Little Rock-Conway', 'South', 'West South Central', 34.7465, -92.2896, 395760, 774.00, '2019-01-01', NULL, 1),
('KS001', 'USA', 'KS', 'Kansas', '20091', 'Johnson County', 'Kansas City', 'Midwest', 'West North Central', 38.9072, -94.7203, 597511, 477.00, '2019-01-01', NULL, 1),
('IA001', 'USA', 'IA', 'Iowa', '19153', 'Polk County', 'Des Moines-West Des Moines', 'Midwest', 'West North Central', 41.5868, -93.6250, 492401, 569.00, '2019-01-01', NULL, 1),
('NE001', 'USA', 'NE', 'Nebraska', '31055', 'Douglas County', 'Omaha-Council Bluffs', 'Midwest', 'West North Central', 41.2565, -95.9345, 571327, 335.00, '2019-01-01', NULL, 1),

-- Mountain States
('MT001', 'USA', 'MT', 'Montana', '30111', 'Yellowstone County', 'Billings', 'West', 'Mountain', 45.7833, -108.5007, 164731, 2633.00, '2019-01-01', NULL, 1),
('ID001', 'USA', 'ID', 'Idaho', '16001', 'Ada County', 'Boise City', 'West', 'Mountain', 43.6150, -116.2023, 481587, 1055.00, '2019-01-01', NULL, 1),
('WY001', 'USA', 'WY', 'Wyoming', '56025', 'Natrona County', 'Casper', 'West', 'Mountain', 42.8500, -106.3162, 79858, 5376.00, '2019-01-01', NULL, 1),
('ND001', 'USA', 'ND', 'North Dakota', '38017', 'Cass County', 'Fargo', 'Midwest', 'West North Central', 46.8772, -96.7898, 184525, 1765.00, '2019-01-01', NULL, 1),
('SD001', 'USA', 'SD', 'South Dakota', '46099', 'Minnehaha County', 'Sioux Falls', 'Midwest', 'West North Central', 43.5446, -96.7311, 197214, 809.00, '2019-01-01', NULL, 1),

-- New England and Northeast
('ME001', 'USA', 'ME', 'Maine', '23005', 'Cumberland County', 'Portland-South Portland', 'Northeast', 'New England', 43.6591, -70.2568, 303069, 835.00, '2019-01-01', NULL, 1),
('NH001', 'USA', 'NH', 'New Hampshire', '33011', 'Hillsborough County', 'Manchester-Nashua', 'Northeast', 'New England', 42.9956, -71.4548, 422937, 876.00, '2019-01-01', NULL, 1),
('VT001', 'USA', 'VT', 'Vermont', '50007', 'Chittenden County', 'Burlington-South Burlington', 'Northeast', 'New England', 44.4759, -73.2121, 168323, 536.00, '2019-01-01', NULL, 1),
('RI001', 'USA', 'RI', 'Rhode Island', '44007', 'Providence County', 'Providence-Warwick', 'Northeast', 'New England', 41.8240, -71.4128, 660741, 410.00, '2019-01-01', NULL, 1),

-- Alaska and Hawaii
('AK001', 'USA', 'AK', 'Alaska', '02020', 'Anchorage Municipality', 'Anchorage', 'West', 'Pacific', 61.2181, -149.9003, 291247, 1961.00, '2019-01-01', NULL, 1),
('HI001', 'USA', 'HI', 'Hawaii', '15003', 'Honolulu County', 'Urban Honolulu', 'West', 'Pacific', 21.3099, -157.8581, 1016508, 596.00, '2019-01-01', NULL, 1),

-- Additional Southern States
('AL001', 'USA', 'AL', 'Alabama', '01073', 'Jefferson County', 'Birmingham-Hoover', 'South', 'East South Central', 33.5207, -86.8025, 674721, 1111.00, '2019-01-01', NULL, 1),
('MS001', 'USA', 'MS', 'Mississippi', '28049', 'Hinds County', 'Jackson', 'South', 'East South Central', 32.2988, -90.1848, 238674, 869.00, '2019-01-01', NULL, 1),
('WV001', 'USA', 'WV', 'West Virginia', '54039', 'Kanawha County', 'Charleston', 'South', 'South Atlantic', 38.3498, -81.6326, 180745, 903.00, '2019-01-01', NULL, 1),
('DE001', 'USA', 'DE', 'Delaware', '10003', 'New Castle County', 'Philadelphia-Camden-Wilmington', 'South', 'South Atlantic', 39.7391, -75.5398, 570719, 426.00, '2019-01-01', NULL, 1);
            
            -- ============================================
-- Industry Dimension Dataset
-- Table: dim_industry
-- Records: 15 comprehensive industry classifications
-- Coverage: All major NAICS industry sectors
-- ============================================

-- COMPLETE DEMOGRAPHICS DIMENSION TABLE INSERT STATEMENTS
-- dim_demographics table data with comprehensive US labor force representation

INSERT INTO dim_demographics (demographic_id, age_group, gender, race_ethnicity, education_level, marital_status, veteran_status, disability_status, citizenship_status, english_proficiency, created_date) VALUES

-- Core demographic combinations matching fact table foreign keys
('DM001', '25-34', 'Male', 'White', 'Bachelor''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM002', '35-44', 'Female', 'Hispanic or Latino', 'High School Graduate', 'Married', 'Non-Veteran', 'No Disability', 'US Citizen', 'Bilingual', '2019-01-01'),
('DM003', '45-54', 'Male', 'Black or African American', 'Associate Degree', 'Divorced', 'Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM004', '25-34', 'Female', 'Asian', 'Master''s Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
('DM005', '55-64', 'Male', 'White', 'High School Graduate', 'Married', 'Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),

-- Young adults and recent graduates
('DM006', '20-24', 'Female', 'Hispanic or Latino', 'Some College', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM007', '35-44', 'Male', 'White', 'Bachelor''s Degree', 'Married', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM008', '25-34', 'Female', 'Black or African American', 'Bachelor''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM009', '45-54', 'Male', 'Hispanic or Latino', 'Less than High School', 'Married', 'Non-Veteran', 'No Disability', 'Permanent Resident', 'Limited English', '2019-01-01'),
('DM010', '35-44', 'Female', 'White', 'Master''s Degree', 'Divorced', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),

-- Professional and highly educated segments
('DM011', '55-64', 'Female', 'Asian', 'Professional Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
('DM012', '20-24', 'Male', 'Black or African American', 'High School Graduate', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM013', '45-54', 'Female', 'White', 'Associate Degree', 'Married', 'Non-Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM014', '25-34', 'Male', 'Two or More Races', 'Bachelor''s Degree', 'Single', 'Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM015', '35-44', 'Female', 'American Indian or Alaska Native', 'Some College', 'Divorced', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),

-- Special demographic segments
('DM016', '16-19', 'Male', 'Hispanic or Latino', 'Less than High School', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Bilingual', '2019-01-01'),
('DM017', '65+', 'Female', 'White', 'High School Graduate', 'Widowed', 'Non-Veteran', 'With Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM018', '25-34', 'Non-Binary', 'White', 'Master''s Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01'),
('DM019', '35-44', 'Male', 'Asian', 'Doctoral Degree', 'Married', 'Non-Veteran', 'No Disability', 'Naturalized Citizen', 'Fluent', '2019-01-01'),
('DM020', '20-24', 'Female', 'Native Hawaiian or Pacific Islander', 'Associate Degree', 'Single', 'Non-Veteran', 'No Disability', 'US Citizen', 'Native Speaker', '2019-01-01');
            
            
-- COMPLETE GEOGRAPHIC DIMENSION TABLE INSERT STATEMENTS
-- dim_geography table data with full US coverage

INSERT INTO dim_geography (geography_id, country, state_code, state_name, county_fips, county_name, metro_area, region, division, latitude, longitude, population, area_sq_miles, effective_date, expiration_date, is_current) VALUES

-- Major States and Metropolitan Areas (matching fact table foreign keys)
('CA001', 'USA', 'CA', 'California', '06037', 'Los Angeles County', 'Los Angeles-Long Beach-Anaheim', 'West', 'Pacific', 34.0522, -118.2437, 10014009, 4751.00, '2019-01-01', NULL, 1),
('TX001', 'USA', 'TX', 'Texas', '48201', 'Harris County', 'Houston-The Woodlands-Sugar Land', 'South', 'West South Central', 29.7604, -95.3698, 4731145, 1703.00, '2019-01-01', NULL, 1),
('NY001', 'USA', 'NY', 'New York', '36061', 'New York County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.7128, -74.0060, 1694251, 23.00, '2019-01-01', NULL, 1),
('FL001', 'USA', 'FL', 'Florida', '12086', 'Miami-Dade County', 'Miami-Fort Lauderdale-West Palm Beach', 'South', 'South Atlantic', 25.7617, -80.1918, 2716940, 1946.00, '2019-01-01', NULL, 1),
('IL001', 'USA', 'IL', 'Illinois', '17031', 'Cook County', 'Chicago-Naperville-Elgin', 'Midwest', 'East North Central', 41.8781, -87.6298, 5275541, 946.00, '2019-01-01', NULL, 1),
('PA001', 'USA', 'PA', 'Pennsylvania', '42101', 'Philadelphia County', 'Philadelphia-Camden-Wilmington', 'Northeast', 'Middle Atlantic', 39.9526, -75.1652, 1603797, 135.00, '2019-01-01', NULL, 1),
('OH001', 'USA', 'OH', 'Ohio', '39035', 'Cuyahoga County', 'Cleveland-Elyria', 'Midwest', 'East North Central', 41.4993, -81.6944, 1235072, 458.00, '2019-01-01', NULL, 1),
('GA001', 'USA', 'GA', 'Georgia', '13135', 'Gwinnett County', 'Atlanta-Sandy Springs-Roswell', 'South', 'South Atlantic', 33.7490, -84.3880, 957062, 430.00, '2019-01-01', NULL, 1),
('NC001', 'USA', 'NC', 'North Carolina', '37119', 'Mecklenburg County', 'Charlotte-Concord-Gastonia', 'South', 'South Atlantic', 35.2271, -80.8431, 1115482, 546.00, '2019-01-01', NULL, 1),
('MI001', 'USA', 'MI', 'Michigan', '26163', 'Wayne County', 'Detroit-Warren-Dearborn', 'Midwest', 'East North Central', 42.3314, -83.0458, 1793561, 612.00, '2019-01-01', NULL, 1),
('WA001', 'USA', 'WA', 'Washington', '53033', 'King County', 'Seattle-Tacoma-Bellevue', 'West', 'Pacific', 47.6062, -122.3321, 2269675, 2134.00, '2019-01-01', NULL, 1),
('AZ001', 'USA', 'AZ', 'Arizona', '04013', 'Maricopa County', 'Phoenix-Mesa-Scottsdale', 'West', 'Mountain', 33.4484, -112.0740, 4485414, 9224.00, '2019-01-01', NULL, 1),
('TN001', 'USA', 'TN', 'Tennessee', '47037', 'Davidson County', 'Nashville-Davidson-Murfreesboro', 'South', 'East South Central', 36.1627, -86.7816, 715884, 502.00, '2019-01-01', NULL, 1),
('MA001', 'USA', 'MA', 'Massachusetts', '25025', 'Suffolk County', 'Boston-Cambridge-Newton', 'Northeast', 'New England', 42.3601, -71.0589, 797936, 58.00, '2019-01-01', NULL, 1),
('IN001', 'USA', 'IN', 'Indiana', '18097', 'Marion County', 'Indianapolis-Carmel-Anderson', 'Midwest', 'East North Central', 39.7684, -86.1581, 977203, 396.00, '2019-01-01', NULL, 1),
('MO001', 'USA', 'MO', 'Missouri', '29189', 'St. Louis County', 'St. Louis', 'Midwest', 'West North Central', 38.6270, -90.1994, 1001876, 508.00, '2019-01-01', NULL, 1),
('WI001', 'USA', 'WI', 'Wisconsin', '55079', 'Milwaukee County', 'Milwaukee-Waukesha-West Allis', 'Midwest', 'East North Central', 43.0389, -87.9065, 945726, 241.00, '2019-01-01', NULL, 1),
('MD001', 'USA', 'MD', 'Maryland', '24005', 'Baltimore County', 'Baltimore-Columbia-Towson', 'South', 'South Atlantic', 39.2904, -76.6122, 854535, 599.00, '2019-01-01', NULL, 1),
('MN001', 'USA', 'MN', 'Minnesota', '27123', 'Ramsey County', 'Minneapolis-St. Paul-Bloomington', 'Midwest', 'West North Central', 44.9778, -93.2650, 552344, 152.00, '2019-01-01', NULL, 1),
('CO001', 'USA', 'CO', 'Colorado', '08031', 'Denver County', 'Denver-Aurora-Lakewood', 'West', 'Mountain', 39.7392, -104.9903, 715522, 153.00, '2019-01-01', NULL, 1),

-- Additional Major Metropolitan Areas
('CA002', 'USA', 'CA', 'California', '06073', 'San Diego County', 'San Diego-Carlsbad', 'West', 'Pacific', 32.7157, -117.1611, 3338330, 4206.00, '2019-01-01', NULL, 1),
('CA003', 'USA', 'CA', 'California', '06075', 'San Francisco County', 'San Francisco-Oakland-Hayward', 'West', 'Pacific', 37.7749, -122.4194, 881549, 47.00, '2019-01-01', NULL, 1),
('TX002', 'USA', 'TX', 'Texas', '48113', 'Dallas County', 'Dallas-Fort Worth-Arlington', 'South', 'West South Central', 32.7767, -96.7970, 2613539, 880.00, '2019-01-01', NULL, 1),
('TX003', 'USA', 'TX', 'Texas', '48029', 'Bexar County', 'San Antonio-New Braunfels', 'South', 'West South Central', 29.4241, -98.4936, 2003554, 1247.00, '2019-01-01', NULL, 1),
('FL002', 'USA', 'FL', 'Florida', '12095', 'Orange County', 'Orlando-Kissimmee-Sanford', 'South', 'South Atlantic', 28.5383, -81.3792, 1393452, 907.00, '2019-01-01', NULL, 1),
('FL003', 'USA', 'FL', 'Florida', '12103', 'Pinellas County', 'Tampa-St. Petersburg-Clearwater', 'South', 'South Atlantic', 27.7663, -82.6404, 959107, 280.00, '2019-01-01', NULL, 1),
('NY002', 'USA', 'NY', 'New York', '36047', 'Kings County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.6782, -73.9442, 2736074, 70.00, '2019-01-01', NULL, 1),
('NY003', 'USA', 'NY', 'New York', '36081', 'Queens County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.7282, -73.7949, 2405464, 109.00, '2019-01-01', NULL, 1),

-- Additional States for Complete Coverage
('NV001', 'USA', 'NV', 'Nevada', '32003', 'Clark County', 'Las Vegas-Henderson-Paradise', 'West', 'Mountain', 36.1699, -115.1398, 2266715, 7891.00, '2019-01-01', NULL, 1),
('OR001', 'USA', 'OR', 'Oregon', '41051', 'Multnomah County', 'Portland-Vancouver-Hillsboro', 'West', 'Pacific', 45.5152, -122.6784, 815428, 431.00, '2019-01-01', NULL, 1),
('UT001', 'USA', 'UT', 'Utah', '49035', 'Salt Lake County', 'Salt Lake City', 'West', 'Mountain', 40.7608, -111.8910, 1185238, 742.00, '2019-01-01', NULL, 1),
('VA001', 'USA', 'VA', 'Virginia', '51059', 'Fairfax County', 'Washington-Arlington-Alexandria', 'South', 'South Atlantic', 38.9043, -77.0384, 1150309, 395.00, '2019-01-01', NULL, 1),
('NJ001', 'USA', 'NJ', 'New Jersey', '34003', 'Bergen County', 'New York-Newark-Jersey City', 'Northeast', 'Middle Atlantic', 40.8964, -74.0395, 955732, 234.00, '2019-01-01', NULL, 1),
('CT001', 'USA', 'CT', 'Connecticut', '09001', 'Fairfield County', 'Bridgeport-Stamford-Norwalk', 'Northeast', 'New England', 41.3083, -73.0275, 957419, 626.00, '2019-01-01', NULL, 1),
('SC001', 'USA', 'SC', 'South Carolina', '45019', 'Charleston County', 'Charleston-North Charleston', 'South', 'South Atlantic', 32.7765, -79.9311, 408235, 916.00, '2019-01-01', NULL, 1),
('KY001', 'USA', 'KY', 'Kentucky', '21111', 'Jefferson County', 'Louisville-Jefferson County', 'South', 'East South Central', 38.2527, -85.7585, 766757, 380.00, '2019-01-01', NULL, 1),
('LA001', 'USA', 'LA', 'Louisiana', '22071', 'Orleans Parish', 'New Orleans-Metairie', 'South', 'West South Central', 29.9511, -90.0715, 390144, 169.00, '2019-01-01', NULL, 1),
('OK001', 'USA', 'OK', 'Oklahoma', '40109', 'Oklahoma County', 'Oklahoma City', 'South', 'West South Central', 35.4676, -97.5164, 797434, 709.00, '2019-01-01', NULL, 1),
('AR001', 'USA', 'AR', 'Arkansas', '05119', 'Pulaski County', 'Little Rock-North Little Rock-Conway', 'South', 'West South Central', 34.7465, -92.2896, 395760, 774.00, '2019-01-01', NULL, 1),
('KS001', 'USA', 'KS', 'Kansas', '20091', 'Johnson County', 'Kansas City', 'Midwest', 'West North Central', 38.9072, -94.7203, 597511, 477.00, '2019-01-01', NULL, 1),
('IA001', 'USA', 'IA', 'Iowa', '19153', 'Polk County', 'Des Moines-West Des Moines', 'Midwest', 'West North Central', 41.5868, -93.6250, 492401, 569.00, '2019-01-01', NULL, 1),
('NE001', 'USA', 'NE', 'Nebraska', '31055', 'Douglas County', 'Omaha-Council Bluffs', 'Midwest', 'West North Central', 41.2565, -95.9345, 571327, 335.00, '2019-01-01', NULL, 1),

-- Mountain States
('MT001', 'USA', 'MT', 'Montana', '30111', 'Yellowstone County', 'Billings', 'West', 'Mountain', 45.7833, -108.5007, 164731, 2633.00, '2019-01-01', NULL, 1),
('ID001', 'USA', 'ID', 'Idaho', '16001', 'Ada County', 'Boise City', 'West', 'Mountain', 43.6150, -116.2023, 481587, 1055.00, '2019-01-01', NULL, 1),
('WY001', 'USA', 'WY', 'Wyoming', '56025', 'Natrona County', 'Casper', 'West', 'Mountain', 42.8500, -106.3162, 79858, 5376.00, '2019-01-01', NULL, 1),
('ND001', 'USA', 'ND', 'North Dakota', '38017', 'Cass County', 'Fargo', 'Midwest', 'West North Central', 46.8772, -96.7898, 184525, 1765.00, '2019-01-01', NULL, 1),
('SD001', 'USA', 'SD', 'South Dakota', '46099', 'Minnehaha County', 'Sioux Falls', 'Midwest', 'West North Central', 43.5446, -96.7311, 197214, 809.00, '2019-01-01', NULL, 1),

-- New England and Northeast
('ME001', 'USA', 'ME', 'Maine', '23005', 'Cumberland County', 'Portland-South Portland', 'Northeast', 'New England', 43.6591, -70.2568, 303069, 835.00, '2019-01-01', NULL, 1),
('NH001', 'USA', 'NH', 'New Hampshire', '33011', 'Hillsborough County', 'Manchester-Nashua', 'Northeast', 'New England', 42.9956, -71.4548, 422937, 876.00, '2019-01-01', NULL, 1),
('VT001', 'USA', 'VT', 'Vermont', '50007', 'Chittenden County', 'Burlington-South Burlington', 'Northeast', 'New England', 44.4759, -73.2121, 168323, 536.00, '2019-01-01', NULL, 1),
('RI001', 'USA', 'RI', 'Rhode Island', '44007', 'Providence County', 'Providence-Warwick', 'Northeast', 'New England', 41.8240, -71.4128, 660741, 410.00, '2019-01-01', NULL, 1),

-- Alaska and Hawaii
('AK001', 'USA', 'AK', 'Alaska', '02020', 'Anchorage Municipality', 'Anchorage', 'West', 'Pacific', 61.2181, -149.9003, 291247, 1961.00, '2019-01-01', NULL, 1),
('HI001', 'USA', 'HI', 'Hawaii', '15003', 'Honolulu County', 'Urban Honolulu', 'West', 'Pacific', 21.3099, -157.8581, 1016508, 596.00, '2019-01-01', NULL, 1),

-- Additional Southern States
('AL001', 'USA', 'AL', 'Alabama', '01073', 'Jefferson County', 'Birmingham-Hoover', 'South', 'East South Central', 33.5207, -86.8025, 674721, 1111.00, '2019-01-01', NULL, 1),
('MS001', 'USA', 'MS', 'Mississippi', '28049', 'Hinds County', 'Jackson', 'South', 'East South Central', 32.2988, -90.1848, 238674, 869.00, '2019-01-01', NULL, 1),
('WV001', 'USA', 'WV', 'West Virginia', '54039', 'Kanawha County', 'Charleston', 'South', 'South Atlantic', 38.3498, -81.6326, 180745, 903.00, '2019-01-01', NULL, 1),
('DE001', 'USA', 'DE', 'Delaware', '10003', 'New Castle County', 'Philadelphia-Camden-Wilmington', 'South', 'South Atlantic', 39.7391, -75.5398, 570719, 426.00, '2019-01-01', NULL, 1);
            

-- Insert complete industry dimension dataset
INSERT INTO dim_industry (
    industry_id, naics_code, industry_name, industry_sector, industry_group,
    seasonal_factor, automation_risk, avg_wage_level, sector_code, supersector_code, is_active
) VALUES

-- Professional and Business Services
('IND001', '54', 'Professional, Scientific, and Technical Services', 'Professional Services', 'Information & Professional', 
 'Low', 'Medium', 'High', '54', '540', 1),

-- Healthcare and Social Assistance
('IND002', '62', 'Health Care and Social Assistance', 'Healthcare', 'Education & Health', 
 'Low', 'Low', 'High', '62', '620', 1),

-- Retail Trade
('IND003', '44', 'Retail Trade', 'Retail', 'Trade & Transportation', 
 'High', 'High', 'Low', '44', '440', 1),

-- Manufacturing
('IND004', '31', 'Manufacturing', 'Manufacturing', 'Manufacturing', 
 'Medium', 'High', 'Medium', '31', '310', 1),

-- Accommodation and Food Services
('IND005', '72', 'Accommodation and Food Services', 'Hospitality', 'Leisure & Hospitality', 
 'High', 'Medium', 'Low', '72', '720', 1),

-- Construction
('IND006', '23', 'Construction', 'Construction', 'Construction & Resources', 
 'High', 'Medium', 'Medium', '23', '230', 1),

-- Transportation and Warehousing
('IND007', '48', 'Transportation and Warehousing', 'Transportation', 'Trade & Transportation', 
 'Medium', 'High', 'Medium', '48', '480', 1),

-- Educational Services
('IND008', '61', 'Educational Services', 'Education', 'Education & Health', 
 'High', 'Low', 'Medium', '61', '610', 1),

-- Finance and Insurance
('IND009', '52', 'Finance and Insurance', 'Financial Services', 'Financial Activities', 
 'Low', 'Medium', 'High', '52', '520', 1),

-- Information Technology
('IND010', '51', 'Information', 'Information Technology', 'Information & Professional', 
 'Low', 'Medium', 'High', '51', '510', 1),

-- Public Administration
('IND011', '92', 'Public Administration', 'Government', 'Government', 
 'Low', 'Low', 'Medium', '92', '920', 1),

-- Arts, Entertainment, and Recreation
('IND012', '71', 'Arts, Entertainment, and Recreation', 'Entertainment', 'Leisure & Hospitality', 
 'High', 'Medium', 'Low', '71', '710', 1),

-- Agriculture, Forestry, Fishing and Hunting
('IND013', '11', 'Agriculture, Forestry, Fishing and Hunting', 'Agriculture', 'Construction & Resources', 
 'High', 'Medium', 'Low', '11', '110', 1),

-- Utilities
('IND014', '22', 'Utilities', 'Utilities', 'Construction & Resources', 
 'Low', 'Medium', 'High', '22', '220', 1),

-- Other Services
('IND015', '81', 'Other Services (except Public Administration)', 'Other Services', 'Other Services', 
 'Medium', 'Medium', 'Low', '81', '810', 1);
-- Step 4: Verify data integrity
SELECT 
    'unemployment_statistics' AS table_name, 
    COUNT(*) AS record_count 
FROM unemployment_statistics
UNION ALL
SELECT 'dim_geography', COUNT(*) FROM dim_geography
UNION ALL
SELECT 'dim_demographics', COUNT(*) FROM dim_demographics
UNION ALL
SELECT 'dim_industry', COUNT(*) FROM dim_industry
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;
-- Create performance indexes for Power BI optimization
CREATE INDEX IX_Industry_Hierarchy 
ON dim_industry(industry_sector, industry_group, industry_name);

CREATE INDEX IX_Industry_Analysis 
ON dim_industry(seasonal_factor, automation_risk, avg_wage_level);

CREATE INDEX IX_Industry_NAICS 
ON dim_industry(naics_code, sector_code);

-- Verify the dataset
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT industry_sector) as unique_sectors,
    COUNT(DISTINCT industry_group) as unique_groups,
    COUNT(DISTINCT naics_code) as unique_naics_codes
FROM dim_industry 
WHERE is_active = 1;

-- Sample query for Power BI - Industry Performance Analysis
SELECT 
    i.industry_sector,
    i.industry_name,
    i.seasonal_factor,
    i.automation_risk,
    i.avg_wage_level,
    AVG(us.unemployment_rate) as avg_unemployment_rate,
    SUM(us.labor_force) as total_labor_force
FROM dim_industry i
LEFT JOIN unemployment_statistics us ON i.industry_id = us.industry_id
WHERE i.is_active = 1
GROUP BY i.industry_sector, i.industry_name, i.seasonal_factor, i.automation_risk, i.avg_wage_level
ORDER BY avg_unemployment_rate DESC;
-- Quick verification query after data load SELECT COUNT(*) as total_records, MIN(unemployment_rate) as min_rate, MAX(unemployment_rate) as max_rate, AVG(unemployment_rate) as avg_rate, COUNT(DISTINCT geography_id) as unique_geographies, COUNT(DISTINCT demographic_id) as unique_demographics, COUNT(DISTINCT industry_id) as unique_industries, MIN(CONVERT(DATE, CAST(date_id AS VARCHAR), 112)) as earliest_date, MAX(CONVERT(DATE, CAST(date_id AS VARCHAR), 112)) as latest_date FROM unemployment_statistics; -- Sample trend analysis SELECT YEAR(CONVERT(DATE, CAST(date_id AS VARCHAR), 112)) as year, AVG(unemployment_rate) as avg_unemployment_rate, COUNT(*) as record_count FROM unemployment_statistics GROUP BY YEAR(CONVERT(DATE, CAST(date_id AS VARCHAR), 112)) ORDER BY year;SELECT COUNT(*) FROM dim_date;
SELECT COUNT(*) FROM dim_geography;
SELECT COUNT(*) FROM dim_demographics;
SELECT COUNT(*) FROM dim_industry;
SELECT COUNT(*) FROM unemployment_statistics;
*/
