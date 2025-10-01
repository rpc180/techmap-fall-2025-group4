-- Step 1: Create the unemployment analytics database
CREATE DATABASE UnemploymentAnalyticsDB;
GO

USE UnemploymentAnalyticsDB;
GO

-- Step 2: Execute table creation scripts from HTML files
-- Run CREATE TABLE statements for:
-- 1. dim_date (from unemployment-date-dimension-part1.html)
-- 2. dim_geography (from unemployment-geography-dimension.html)
-- 3. dim_demographics (from unemployment-demographics-dimension.html)
-- 4. dim_industry (from unemployment-industry-dimension.html)
-- 5. unemployment_statistics (from unemployment-dataset-samples.html)
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BIT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter INT NOT NULL,
    created_date DATETIME2 DEFAULT GETDATE()
);
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
    age_group VARCHAR(20) NOT NULL 
    gender VARCHAR(10) NOT NULL
    race_ethnicity VARCHAR(50) NOT NULL
    education_level VARCHAR(50) NOT NULL
    marital_status VARCHAR(20) NOT NULL              
    veteran_status VARCHAR(20) NOT NULL
    disability_status VARCHAR(20) NOT NULL
    citizenship_status VARCHAR(20) NOT NULL
    english_proficiency VARCHAR(20) NOT NULL
    created_date DATETIME2 DEFAULT GETDATE()
);
    
CREATE TABLE unemployment_statistics (
    record_id INT PRIMARY KEY IDENTITY(1,1),
    date_id INTNOT NULL,
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
CREATE TABLE dim_industry (
    industry_id VARCHAR(10) PRIMARY KEY,
    naics_code VARCHAR(6) NOT NULL,
    industry_name VARCHAR(200) NOT NULL,
    industry_sector VARCHAR(100) NOT NULL,
    industry_group VARCHAR(100) NOT NULL,
    seasonal_factor VARCHAR(20),
    automation_risk VARCHAR(20),
    avg_wage_level VARCHAR(20),
    sector_code VARCHAR(2),
    supersector_code VARCHAR(3),
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE()
);
-- Step 3: Insert all data using HTML dataset files


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
