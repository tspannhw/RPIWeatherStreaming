create or replace TABLE DEMO.DEMO.WEATHER_DATA (
	UUID VARCHAR(16777216),
	ROWID VARCHAR(16777216),
	HOSTNAME VARCHAR(16777216),
	HOST VARCHAR(16777216),
	IPADDRESS VARCHAR(16777216),
	MACADDRESS VARCHAR(16777216),
	SYSTEMTIME VARCHAR(16777216),
	STARTTIME VARCHAR(16777216),
	ENDTIME VARCHAR(16777216),
	TS NUMBER(38,0),
	TE VARCHAR(16777216),
	RUNTIME NUMBER(38,0),
	CPUTEMPF NUMBER(38,0),
	CPU FLOAT,
	MEMORY FLOAT,
	DISKUSAGE VARCHAR(16777216),
	TEMPERATURE FLOAT,
	HUMIDITY FLOAT,
	PRESSURE FLOAT,
	DEVICETEMPERATURE FLOAT,
	DEWPOINT FLOAT,
	LUX FLOAT,
	DATETIMESTAMP TIMESTAMP_NTZ(9)
);


CREATE OR REPLACE VIEW DEMO.DEMO.WEATHER_LATEST AS
SELECT 
    hostname,
    temperature,
    humidity,
    pressure,
    lux,
    dewpoint,
    devicetemperature,
    systemtime,
    ts
FROM DEMO.DEMO.WEATHER_DATA
QUALIFY ROW_NUMBER() OVER (PARTITION BY hostname ORDER BY ts DESC) = 1;


CREATE OR REPLACE VIEW DEMO.DEMO.WEATHER_RECENT AS
SELECT *
FROM DEMO.DEMO.WEATHER_DATA
WHERE ts >= DATE_PART(epoch_second, DATEADD(hour, -1, CURRENT_TIMESTAMP()));

GRANT USAGE ON DATABASE DEMO TO ROLE THERMAL_STREAMING_ROLE;
GRANT USAGE ON SCHEMA DEMO.DEMO TO ROLE THERMAL_STREAMING_ROLE;
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE DEMO.DEMO.WEATHER_DATA TO ROLE THERMAL_STREAMING_ROLE;
GRANT ALL PRIVILEGES ON TABLE DEMO.DEMO.WEATHER_DATA TO ROLE ACCOUNTADMIN;
GRANT ROLE SYSADMIN TO USER THERMAL_STREAMING_USER;

GRANT ALL PRIVILEGES ON TABLE DEMO.DEMO.WEATHER_DATA TO ROLE ACCOUNTADMIN;

SHOW TABLES LIKE 'WEATHER_DATA' IN SCHEMA DEMO.DEMO;

DESC TABLE DEMO.DEMO.WEATHER_DATA;

-- Check grants
SHOW GRANTS ON TABLE DEMO.DEMO.WEATHER_DATA;

CREATE OR REPLACE PIPE WEATHER_SENSOR_PIPE
COMMENT = 'Snowpipe Streaming v2 pipe for Raspberry Pi Weather HAT sensor data'
AS 
COPY INTO WEATHER_DATA (
    uuid,
    rowid,
    hostname,
    host,
    ipaddress,
    macaddress,
    systemtime,
    starttime,
    endtime,
    ts,
    te,
    runtime,
    cputempf,
    cpu,
    memory,
    diskusage,
    temperature,
    humidity,
    pressure,
    lux,
    devicetemperature,
    dewpoint
)
FROM (
    SELECT 
        $1:uuid::VARCHAR as uuid,
        $1:rowid::VARCHAR as rowid,
        $1:hostname::VARCHAR as hostname,
        $1:host::VARCHAR as host,
        $1:ipaddress::VARCHAR as ipaddress,
        $1:macaddress::VARCHAR as macaddress,
        $1:systemtime::VARCHAR as systemtime,
        $1:starttime::VARCHAR as starttime,
        $1:endtime::VARCHAR as endtime,
        $1:ts::NUMBER as ts,
        $1:te::VARCHAR as te,
        $1:runtime::NUMBER as runtime,
        $1:cputempf::NUMBER as cputempf,
        $1:cpu::FLOAT as cpu,
        $1:memory::FLOAT as memory,
        $1:diskusage::VARCHAR as diskusage,
        $1:temperature::FLOAT as temperature,
        $1:humidity::FLOAT as humidity,
        $1:pressure::FLOAT as pressure,
        $1:lux::FLOAT as lux,
        $1:devicetemperature::FLOAT as devicetemperature,
        $1:dewpoint::FLOAT as dewpoint
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

describe pipe WEATHER_SENSOR_PIPE;
describe pipe THERMAL_SENSOR_PIPE;


GRANT OPERATE ON PIPE WEATHER_SENSOR_PIPE TO ROLE ACCOUNTADMIN;
GRANT MONITOR ON PIPE WEATHER_SENSOR_PIPE TO ROLE ACCOUNTADMIN;

-- Also grant to THERMAL_STREAMING_ROLE (for consistency with thermal setup)
GRANT OPERATE ON PIPE WEATHER_SENSOR_PIPE TO ROLE THERMAL_STREAMING_ROLE;
GRANT MONITOR ON PIPE WEATHER_SENSOR_PIPE TO ROLE THERMAL_STREAMING_ROLE;

GRANT ALL ON PIPE DEMO.DEMO.WEATHER_SENSOR_PIPE TO USER THERMAL_STREAMING_USER;
GRANT ALL ON PIPE DEMO.DEMO.WEATHER_SENSOR_PIPE TO ROLE ACCOUNTADMIN;
-- ============================================================================
-- Step 4: Grant table permissions
-- ============================================================================

GRANT ALL ON TABLE WEATHER_DATA TO ROLE ACCOUNTADMIN;
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE WEATHER_DATA TO USER THERMAL_STREAMING_USER;

SHOW PIPES LIKE 'WEATHER_SENSOR_PIPE';

-- Check table exists
SHOW TABLES LIKE 'WEATHER_DATA';

-- Check pipe details
DESC PIPE WEATHER_SENSOR_PIPE;

-- Check grants on pipe
SHOW GRANTS ON PIPE WEATHER_SENSOR_PIPE;

-- Check grants on table
SHOW GRANTS ON TABLE WEATHER_DATA;

select * from weather_data order by systemtime desc;


select count(*) from weather_data;

-- ============================================================================
-- 6. Sample Queries
-- ============================================================================

-- View latest reading from each device
SELECT * FROM DEMO.DEMO.WEATHER_LATEST;

-- View recent weather data
SELECT 
    hostname,
    temperature,
    humidity,
    pressure,
    lux,
    systemtime
FROM DEMO.DEMO.WEATHER_RECENT
ORDER BY ts DESC
LIMIT 100;

-- Average readings per minute
SELECT 
    DATE_TRUNC('minute', TO_TIMESTAMP(ts)) as minute,
    hostname,
    AVG(temperature) as avg_temp_f,
    AVG(humidity) as avg_humidity_pct,
    AVG(pressure) as avg_pressure_hpa,
    AVG(lux) as avg_lux,
    COUNT(*) as reading_count
FROM DEMO.DEMO.WEATHER_DATA
WHERE ts >= UNIX_TIMESTAMP(DATEADD(hour, -24, CURRENT_TIMESTAMP()))
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Temperature trend (hourly)
SELECT 
    DATE_TRUNC('hour', TO_TIMESTAMP(ts)) as hour,
    hostname,
    MIN(temperature) as min_temp,
    AVG(temperature) as avg_temp,
    MAX(temperature) as max_temp,
    AVG(humidity) as avg_humidity,
    AVG(pressure) as avg_pressure
FROM DEMO.DEMO.WEATHER_DATA
WHERE ts >= UNIX_TIMESTAMP(DATEADD(day, -7, CURRENT_TIMESTAMP()))
GROUP BY 1, 2
ORDER BY 1 DESC, 2;




