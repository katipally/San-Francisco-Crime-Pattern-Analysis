WITH incidents_with_period AS (
    SELECT
        *,
        CASE
            WHEN "incident_datetime" < '2020-03-01' THEN 'Pre-COVID'
            ELSE 'Post-COVID'
        END AS covid_period
    FROM {{ source('raw_data', 'incidents') }}
)


SELECT 
    "latitude" as latitude,
    "longitude" as longitude,
    COUNT(*) AS incident_count
FROM 
    incidents_with_period
group by latitude,longitude