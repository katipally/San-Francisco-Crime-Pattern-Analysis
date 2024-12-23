WITH incidents_with_period AS (
    SELECT
        *,
        CASE
            WHEN "incident_datetime" < '2020-03-01' THEN 'Pre-COVID'
            ELSE 'Post-COVID'
        END AS covid_period
    FROM {{ source('raw_data', 'incidents') }}
)
,
incident_totals AS (
    SELECT
        covid_period,
        COUNT("incident_id") AS total_incidents
    FROM incidents_with_period
    GROUP BY covid_period
)
SELECT * 
FROM incident_totals