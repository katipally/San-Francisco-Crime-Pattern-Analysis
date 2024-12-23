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
    "incident_category" as incident_category,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY "incident_category", covid_period
ORDER BY "incident_category", covid_period