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
    TO_CHAR("incident_datetime", 'YYYY-MM') AS month,
    COUNT(*) AS incident_count,
    covid_period
FROM incidents_with_period
GROUP BY month, covid_period
ORDER BY incident_count desc
