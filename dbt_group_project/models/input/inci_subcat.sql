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
    "incident_subcategory" as incident_subcategory,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY incident_subcategory, covid_period
ORDER BY incident_count desc, incident_subcategory, covid_period