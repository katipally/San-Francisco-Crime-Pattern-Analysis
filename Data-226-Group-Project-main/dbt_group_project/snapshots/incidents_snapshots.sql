{% snapshot police_district_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='police_district',
    strategy='check',
    check_cols=['covid_period', 'incident_count']
) }}

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
    "police_district" AS police_district,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY "police_district", covid_period

{% endsnapshot %}


{% snapshot hour_of_day_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='hour_of_day',
    strategy='check',
    check_cols=['covid_period', 'incident_count']
) }}

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
    TO_CHAR("incident_datetime", 'HH24') AS hour_of_day,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY hour_of_day, covid_period

{% endsnapshot %}


{% snapshot incident_category_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='incident_category',
    strategy='check',
    check_cols=['covid_period', 'incident_count']
) }}

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
    "incident_category" AS incident_category,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY "incident_category", covid_period

{% endsnapshot %}


{% snapshot location_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='location_key',
    strategy='check',
    check_cols=['incident_count']
) }}

WITH incidents_with_period AS (
    SELECT
        *,
        CASE
            WHEN "incident_datetime" < '2020-03-01' THEN 'Pre-COVID'
            ELSE 'Post-COVID'
        END AS covid_period
    FROM {{ source('raw_data', 'incidents') }}
),
location_with_key AS (
    SELECT
        "latitude" AS latitude,
        "longitude" AS longitude,
        CONCAT("latitude", ',', "longitude") AS location_key,
        COUNT(*) AS incident_count
    FROM incidents_with_period
    GROUP BY "latitude", "longitude"
)

SELECT 
    latitude,
    longitude,
    location_key,
    incident_count
FROM location_with_key

{% endsnapshot %}



{% snapshot incident_subcategory_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='incident_subcategory',
    strategy='check',
    check_cols=['covid_period', 'incident_count']
) }}

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
    "incident_subcategory" AS incident_subcategory,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY "incident_subcategory", covid_period

{% endsnapshot %}


{% snapshot intersection_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='intersection',
    strategy='check',
    check_cols=['covid_period', 'incident_count']
) }}

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
    "intersection" AS intersection,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY intersection, covid_period

{% endsnapshot %}


{% snapshot month_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='month',
    strategy='check',
    check_cols=['covid_period', 'incident_count']
) }}

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
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY month, covid_period

{% endsnapshot %}


{% snapshot day_of_week_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='incident_day_of_week',
    strategy='check',
    check_cols=['covid_period', 'incident_count']
) }}

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
    "incident_day_of_week" AS incident_day_of_week,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY "incident_day_of_week", covid_period

{% endsnapshot %}


{% snapshot neighborhood_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='analysis_neighborhood',
    strategy='check',
    check_cols=['covid_period', 'incident_count']
) }}

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
    "analysis_neighborhood" AS analysis_neighborhood,
    covid_period,
    COUNT(*) AS incident_count
FROM incidents_with_period
GROUP BY "analysis_neighborhood", covid_period

{% endsnapshot %}

{% snapshot incident_totals_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='covid_period',
    strategy='check',
    check_cols=['total_incidents']
) }}

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
    covid_period,
    COUNT("incident_id") AS total_incidents
FROM incidents_with_period
GROUP BY covid_period

{% endsnapshot %}


