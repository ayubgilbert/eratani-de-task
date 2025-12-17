SELECT
    "Farm_ID",
    "Crop_Type",
    CAST("Farm_Area(acres)" AS FLOAT) as farm_area_acres,
    "Irrigation_Type",
    CAST("Fertilizer_Used(tons)" AS FLOAT) as fertilizer_used_tons,
    CAST("Pesticide_Used(kg)" AS FLOAT) as pesticide_used_kg,
    CAST("Yield(tons)" AS FLOAT) as yield_tons,
    "Soil_Type",
    "Season",
    CAST("Water_Usage(cubic meters)" AS FLOAT) as water_usage_cubic_meters
FROM {{ source('eratani_db', 'staging_agriculture') }}
WHERE "Yield(tons)" IS NOT NULL