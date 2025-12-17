SELECT
    "Crop_Type",
    "Season",
    "Irrigation_Type",
    SUM(yield_tons) as total_yield_tons,
    SUM(yield_tons) / NULLIF(SUM(farm_area_acres), 0) as yield_per_acre,
    SUM(yield_tons) / NULLIF(SUM(fertilizer_used_tons), 0) as fertilizer_efficiency,
    SUM(yield_tons) / NULLIF(SUM(water_usage_cubic_meters), 0) as water_productivity
FROM {{ ref('fact_farm_production') }} 
GROUP BY 1, 2, 3