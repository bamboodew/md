WITH base_data AS (
    -- 提前计算出时间字段
    SELECT brand_class_name,
        CASE
            WHEN brand_class_name = '空调' THEN second_class_name
            ELSE model_name
        END AS model,
        project_code,
        CAST(signed_tm / 10000 AS DECIMAL(8, 0)) AS natural_year,
        -- 妥投-年
        CAST(signed_tm / 100 AS DECIMAL(8, 0)) AS natural_month,
        --妥投-月
        SUM(signed_cnt) AS signed
    FROM hive_zjyprc_hadoop.dw_business.view_dm_asc_qms_srd_sign_d
    WHERE brand_class_name = '空调'
    GROUP BY brand_class_name,
        CASE
            WHEN brand_class_name = '空调' THEN second_class_name
            ELSE model_name
        END,
        project_code,
        CAST(signed_tm / 10000 AS DECIMAL(8, 0)),
        CAST(signed_tm / 100 AS DECIMAL(8, 0))
),
ord_data AS (
    -- 简化后仅处理必要的数据
    SELECT model,
        CAST(natural_day / 10000 AS DECIMAL(8, 0)) AS natural_year,
        -- 工单-年
        CAST(natural_day / 100 AS DECIMAL(8, 0)) AS natural_month,
        -- 工单-月
        SUM(ord_cnt) AS ord
    FROM hive_zjyprc_hadoop.dw_business.view_dm_asc_qms_srd_ord_d
    WHERE formula_name = '售后维修'
    GROUP BY model,
        CAST(natural_day / 10000 AS DECIMAL(8, 0)),
        CAST(natural_day / 100 AS DECIMAL(8, 0))
),
afr_data AS (
    -- 合并妥投和维修单数据，尽量减少窗口函数的计算
    SELECT sd.brand_class_name,
        sd.model,
        sd.project_code,
        sd.natural_year,
        sd.natural_month,
        SUM(sd.signed) OVER (
            PARTITION BY sd.model,
            sd.natural_year
            ORDER BY sd.natural_month
        ) AS signed,
        SUM(COALESCE(od.ord, 0)) OVER (
            PARTITION BY sd.model,
            sd.natural_year
            ORDER BY sd.natural_month
        ) AS ord
    FROM base_data sd
        LEFT JOIN ord_data od ON sd.model = od.model
        AND sd.natural_month = od.natural_month -- 
),
final_data AS (
    -- 整合目标数据
    SELECT DISTINCT ad.*,
        md.part_one,
        md.market_date,
        md.brief_name,
        md.ziyan,
        md.factory,
        CAST(gb.challenge_goal AS DECIMAL(8, 6)) AS goal_one,
        CAST(gp.challenge_goal AS DECIMAL(8, 6)) AS goal_two,
        CAST(gb.last_year_ffr AS DECIMAL(8, 6)) AS last_year_one,
        CAST(gp.last_year_ffr AS DECIMAL(8, 6)) AS last_year_two
    FROM afr_data ad
        LEFT JOIN iceberg_zjyprc_hadoop.tmp.model_detail md ON ad.model = md.model
        JOIN iceberg_zjyprc_hadoop.tmp.goal_afr_ffr gb ON md.brand_class_name = gb.brand_class_name
        AND ad.natural_month = gb.date
        JOIN iceberg_zjyprc_hadoop.tmp.goal_afr_ffr gp ON md.part_one = gp.goal_type
        AND ad.natural_month = gp.date
    WHERE gb.indicator = '年度滚动AFR'
        AND gb.goal_type = 'overall'
        AND gp.indicator = '年度滚动AFR'
        AND gp.goal_type != 'overall'
) -- 最终汇总和计算
SELECT natural_month AS `natural_month_月`,
    (SUM(ord)) /(SUM(signed)) AS afr,
    goal_one,
    last_year_one
FROM final_data
GROUP BY natural_month,
    goal_one,
    last_year_one
ORDER BY `natural_month_月`
LIMIT 5000