WITH
    --- 妥投原始数据
    signed_data_orig AS (
        SELECT
            brand_class_name,
            CASE
                WHEN brand_class_name='空调' THEN second_class_name
                ELSE model_name
            END AS model,
            model_name,
            project_code,
            CAST(signed_tm/10000 AS DECIMAL(8, 0)) AS natural_year,
            CAST(signed_tm/100 AS DECIMAL(8, 0)) AS natural_month,
            SUM(signed_cnt) AS signed
        FROM
            hive_zjyprc_hadoop.dw_business.view_dm_asc_qms_srd_sign_d
        WHERE
            1=1
            AND brand_class_name IN ('空调', '中央空调')
            AND model_name not LIKE '%内机%'
        GROUP BY
            brand_class_name,
            model,
            model_name,
            project_code,
            CAST(signed_tm/10000 AS DECIMAL(8, 0)),
            CAST(signed_tm/100 AS DECIMAL(8, 0))
    ),
    -- 妥投数据：按自然月求和
    signed_data AS (
        SELECT
            CONCAT(
                SUBSTRING(natural_month, 1, 4),
                '-',
                SUBSTRING(natural_month, 5, 2)
            ) AS natural_month,
            sum(signed) AS signed_ct
        FROM
            signed_data_orig
        WHERE
            brand_class_name='空调'
        GROUP BY
            CONCAT(
                SUBSTRING(natural_month, 1, 4),
                '-',
                SUBSTRING(natural_month, 5, 2)
            )
    ),

    --- 工单数据：年、关单时间、数量
    subquery AS (
        SELECT
            SUBSTR(sign_time, 1, 4) AS year_part,
            to_date(close_time) AS close_time,
            count(sn) AS sn_ct
        FROM
            hive_zjyprc_hadoop.ods.quality_ea_thx_detail
        WHERE
            1=1
            AND brand_class_name='空调'
            AND is_ffr=1
            AND (
                fault_level_name_2 LIKE '%主板%'
                OR fault_level_name_2 LIKE '%内外机通讯%'
                OR sub_cls_name LIKE '%主板%'
            )
        GROUP BY
            SUBSTR(sign_time, 1, 4),
            to_date(close_time)
    ),
    
    --- 工单数据：年、年月、数量
    main_query AS (
        SELECT
            year_part,
            concat(YEAR(close_time), '-', MONTH(close_time)) AS year_month,
            sum(sn_ct) AS total_sn_ct
        FROM
            subquery
        GROUP BY
            year_part,
            concat(YEAR(close_time), '-', MONTH(close_time))
    ),
    -- 工单数据：年、年月、总数
    ord_data AS (
        SELECT
            year_part,
            CONCAT(
                SUBSTRING(year_month, 1, 5),
                LPAD(SUBSTRING(year_month, 6), 2, '0')
            ) AS year_month,
            total_sn_ct
        FROM
            main_query
    ),
    -- 妥投总数、工单总数
    signed_ord_by_m AS (
        SELECT
            SUBSTR(sd.natural_month, 1, 4) AS year_part,
            SUBSTR(sd.natural_month, 6) AS month_part,
            sd.signed_ct,
            od.total_sn_ct
        FROM
            signed_data sd
            LEFT JOIN ord_data od ON sd.natural_month=od.year_month
            AND SUBSTR(sd.natural_month, 1, 4)=od.year_part
        ORDER BY
            natural_month
    )
SELECT
    *,
    SUM(signed_ct) OVER (
        PARTITION BY
            year_part
        ORDER BY
            month_part
    ) AS signed,
    SUM(COALESCE(total_sn_ct, 0)) OVER (
        PARTITION BY
            year_part
        ORDER BY
            month_part
    ) AS ord
FROM
    signed_ord_by_m