
-- ============================================
-- Which states have highest total drug spend
-- and highest cost per beneficiary?
-- ============================================

SELECT
    PRSCRBR_STATE_ABRVTN                                        AS STATE,
    COUNT(DISTINCT PRSCRBR_NPI)                                 AS TOTAL_PRESCRIBERS,
    SUM(TOT_DRUG_CST::FLOAT)                                    AS TOTAL_DRUG_SPEND,
    SUM(TOT_BENES::FLOAT)                                       AS TOTAL_BENEFICIARIES,
    ROUND(SUM(TOT_DRUG_CST::FLOAT) /
          NULLIF(SUM(TOT_BENES::FLOAT),0), 2)                   AS COST_PER_BENEFICIARY
FROM CMS_MEDICARE.RAW.PART_D_PRESCRIBERS
WHERE PRSCRBR_CNTRY = 'US'
AND PRSCRBR_STATE_ABRVTN IS NOT NULL
GROUP BY 1
ORDER BY TOTAL_DRUG_SPEND DESC;


-- ============================================
-- Chapter 2: Who is prescribing?
-- Which specialties drive 80% of drug spend?
-- ============================================

WITH SPECIALTY_SPEND AS (
    SELECT
        PRSCRBR_TYPE                                            AS SPECIALTY,
        COUNT(DISTINCT PRSCRBR_NPI)                             AS TOTAL_PRESCRIBERS,
        SUM(TOT_CLMS::FLOAT)                                    AS TOTAL_CLAIMS,
        SUM(TOT_DRUG_CST::FLOAT)                                AS TOTAL_SPEND,
        ROUND(SUM(TOT_DRUG_CST::FLOAT) /
              NULLIF(COUNT(DISTINCT PRSCRBR_NPI),0), 2)         AS AVG_SPEND_PER_PRESCRIBER
    FROM CMS_MEDICARE.RAW.PART_D_PRESCRIBERS
    WHERE PRSCRBR_TYPE IS NOT NULL
    GROUP BY 1
),
CUMULATIVE AS (
    SELECT *,
        ROUND(SUM(TOTAL_SPEND) OVER (ORDER BY TOTAL_SPEND DESC) /
              SUM(TOTAL_SPEND) OVER () * 100, 2)                AS CUMULATIVE_PCT
    FROM SPECIALTY_SPEND
)
SELECT * FROM CUMULATIVE
ORDER BY TOTAL_SPEND DESC;


-- ============================================
-- Chapter 3: What are they prescribing?
-- Where is branded drug spend concentrated
-- by specialty?
-- ============================================

SELECT
    PRSCRBR_TYPE                                                AS SPECIALTY,
    SUM(TOT_DRUG_CST::FLOAT)                                    AS TOTAL_SPEND,
    SUM(BRND_TOT_DRUG_CST::FLOAT)                               AS BRANDED_SPEND,
    SUM(GNRC_TOT_DRUG_CST::FLOAT)                               AS GENERIC_SPEND,
    ROUND(SUM(BRND_TOT_DRUG_CST::FLOAT) /
          NULLIF(SUM(TOT_DRUG_CST::FLOAT),0) * 100, 2)          AS BRANDED_PCT,
    ROUND(SUM(GNRC_TOT_DRUG_CST::FLOAT) /
          NULLIF(SUM(TOT_DRUG_CST::FLOAT),0) * 100, 2)          AS GENERIC_PCT
FROM CMS_MEDICARE.RAW.PART_D_PRESCRIBERS
WHERE PRSCRBR_TYPE IS NOT NULL
AND BRND_TOT_DRUG_CST IS NOT NULL
AND GNRC_TOT_DRUG_CST IS NOT NULL
GROUP BY 1
ORDER BY TOTAL_SPEND DESC
LIMIT 20;


-- ============================================
-- Chapter 4: Are there risk signals?
-- Which prescribers show opioid rates
-- significantly above their specialty average?
-- ============================================

SELECT
    PRSCRBR_NPI,
    PRSCRBR_LAST_ORG_NAME                                       AS LAST_NAME,
    PRSCRBR_FIRST_NAME                                          AS FIRST_NAME,
    PRSCRBR_TYPE                                                AS SPECIALTY,
    PRSCRBR_STATE_ABRVTN                                        AS STATE,
    OPIOID_PRSCRBR_RATE::FLOAT                                  AS OPIOID_RATE,
    ROUND(AVG(OPIOID_PRSCRBR_RATE::FLOAT)
          OVER (PARTITION BY PRSCRBR_TYPE), 2)                  AS SPECIALTY_AVG_RATE,
    ROUND(OPIOID_PRSCRBR_RATE::FLOAT -
          AVG(OPIOID_PRSCRBR_RATE::FLOAT)
          OVER (PARTITION BY PRSCRBR_TYPE), 2)                  AS DEVIATION_FROM_AVG,
    CASE
        WHEN OPIOID_PRSCRBR_RATE::FLOAT >
             AVG(OPIOID_PRSCRBR_RATE::FLOAT)
             OVER (PARTITION BY PRSCRBR_TYPE) * 2
        THEN 'HIGH OUTLIER'
        ELSE 'NORMAL'
    END                                                         AS OUTLIER_FLAG
FROM CMS_MEDICARE.RAW.PART_D_PRESCRIBERS
WHERE OPIOID_PRSCRBR_RATE IS NOT NULL
AND OPIOID_PRSCRBR_RATE != ''
QUALIFY OUTLIER_FLAG = 'HIGH OUTLIER'
ORDER BY DEVIATION_FROM_AVG DESC;