WITH diagnoses AS (
    SELECT dc.surrogated_id AS surrogated_certificate_id, date_01.date AS birth_date,
           fc.consultation_id, date_02.date AS consultation_date, dd.cie_id, dd.cie_name
    
    FROM silver.dim_certificate dc

    INNER JOIN silver.fact_consultation fc
    ON dc.surrogated_id = fc.surrogated_certificate_id

    INNER JOIN silver.bridge_consultation_diagnosis bcd
    ON fc.consultation_id = bcd.consultation_id

    INNER JOIN silver.dim_cie dd
    ON bcd.surrogated_cie_id = dd.surrogated_id

    INNER JOIN silver.dim_birth_date date_01 ON date_01.birth_date_id = dc.birth_date_id
    INNER JOIN silver.dim_consultation_date date_02 ON date_02.consultation_date_id = fc.consultation_date_id
),

age_at_diagnosis AS (
    SELECT surrogated_certificate_id, consultation_id, 
           cie_id, cie_name, birth_date, consultation_date,
           DATEDIFF('year', birth_date, consultation_date) AS 'age_at_diagnosis'
    FROM diagnoses
)

SELECT * FROM age_at_diagnosis ORDER BY surrogated_certificate_id, age_at_diagnosis
