WITH patient_data AS (
    SELECT surrogated_id AS surrogated_certificate_id, name, certificate_number
    FROM silver.dim_certificate
),

term_begin_interactions AS (
    SELECT a.surrogated_certificate_id, 'Inicio de Cobertura' AS 'tipo_de_interaccion',
           b.date_name AS 'fecha_de_interaccion'
    FROM silver.fact_term a
    INNER JOIN silver.dim_term_begin_date b
    ON b.term_begin_date_id = a.term_begin_date_id
),

term_end_interactions AS (
    SELECT a.surrogated_certificate_id, 'Fin de Cobertura' AS 'tipo_de_interaccion',
           b.date_name AS 'fecha_de_interaccion'
    FROM silver.fact_term a
    INNER JOIN silver.dim_term_end_date b
    ON b.term_end_date_id = a.term_end_date_id
),

consultations AS (
    SELECT a.surrogated_certificate_id, 'Consulta Médica' AS 'tipo_de_interaccion',
           b.date_name AS 'fecha_de_interaccion'
    FROM silver.fact_consultation a
    INNER JOIN silver.dim_consultation_date b
    ON b.consultation_date_id = a.consultation_date_id
),

incidents AS (
    SELECT a.surrogated_certificate_id, 'Incidente Reportado' AS 'tipo_de_interaccion',
           b.date_name AS 'fecha_de_interaccion'
    FROM silver.fact_claim a
    INNER JOIN silver.dim_incident_date b
    ON b.incident_date_id = a.incident_date_id
),

first_expenses AS (
    SELECT a.surrogated_certificate_id, 'Primer Gasto' AS 'tipo_de_interaccion',
           b.date_name AS 'fecha_de_interaccion'
    FROM silver.fact_claim a
    INNER JOIN silver.dim_first_expense_date b
    ON b.first_expense_date_id = a.first_expense_date_id
),

payments AS (
    SELECT a.surrogated_certificate_id, 'Pago Realizado' AS 'tipo_de_interaccion',
           b.date_name AS 'fecha_de_interaccion'
    FROM silver.fact_claim a
    INNER JOIN silver.dim_payment_date b
    ON b.payment_date_id = a.payment_date_id
),

interacciones AS (
    SELECT * FROM term_begin_interactions
    UNION ALL
    SELECT * FROM term_end_interactions
    UNION ALL
    SELECT * FROM consultations
    UNION ALL
    SELECT * FROM incidents
    UNION ALL
    SELECT * FROM first_expenses
    UNION ALL
    SELECT * FROM payments
),

final AS (
    SELECT c.surrogated_certificate_id,
           c.certificate_number, 
           c.name,
           i.tipo_de_interaccion,
           i.fecha_de_interaccion
    FROM patient_data c
    INNER JOIN interacciones i
    ON c.surrogated_certificate_id = i.surrogated_certificate_id
    ORDER BY c.certificate_number, i.fecha_de_interaccion
)

SELECT * FROM final
