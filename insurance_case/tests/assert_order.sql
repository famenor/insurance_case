SELECT * FROM (
    SELECT * FROM {{ ref('age_at_diagnosis') }}
    WHERE birth_date > consultation_date
) validation_errors