{% test not_audit_passed(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE audit_passed <> 'Sí'

{% endtest %}