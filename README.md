# FLUJO DE DATOS PARA ASEGURADORA

## CONTENIDO

  1 - Perfilamiento de Datos
  
  2 - Implementación del Almacén de Datos
  
  3 - Tablas Oro
  
  4 - Estructura del Repositorio
  
  5 - Discusión y Mejoras
  
 
## 1.- PERFILAMIENTO DE DATOS

Se recibieron 5 archivos con datos de la aseguradora, lo primero que se hizo fue hacer un análisis de perfilamiento, a continuación se presenta un resumen de este análisis (se puede revisar el analisis completo en el cuaderno https://github.com/famenor/insurance_case/blob/main/appendix_a_profiling.ipynb )

### Certificados

Cada cerfificado está ligado directamente a un cliente, contiene la información personal de los clientes.

- Llave natural en campo id y un número de certificado único.
- Nombre, email, edad (entre 40 y 85 años), fecha de nacimiento (entre 1941 y 1985), género (Posibles valores M y F) y ciudad (47 ciudades españolas).
- No se encontraron valores nulos.

### Términos

 Contienen los periodos de inicio y fin asociados a un cértificado, podría tratarse de la validez en la que una membresia o un seguro fue válido.

 - Llave natural en campo id.
 - La relación Certificados-Términos es 1:n mediante el id del certificado.
 - No se encontraron valores nulos.
 - Las fechas de inicio y témino oscilan entre 2021 y 2025.

### Patologias / Catálogo de enfermedades CIE

Contiene una lista de patologías y sus códigos internacionales

 - Llave natural: id
 - No se encontraron valores nulos

Sin embargo se encontraron faltantes por lo que se decidió incorporar un catálogo completo en su lugar. 

### Consultas y Diagnósticos

Contiene la información de las consultas médicas a las que acudieron los clientes, en el archivo original había una columna con formato JSON con información detallada, se decidió preprocesar este archivo para poderlo perfilar, finalmente se dividió en dos tablas.

La primera tabla contiene toda la información uno a uno correspondiente a la consulta médica.

- Llave natural: id
- La relación Cértificados-Consultas es 1:n mediante el id del certificado.
- Las fechas de consultas oscilan entre 2021 y 2025.
- Especialidades médicas (4 posibles valores).
- Indicador de receta o de orden médica.
- Indicador de si habrá siguiente consulta.
- Campos textuales con objetivos del paciente y médico.
- Campo textual con descripción de la consulta por parte del médico.

La segunda tabla contiene una lista de diagnosticos derivados de la consulta médica.

- No tiene llave natural.
- La relación Consulta-Diagnósticos es 1:n mediante el id de la consulta.
- Los diagnósticos (de haberlos) están representados por un identificador que se enlaza con el catálogo de patologías.

### Reclamos

Contiene los reclamos hechos por los clientes asegurados para recibir algún pago derivado de accidentes o enfermedades cubiertas. 

- Llave natural: CLAIM ID
- La relación Cértificados-Reclamos es 1:n mediante el número del certificado (en este caso no se usa el id)
- Existen 32 valores para las provincias de España.
- Contiene un diagnostico con clave CIE.
- Fechas de ocurrencia, primer gasto y pago, las cuales siguen un orden cronológico.
- Campos de pagos y gastos, con valores que oscilan entre -85000 y 1000000 (con formato utilizado en España).
- Campo de causa con posibles dos valores entre ACCIDENTE y ENFERMEDAD.
- Tipo de pago, con un solo posible valor: PAGO DIRECTO.
- Se encontro un solo valor para tipo de pago: PAGO DIRECTO
- La columna NumCertificado es llave foranea hacia certificados (numero_certificado)
- En general no habia valores nulos, pero se supondrá más adelante que algunos campos pueden tenerlos.

![](https://github.com/famenor/insurance_case/blob/main/pictures/diagrama_er.jpg)

## 2.- Implementación del Almacén de Datos

Con el perfilamiento de la sección anterior ahora se conoce la estructura mediante la cual los datos se relacionan, así como las reglas principales que se deben de cumplir.

Se proponen 3 niveles de madurez segun el procesamiento de los datos:

a) Nivel Bronce: La fuente ha pasado por un proceso de extracción en el cual se han formateado los datos y se han aplicado políticas de validación e integridad, ningun dato se descarta pero aquellos registros que no cumplen con las políticas son etiquetados para fines de auditoría.

b) Nivel Plata: La fuente ha sido filtrada al descartar las filas etiquetadas del nivel anterior, los datos también han sido modelados con estructura de dimensión y hechos.

c) Nivel Oro: Las fuentes han sido utilizadas para generar información de alto valor.

### Herramientas para procesamiento

- Debido a la complejidad que requiere el tratamiento prematuro de los datos, se utilizará Python para procesar los datos hasta que lleguen al nivel plata.
- Una vez que los datos estén en el nivel plata, se utilizará DBT hasta que lleguen al nivel oro.
- Los procesos serán ejecutados mediante Dagster.

### Herramientas para almacenamiento

- Los archivos de entrada y salida se ubicarán en este mismo repositorio.
- Las tablas con nivel bronce, plata y oro se guardarán en Duckdb (emulando a un almacén de datos).

### Politicas de validación e integridad

Se definieron 7 políticas para aplicar, en las cuales se detectarán valores nulos, valores fuera de intervalo, valores con formato incorrecto, valores no únicos, valores fuera de lista (incluye llave foranea) y valores con orden incorrecto:

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_screen.jpg)

Cuando se aplique una política y se encuentre un error, este será almacenado en una tabla de hechos especial, con esta información será posible etiquetar a los registros que no cumplieron con la política y en futuras iteraciones robustecer el proceso de auditoría.

![](https://github.com/famenor/insurance_case/blob/main/pictures/fact_event_error_datail.jpg)

### Dimensión Fecha

Todos las fechas fueron sustituidas con un identificador que apunta a una vista particular, estas vistas se generaron a partir de una dimensión fecha principal:

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_date.jpg)

### Dimensión Certificado o Cliente

Se aplicaron las siguientes políticas de validación:

~~~python
        #CHECK NULL VALUES
        columns = ['name', 'email', 'age', 'city', 'birth_date', 'certificate_number', 'gender']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('certificate_id')
        self.facade_screens.apply_screen_is_not_unique('certificate_number')

        #CHECK NOT DIGIT STRING VALUES
        self.facade_screens.apply_screen_is_not_digit_string('certificate_number', 6)

        #CHECK NOT DATE FORMAT VALUES
        self.facade_screens.apply_screen_is_not_date_format('birth_date', '%Y-%m-%d')

        self.data['birth_date'] = pd.to_datetime(self.data['birth_date'], format='%Y-%m-%d')

        #CHECK OUT OF BOUNDS VALUES
        self.facade_screens.apply_screen_is_out_of_bounds_value('age', 0, 100)
        self.facade_screens.apply_screen_is_out_of_bounds_value('birth_date', pd.Timestamp('1925-01-01'), pd.Timestamp('2024-12-31'))

        #CHECK OUT OF LIST VALUES
        self.facade_screens.apply_screen_is_out_of_list_value('gender', ['M', 'F'])
~~~

y se generó la dimensión bronce:

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_certificate_bronze.jpg)

Posteriormente, para el nivel plata se enmascararon los datos sensibles, se reemplazaron las llaves naturales por subrogadas (propia y en fecha de nacimiento):

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_certificate_silver.jpg)

### Dimensión Patologías CIE

Se aplicaron las siguientes políticas de validación:

~~~python
        self.facade_screens.setup(data=self.data, table_name='cie_catalog', identifier='cie_id')
        
        #CHECK NULL VALUES
        columns = ['cie_name']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('cie_id')
~~~

y se generó la dimensión bronce:

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_cie_bronze.jpg)

Posteriormente, para el nivel plata, se reemplazaron las llaves naturales por subrogadas (propia):

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_cie_silver.jpg)

### Hechos de Términos

Se aplicaron las siguientes políticas de validación, en las tablas de hechos sí se incluye una columna especial para indicar si pasó la auditoria:

~~~python
        conn = duckdb.connect("insurance_case.db")
        certificate_ids = conn.sql("SELECT certificate_id FROM silver.dim_certificate").df()
        conn.close()

        self.facade_screens.setup(data=self.data, table_name='term_dummy', identifier='term_id')
        
        #CHECK NULL VALUES
        columns = ['certificate_id', 'term_begin_date', 'term_end_date']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('term_id')

        #CHECK NOT DATE FORMAT VALUES
        self.facade_screens.apply_screen_is_not_date_format('term_begin_date', '%Y-%m-%d')
        self.facade_screens.apply_screen_is_not_date_format('term_end_date', '%Y-%m-%d')

        self.data['term_begin_date'] = pd.to_datetime(self.data['term_begin_date'], format='%Y-%m-%d')
        self.data['term_end_date'] = pd.to_datetime(self.data['term_end_date'], format='%Y-%m-%d')

        #CHECK OUT OF BOUNDS VALUES
        self.facade_screens.apply_screen_is_out_of_bounds_value('term_begin_date', pd.Timestamp('2020-01-01'), pd.Timestamp('2030-12-31'))
        self.facade_screens.apply_screen_is_out_of_bounds_value('term_end_date', pd.Timestamp('2020-01-01'), pd.Timestamp('2030-12-31'))

        #CHECK CRONOLOGICAL ORDER
        self.facade_screens.apply_screen_is_lower_than('term_end_date', 'term_begin_date')

        #CHECK OUT OF LIST VALUES
        self.facade_screens.apply_screen_is_out_of_list_value('certificate_id', certificate_ids['certificate_id'].tolist())
~~~

quedando así la tabla de hechos bronce:

![](https://github.com/famenor/insurance_case/blob/main/pictures/fact_term_bronze.jpg)

Posteriormente, para el nivel plata, se sustituyeron llaves naturales foraneas por subrogadas y se filtraron datos que no pasaron la auditoria:

![](https://github.com/famenor/insurance_case/blob/main/pictures/fact_term_silver.jpg)

### Hechos de Consultas

Se aplicaron las siguientes políticas de validación:

~~~python
        conn = duckdb.connect("insurance_case.db")
        certificate_ids = conn.sql("SELECT certificate_id FROM silver.dim_certificate").df()
        conn.close()

        self.facade_screens.setup(data=self.data, table_name='consultations', identifier='consultation_id')
        
        #CHECK NULL VALUES
        columns = ['certificate_id', 'consultation_date', 'specialty', 'placed_by', 'pause_consultations', 'prescription_or_medical_order']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('consultation_id')

        #CHECK NOT DATE FORMAT VALUES
        self.facade_screens.apply_screen_is_not_date_format('consultation_date', '%Y-%m-%d')

        self.data['consultation_date'] = pd.to_datetime(self.data['consultation_date'], format='%Y-%m-%d')

        #CHECK OUT OF BOUNDS VALUES
        self.facade_screens.apply_screen_is_out_of_bounds_value('consultation_date', pd.Timestamp('2020-01-01'), pd.Timestamp('2030-12-31'))

        #CHECK OUT OF LIST VALUES
        self.facade_screens.apply_screen_is_out_of_list_value('certificate_id', certificate_ids['certificate_id'].tolist())
        self.facade_screens.apply_screen_is_out_of_list_value('pause_consultations', ['Sí', 'No', 'Sin especificar'])
        self.facade_screens.apply_screen_is_out_of_list_value('prescription_or_medical_order', ['Sin Especificar', 'Orden Médica', 'Receta Médica', 'Ambos', 'Ninguno'])
        self.facade_screens.apply_screen_is_out_of_list_value('specialty', ['Medicina General', 'Geriatría', 'Gerontología', 'Nutrición'])
~~~

quedando así la tabla de hechos bronce:

![](https://github.com/famenor/insurance_case/blob/main/pictures/fact_consultation_bronze.jpg)

Posteriormente, para el nivel plata, se sustituyeron llaves naturales foraneas por subrogadas y se filtraron datos que no pasaron la auditoria:

![](https://github.com/famenor/insurance_case/blob/main/pictures/fact_consultation_silver.jpg)

### Hechos de Diágnosticos (Puente)

En este caso, se decidió optar por una tabla puente que permita enlazar múltiples valores de la dimensión de patologías con las consultas médicas, se aplicaron las siguientes validaciones:

~~~python
        conn = duckdb.connect("insurance_case.db")
        consultation_identifiers = conn.sql("SELECT consultation_id FROM silver.fact_consultation").df()
        cie_identifiers = conn.sql("SELECT cie_id FROM silver.dim_cie").df()
        conn.close()
        
        self.facade_screens.setup(data=self.data, table_name='consultation_diagnoses', identifier='consultation_diagnosis_id')
        
        #CHECK NULL VALUES
        columns = ['consultation_id', 'cie_id']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('consultation_diagnosis_id')

        #CHECK OUT OF LIST VALUES
        self.facade_screens.apply_screen_is_out_of_list_value('consultation_id', consultation_identifiers['consultation_id'].tolist())
        self.facade_screens.apply_screen_is_out_of_list_value('cie_id', cie_identifiers['cie_id'].tolist())
~~~

quedando así la tabla de hechos bronce:

![](https://github.com/famenor/insurance_case/blob/main/pictures/bridge_consultation_diagnosis_bronze.jpg)

Posteriormente, para el nivel plata, se sustituyeron llaves naturales foraneas por subrogadas y se filtraron datos que no pasaron la auditoria:

![](https://github.com/famenor/insurance_case/blob/main/pictures/bridge_consultation_diagnosis_silver.jpg)

### Hechos de Reclamos

Se aplicaron las siguientes políticas de validación:

~~~python
        conn = duckdb.connect("insurance_case.db")
        certificate_numbers = conn.sql("SELECT certificate_number FROM silver.dim_certificate").df()
        cie_identifiers = conn.sql("SELECT cie_id FROM silver.dim_cie").df()
        conn.close()
        
        self.facade_screens.setup(data=self.data, table_name='claims', identifier='claim_id')
        
        #CHECK NULL VALUES
        columns = ['state', 'cie_id', 'diagnosis', 'incident_date', 'payments', 'coinsurance',
                   'ivarec', 'deductible', 'incident_reason', 'cve_mes', 'month_cont_date', 'payment_type', 'provider', 'certificate_number']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('claim_id')

        #CHECK NOT DIGIT STRING VALUES
        self.facade_screens.apply_screen_is_not_digit_string('certificate_number', 6)

        #CHECK NOT DATE FORMAT VALUES
        self.facade_screens.apply_screen_is_not_date_format('incident_date', '%d/%m/%Y')
        self.facade_screens.apply_screen_is_not_date_format('payment_date', '%d/%m/%Y')
        self.facade_screens.apply_screen_is_not_date_format('first_expense_date', '%d/%m/%Y')
        self.facade_screens.apply_screen_is_not_date_format('month_cont_date', '%d/%m/%Y')

        self.data['incident_date'] = pd.to_datetime(self.data['incident_date'], format='%d/%m/%Y')
        self.data['payment_date'] = pd.to_datetime(self.data['payment_date'], format='%d/%m/%Y')
        self.data['first_expense_date'] = pd.to_datetime(self.data['first_expense_date'], format='%d/%m/%Y')
        self.data['month_cont_date'] = pd.to_datetime(self.data['month_cont_date'], format='%d/%m/%Y')

        #CHECK OUT OF BOUNDS VALUES
        self.facade_screens.apply_screen_is_out_of_bounds_value('incident_date', pd.Timestamp('2020-01-01'), pd.Timestamp('2030-12-31'))
        self.facade_screens.apply_screen_is_out_of_bounds_value('payment_date', pd.Timestamp('2020-01-01'), pd.Timestamp('2030-12-31'))
        self.facade_screens.apply_screen_is_out_of_bounds_value('first_expense_date', pd.Timestamp('2020-01-01'), pd.Timestamp('2030-12-31'))
        self.facade_screens.apply_screen_is_out_of_bounds_value('month_cont_date', pd.Timestamp('2020-01-01'), pd.Timestamp('2030-12-31'))

        self.facade_screens.apply_screen_is_out_of_bounds_value('ocurrido', -1000000, 10000000)
        self.facade_screens.apply_screen_is_out_of_bounds_value('payments', 0, 10000000)
        self.facade_screens.apply_screen_is_out_of_bounds_value('coinsurance', 0, 1000000)
        self.facade_screens.apply_screen_is_out_of_bounds_value('ivarec', 0, 1000000)
        self.facade_screens.apply_screen_is_out_of_bounds_value('deductible', 0, 1000000)

        #CHECK CRONOLOGICAL ORDER
        self.facade_screens.apply_screen_is_lower_than('month_cont_date', 'payment_date')
        self.facade_screens.apply_screen_is_lower_than('payment_date', 'first_expense_date')
        self.facade_screens.apply_screen_is_lower_than('first_expense_date', 'incident_date')

        self.data['incident_date_id'] = self.data['incident_date'].dt.strftime('%Y%m%d')
        self.data['payment_date_id'] = self.data['payment_date'].dt.strftime('%Y%m%d')
        self.data['first_expense_date_id'] = self.data['first_expense_date'].dt.strftime('%Y%m%d')
        self.data['month_cont_date_id'] = self.data['month_cont_date'].dt.strftime('%Y%m%d')

        self.data['incident_date_id'] = self.data['incident_date_id'].fillna('-2').astype(int)
        self.data['payment_date_id'] = self.data['payment_date_id'].fillna('-2').astype(int)
        self.data['first_expense_date_id'] = self.data['first_expense_date_id'].fillna('-2').astype(int)
        self.data['month_cont_date_id'] = self.data['month_cont_date_id'].fillna('-2').astype(int)

        #CHECK OUT OF LIST VALUES
        self.facade_screens.apply_screen_is_out_of_list_value('certificate_number', certificate_numbers['certificate_number'].tolist())
        self.facade_screens.apply_screen_is_out_of_list_value('incident_reason', ['Enfermedad', 'Accidente'])
        self.facade_screens.apply_screen_is_out_of_list_value('payment_type', ['Pago Directo'])
        self.facade_screens.apply_screen_is_out_of_list_value('cie_id', cie_identifiers['cie_id'].tolist())

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])
~~~

quedando así la tabla de hechos bronce:

![](https://github.com/famenor/insurance_case/blob/main/pictures/fact_claim_bronze.jpg)

Posteriormente, para el nivel plata, se sustituyeron llaves naturales foraneas por subrogadas y se filtraron datos que no pasaron la auditoria:

![](https://github.com/famenor/insurance_case/blob/main/pictures/fact_claim_silver.jpg)

## 3.- Tablas Oro

Una vez cargadas las tablas de dimensiones y hechos en el almacén de datos se pueden generar productos de alto valor.

### Interacción de Clientes

Se comenzó con la interacción de clientes mediante la siguiente consulta en DBT:

~~~sql
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
~~~

con la cual se generó la tabla oro:

![](https://github.com/famenor/insurance_case/blob/main/pictures/customer_interaction_gold.jpg)

Esta tabla está disponible en el enlace https://github.com/famenor/insurance_case/blob/main/datalake/gold/customer_interaction.csv

### Edades al Diagnosticar

Esta tabla se generó mediante la siguiente consulta en DBT:

~~~sql
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
~~~

que al ejecutarsé generó la tabla oro:

![](https://github.com/famenor/insurance_case/blob/main/pictures/age_at_diagnosis.jpg)

la cual está disponible en el enlace https://github.com/famenor/insurance_case/blob/main/datalake/gold/age_at_diagnosis.csv

## 4.- Estructura del Repositorio


## 5 .- Discusión y Mejoras

Hasta antes de realizar esta prueba, no había utilizado Dagster ni Duckdb; con DBT tenía poca experiencia, además de ello decidí incorporar algunos componentes que antes no había implementado como son el etiquetado de filas erroneas a partir de políticas de validación, manejo de llaves surrogadas y dimensiones especiales para fechas y auditoría. Las mejoras que veo son las siguientes:

- Ahora considero que es factible implementar en DBT la generación de tablas a nivel plata, al principio solo consideraba que sería útil para el nivel oro.
- Para el preprocesamiento, ensambles de auditoría y otros componentes considero que puede ser mejor usar otras herramientas.
- Entender mejor los componentes de Dagster y usarlos adecuadamente.
- Modularizar mejor el proyecto, acomodar las clases implementadas en archivos para cada subsistema.
- Los campos de texto capturado son muy importantes en el sector médico, sí es importante incorporar módulos para el analisis inteligente de texto y extracción de rasgos.
- El catálogo del CIE parece tener diferenctes versiones o formatos, algunos de las patologías no pudieron ser asociadas por mínimas discrepancias en los códigos CIE.
- Incorporar pruebas unitarias y más pruebas de validación de datos, especialmente en DBT.





