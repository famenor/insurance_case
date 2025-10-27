# FLUJO DE DATOS PARA ASEGURADORA

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

## 2.- Implementación para el Almacén de Datos

Con el perfilamiento de la sección anterior ahora se conoce la estructura mediante la cual los datos se relacionan, así como las reglas principales que se deben de cumplir.

Se proponen 3 niveles de madurez segun el procesamiento de los datos:

a) Nivel Bronce: La fuente ha pasado por un proceso de extracción en el cual se han formateado los datos y se han aplicado políticas de validación e integridad, ningun dato se descarta pero aquellos registros que no cumplen con las políticas son etiquetados para fines de auditoría.
b) Nivel Plata: La fuente ha sido filtrada al descartar las filas etiquetadas del nivel anterior, los datos también han sido modelados con estructura de dimensión y hechos.
c) Nivel Oro: Las fuentes han sido utilizadas para generar información de alto valor.

- Debido a la complejidad que requiere el tratamiento prematuro de los datos, se utilizará Python para procesar los datos hasta que lleguen al nivel plata.
- Una vez que los datos estén en el nivel plata, se utilizará DBT hasta que lleguen al nivel oro.



Validaciones de certificados:
 - El número de certificado debe tener una relación uno a uno con la llave natural
 - Edad debe corresponder con la fecha de nacimiento
 - El año de nacimiento debe estar entre 1925 y 2025
 - Numero de certificado debe ser numerico y con 6 o más digitos
 - Géneros con valores M y F
 - Las ciudades deberán estar ligadas a un conjunto de valores válidos
 - Todos los campos son requeridos




 Validaciones de terminos:
  - La fecha de inicio debe ser mayor que 2020 y la fecha de termino debe ser menor que 2050
  - La fecha de inicio debe ser menor que la fecha de termino
  - Validar la llave foranea del certificado
  - Todos los campos son requeridos

 Validaciones:
  - Todos los campos son requeridos
  - El codigo de la patologia debe tener una relación uno a uno con la llave natural





