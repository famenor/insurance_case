import dagster as dg
import logging

import pandas as pd
import numpy as np
import datetime
import duckdb
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


#############################################################################

## SUBSYSTEM 03 FOR DATA EXTRACTION
class DatawarehouseResourcesCreator(ABC):
    
    def __init__(self):
        pass

    def init_schemas(self):
        conn = duckdb.connect("insurance_case.db")
        conn.sql('CREATE SCHEMA IF NOT EXISTS governance')
        conn.sql('CREATE SCHEMA IF NOT EXISTS bronze')
        conn.sql('CREATE SCHEMA IF NOT EXISTS silver')
        conn.sql("CREATE SCHEMA IF NOT EXISTS gold")
        conn.close()
    
    def init_error_tables(self):
        conn = duckdb.connect("insurance_case.db")

        conn.sql("""
            CREATE OR REPLACE TABLE governance.fact_error_event (
                error_event_id INTEGER,
                batch_id INTEGER
            )
        """)

        conn.sql("""
            CREATE OR REPLACE TABLE governance.fact_error_event_detail (
                error_event_id INTEGER,
                batch_id INTEGER,
                screen_id INTEGER,	
                error_utc_timestamp TIMESTAMP,
                table_name VARCHAR, 
                column_name VARCHAR,
                record_identifier VARCHAR,
                original_value VARCHAR,
                replaced_value VARCHAR,
                error_condition VARCHAR
            )
        """)

        conn.close()

#SOURCE EXTRACTOR DEFINITION
class AbstractSourceExtractor(ABC):

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def screen_data(self):
        pass

class AbstractFileExtractor(AbstractSourceExtractor):

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def screen_data(self):
        pass

class AbstractDatabaseExtractor(AbstractSourceExtractor):

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def screen_data(self):
        pass

class GenericCsvExtractor(AbstractFileExtractor):

    def __init__(self):
        self.data = None
        self.facade_screens = FacadeValidationScreens()
        self.errors = None

    @abstractmethod
    def extract_data(self, file_path: str):
        pass

    @abstractmethod
    def screen_data(self):
        pass

    @abstractmethod
    def export_data():
        pass
    
class CertificatesCsvExtractor(GenericCsvExtractor):

    def extract_data(self, file_path):
        print('Reading file: ', file_path)
        self.data = pd.read_csv(file_path)
        self.data = self.data.rename(columns={'id': 'certificate_id', 
                                              'nombre': 'name',
                                              'email': 'email',
                                              'edad': 'age',
                                              'ciudad': 'city',
                                              'fecha_nacimiento': 'birth_date',
                                              'numero_certificado': 'certificate_number',
                                              'sexo': 'gender'})
        
        self.data['age'] = pd.to_numeric(self.data['age'], errors='raise')
        self.data['certificate_number'] = self.data['certificate_number'].astype(str)

    def screen_data(self):
        print('Screening data...')

        self.facade_screens.setup(data=self.data, table_name='certificate_dummy', identifier='certificate_id')
        
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

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])

    def export_data(self):

        if self.errors is None:
            data = self.data

            conn = duckdb.connect("insurance_case.db")
            conn.sql("CREATE OR REPLACE TABLE bronze.dim_certificate AS SELECT * FROM data")
            conn.close()

        else:
            raise Exception('Dimension data contains errors. Cannot export to datawarehouse.')

class TermsCsvExtractor(GenericCsvExtractor):

    def extract_data(self, file_path):
        print('Reading file: ', file_path)
        self.data = pd.read_csv(file_path)
        self.data = self.data.rename(columns={'id': 'term_id', 
                                              'certificate_number': 'certificate_id',
                                              'fecha_inicio_vigencia': 'term_begin_date',
                                              'fecha_fin_periodo': 'term_end_date'})

    def screen_data(self):
        print('Screening data...')

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

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])

        #ADD AUDIT FACT COLUMN
        self.data['audit_passed'] = 'Sí'
        audit_dim_assembler = AuditDimensionAssembler(self.errors, 'term_dummy')
        unsolved_rows = audit_dim_assembler.get_unsolved_rows()
        self.data.loc[self.data['term_id'].isin(unsolved_rows), 'audit_passed'] = 'No'

    def export_data(self):

        if self.errors is None:
            data = self.data

            conn = duckdb.connect("insurance_case.db")
            conn.sql("CREATE OR REPLACE TABLE bronze.fact_term AS SELECT * FROM data")
            conn.close()  

class CieCsvExtractor(GenericCsvExtractor):

    def extract_data(self, file_path):
        print('Reading file: ', file_path)
        self.data = pd.read_csv(file_path)
        self.data = self.data.rename(columns={'cie_code': 'cie_id', 
                                              'cie_name': 'cie_name'})

        #DELETE 'X' CHARACTERS AT THE END OF CIE IDENTIFIERS
        #self.data['cie_id'] = self.data['cie_id'].str.rstrip('X')

    def screen_data(self):
        print('Screening data...')

        self.facade_screens.setup(data=self.data, table_name='cie_catalog', identifier='cie_id')
        
        #CHECK NULL VALUES
        columns = ['cie_name']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('cie_id')

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])

    def export_data(self):

        if self.errors is None:
            data = self.data

            conn = duckdb.connect("insurance_case.db")
            conn.sql("CREATE OR REPLACE TABLE bronze.dim_cie AS SELECT * FROM data")
            conn.close()

        else:
            raise Exception('Dimension data contains errors. Cannot export to datawarehouse.')

class ConsultationsCsvExtractor(GenericCsvExtractor):

    def extract_data(self, file_path):
        print('Reading file: ', file_path)
        self.data = pd.read_csv(file_path)
        self.data = self.data.rename(columns={'id': 'consultation_id',
                                              'certificate_number': 'certificate_id',
                                              'fecha_consulta': 'consultation_date',
                                              'specialty': 'specialty',
                                              'placed_by': 'placed_by',
                                              'day_note_consultation_observation': 'consultation_observation',
                                              'day_note_next_consultation_pending': 'next_consultation_pending',
                                              'day_note_needs_prescription_or_medical_order': 'prescription_or_medical_order',
                                              'patiend_goal': 'patient_goal',
                                              'specialist_goal': 'specialist_goal',
                                              'pause_consultations': 'pause_consultations'})

        self.data.loc[self.data['pause_consultations'] == 'no', 'pause_consultations'] = 'No'
        self.data.loc[self.data['pause_consultations'] == 'yes', 'pause_consultations'] = 'Sí'
        self.data['pause_consultations'] = self.data['pause_consultations'].fillna('Sin especificar')

        self.data.loc[self.data['prescription_or_medical_order'] == 'none', 'prescription_or_medical_order'] = 'Ninguno'
        self.data.loc[self.data['prescription_or_medical_order'] == 'medicalOrder', 'prescription_or_medical_order'] = 'Orden Médica'
        self.data.loc[self.data['prescription_or_medical_order'] == 'medicalPrescription', 'prescription_or_medical_order'] = 'Receta Médica'
        self.data.loc[self.data['prescription_or_medical_order'] == 'both', 'prescription_or_medical_order'] = 'Ambos'
        self.data['prescription_or_medical_order'] = self.data['prescription_or_medical_order'].fillna('Sin Especificar')

        self.data.loc[self.data['specialty'] == 'general_medicine', 'specialty'] = 'Medicina General'
        self.data.loc[self.data['specialty'] == 'geriatrics', 'specialty'] = 'Geriatría'
        self.data.loc[self.data['specialty'] == 'gerontology', 'specialty'] = 'Gerontología'
        self.data.loc[self.data['specialty'] == 'nutrition', 'specialty'] = 'Nutrición'

    def screen_data(self):
        print('Screening data...')

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

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])


        #ADD AUDIT FACT COLUMN
        self.data['audit_passed'] = 'Sí'
        audit_dim_assembler = AuditDimensionAssembler(self.errors, 'consultations')
        unsolved_rows = audit_dim_assembler.get_unsolved_rows()
        self.data.loc[self.data['consultation_id'].isin(unsolved_rows), 'audit_passed'] = 'No'

    def export_data(self):

        if self.errors is None:
            data = self.data

            conn = duckdb.connect("insurance_case.db")
            conn.sql("CREATE OR REPLACE TABLE bronze.fact_consultation AS SELECT * FROM data")
            conn.close()  

class ConsultationDiagnosesCsvExtractor(GenericCsvExtractor):

    def extract_data(self, file_path):

        path_pathologies = '../datalake/raw/pathologies.csv'
        pathologies = pd.read_csv(path_pathologies, usecols=['code', 'id'])
        pathologies = pathologies.rename(columns={'code': 'cie_id', 'id': 'pathology_id'})

        self.data = pd.read_csv(file_path)
        self.data = self.data.rename(columns={'consultation_id': 'consultation_id',
                                              'diagnosis': 'pathology_id'}) 
        self.data['consultation_diagnosis_id'] = np.arange(1, self.data.shape[0] + 1)

        self.data = self.data.merge(pathologies, how='inner', on='pathology_id')
        self.data = self.data.drop(columns=['pathology_id'])
    

    def screen_data(self):
        
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

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])

        #ADD AUDIT FACT COLUMN
        self.data['audit_passed'] = 'Sí'
        audit_dim_assembler = AuditDimensionAssembler(self.errors, 'consultation_diagnoses')
        unsolved_rows = audit_dim_assembler.get_unsolved_rows()
        self.data.loc[self.data['consultation_diagnosis_id'].isin(unsolved_rows), 'audit_passed'] = 'No'

    def export_data(self):

        data = self.data

        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE bronze.bridge_consultation_diagnosis AS SELECT * FROM data")
        conn.close() 


class ClaimsCsvExtractor(GenericCsvExtractor):

    def extract_data(self, file_path):
        print('Reading file: ', file_path)
        self.data = pd.read_csv(file_path)
        self.data = self.data.rename(columns={'SINIESTRO': 'claim_id',
                                              'STATE': 'state',
                                              'CIE10': 'cie_id',
                                              'DIAGNOSIS': 'diagnosis',
                                              'FECHA_OCURRIDO': 'incident_date',
                                              'FECHA_PAGO': 'payment_date',
                                              'FECHA_PRIMERGASTO': 'first_expense_date',
                                              'OCURRIDO': 'ocurrido',
                                              'PAGOS': 'payments',
                                              'COASEGURO': 'coinsurance',
                                              'IVAREC': 'ivarec',
                                              'DEDUCIBLE': 'deductible',
                                              'CAUSA': 'incident_reason',
                                              'CVE_MES': 'cve_mes',
                                              'MES_CONT': 'month_cont_date',
                                              'TIPO_PAGO': 'payment_type',
                                              'CLASF_PROV': 'provider',
                                              'NumCertificado': 'certificate_number'                                             
                                              }) 

        self.data['certificate_number'] = self.data['certificate_number'].astype(str)
        self.data['cie_id'] = self.data['cie_id'].str.replace('.','', regex=False)

        self.data.loc[self.data['incident_reason'] == 'ENFERMEDAD', 'incident_reason'] = 'Enfermedad'
        self.data.loc[self.data['incident_reason'] == 'ACCIDENTE', 'incident_reason'] = 'Accidente'

        self.data.loc[self.data['payment_type'] == 'PAGO DIRECTO', 'payment_type'] = 'Pago Directo'

        self.data.loc[self.data['payment_date'] == '45491', 'payment_date'] = None

        for column in ['ocurrido', 'payments', 'coinsurance', 'ivarec', 'deductible']:
            self.data[column] = self.data[column].astype(str)
            self.data[column] = self.data[column].str.replace('.', '', regex=False)
            self.data[column] = self.data[column].str.replace(',', '.', regex=False)
            self.data[column] = pd.to_numeric(self.data[column], errors='raise')

    def screen_data(self):
        print('Screening data...')

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

        #ADD AUDIT FACT COLUMN
        self.data['audit_passed'] = 'Sí'
        audit_dim_assembler = AuditDimensionAssembler(self.errors, 'claims')
        unsolved_rows = audit_dim_assembler.get_unsolved_rows()
        self.data.loc[self.data['claim_id'].isin(unsolved_rows), 'audit_passed'] = 'No'

    def export_data(self):

        data = self.data

        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE bronze.fact_claim AS SELECT * FROM data")
        conn.close() 



#############################################################################

## SUBSYSTEM 04 FOR DATA CLEANSING
class AbstractValidator(ABC):

    def __init__(self):
        pass
   
    @abstractmethod
    def validate(self, **kwargs) -> bool:
        pass

class ValidatorIsMissingValue(AbstractValidator):

    def validate(self, **kwargs):

        value = kwargs.get('value', None)       
        result = False

        if pd.isnull(value):
            result = True

        return result

class ValidatorIsOutOfBoundsValue(AbstractValidator):

    def validate(self, **kwargs):

        value = kwargs.get('value', None)
        min_value = kwargs.get('min_value', None)
        max_value = kwargs.get('max_value', None)
        
        result = False
        if value < min_value or value > max_value:
            result = True

        return result
    
class ValidatorIsOutOfListValue(AbstractValidator):

    def validate(self, **kwargs):
        
        value = kwargs.get('value', None)
        valid_values = kwargs.get('valid_values', [])
        result = False

        if value not in valid_values:
            result = True

        return result
    
class ValidatorIsNotDigitString(AbstractValidator):

    def validate(self, **kwargs):

        value = kwargs.get('value', None)
        number_of_digits = kwargs.get('number_of_digits', None)       
        result = False

        if not isinstance(value, str) or not value.isdigit():
            result = True

        if len(value) != number_of_digits:
            result = True

        return result
    
class ValidatorIsNotDateFormat(AbstractValidator):
    
    def validate(self, **kwargs):

        value = kwargs.get('value', None)
        date_format = kwargs.get('date_format', None)  

        if pd.isnull(value):
            return False     

        try:
            datetime.datetime.strptime(value, date_format)
            result = False
        
        except ValueError:
            result = True
        
        return result

class ValidatorIsLowerThan(AbstractValidator):

    def validate(self, **kwargs):

        value_a = kwargs.get('value_a', None)
        value_b = kwargs.get('value_b', None) 
      
        result = False

        if pd.isnull(value_a) or pd.isnull(value_b):
            return result

        if value_a < value_b:
            result = True

        return result
    

#SCREENS
class AbstractValidationScreen(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def apply_validation(self):
        pass

class ValidationScreenIsNotUnique(AbstractValidationScreen):

    def apply_validation(self, array):
        shape_01 = array.shape[0]
        shape_02 = array.nunique(dropna=True)

        if shape_01 != shape_02:
            raise Exception('Violation on uniqueness constraint detected.')

class ValidationScreenIsMisssingValue(AbstractValidationScreen):

    def apply_validation(self, array):
        validator = ValidatorIsMissingValue()
        results = array.apply(lambda x: validator.validate(value=x))
        return results
    
class ValidationScreenIsOutOfBoundsValue(AbstractValidationScreen):

    def apply_validation(self, array, min_value, max_value):
        validator = ValidatorIsOutOfBoundsValue()
        results = array.apply(lambda x: validator.validate(value=x, min_value=min_value, max_value=max_value))
        return results
    
class ValidationScreenIsOutOfListValue(AbstractValidationScreen):

    def apply_validation(self, array, valid_values):
        validator = ValidatorIsOutOfListValue()
        results = array.apply(lambda x: validator.validate(value=x, valid_values=valid_values))
        return results
    
class ValidationScreenIsNotDigitString(AbstractValidationScreen):
    
    def apply_validation(self, array, number_of_digits):
        validator = ValidatorIsNotDigitString()
        results = array.apply(lambda x: validator.validate(value=x, number_of_digits=number_of_digits))
        return results
    
class ValidationScreenIsNotDateFormat(AbstractValidationScreen):

    def apply_validation(self, array, date_format):
        validator = ValidatorIsNotDateFormat()
        results = array.apply(lambda x: validator.validate(value=x, date_format=date_format))
        return results

class ValidatorScreenIsLowerThan(AbstractValidationScreen):

    def apply_validation(self, array_a, array_b):
        validator = ValidatorIsLowerThan()
        results = pd.Series(zip(array_a, array_b)).apply(lambda x: validator.validate(value_a=x[0], value_b=x[1]))
        return results
    
    
#FACADE FOR SCREENS

class FacadeValidationScreens():

    def __init__(self):
        self.event_errors = []
        self.screen_is_not_unique = ValidationScreenIsNotUnique()
        self.screen_is_missing_value = ValidationScreenIsMisssingValue()
        self.screen_is_out_of_bounds_value = ValidationScreenIsOutOfBoundsValue()
        self.screen_is_out_of_list_value = ValidationScreenIsOutOfListValue()
        self.screen_is_not_digit_string = ValidationScreenIsNotDigitString()
        self.screen_is_not_date_format = ValidationScreenIsNotDateFormat()
        self.screen_is_lower_than = ValidatorScreenIsLowerThan()

    def setup(self, data, table_name, identifier):
        self.data = data
        self.table_name = table_name
        self.identifier = identifier

    def ensemble_error_event_detail(self, errors, column, screen_id):

        if errors.shape[0] == 0:
            return None
    
        utc_now = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        errors['error_event_id'] = 0
        errors['screen_id'] = screen_id
        errors['batch_id'] = 1
        errors['error_utc_timestamp'] = utc_now
        errors['table_name'] = self.table_name
        errors['column_name'] = column
        errors['record_identifier'] = errors[self.identifier]
        errors['original_value'] = errors[column]
        errors['replaced_value'] = None
        errors['error_condition'] = 'Sin Resolver'

        errors = errors[['error_event_id', 'batch_id', 'screen_id', 'error_utc_timestamp',
                         'table_name', 'column_name', 'record_identifier', 'original_value',
                         'replaced_value', 'error_condition']]
        
        self.event_errors.append(errors)

    def get_error_events_detail(self):
        
        if self.event_errors == []:
            return None
        
        else:
            return pd.concat(self.event_errors, ignore_index=True) 

    def apply_screen_is_not_unique(self, column):
        screen_id = 6
        self.screen_is_not_unique.apply_validation(self.data[column])     

    def apply_screen_is_missing_value(self, column):
        
        screen_id = 1        
        self.data['__screen__'] = self.screen_is_missing_value.apply_validation(self.data[column])
        
        errors = self.data.loc[self.data['__screen__'] == True][[self.identifier, column]].copy()
        self.ensemble_error_event_detail(errors, column, screen_id)

    def apply_screen_is_out_of_bounds_value(self, column, min_value, max_value):
        
        screen_id = 2        
        self.data['__screen__'] = self.screen_is_out_of_bounds_value.apply_validation(self.data[column], min_value, max_value)
        
        errors = self.data.loc[self.data['__screen__'] == True][[self.identifier, column]].copy()
        self.ensemble_error_event_detail(errors, column, screen_id)

    def apply_screen_is_out_of_list_value(self, column, valid_values):
        
        screen_id = 3        
        self.data['__screen__'] = self.screen_is_out_of_list_value.apply_validation(self.data[column], valid_values)
        
        errors = self.data.loc[self.data['__screen__'] == True][[self.identifier, column]].copy()
        self.ensemble_error_event_detail(errors, column, screen_id)

    def apply_screen_is_not_digit_string(self, column, number_of_digits):
        
        screen_id = 4        
        self.data['__screen__'] = self.screen_is_not_digit_string.apply_validation(self.data[column], number_of_digits)
        
        errors = self.data.loc[self.data['__screen__'] == True][[self.identifier, column]].copy()
        self.ensemble_error_event_detail(errors, column, screen_id)

    def apply_screen_is_not_date_format(self, column, date_format):
        
        screen_id = 5        
        self.data['__screen__'] = self.screen_is_not_date_format.apply_validation(self.data[column], date_format)
        
        errors = self.data.loc[self.data['__screen__'] == True][[self.identifier, column]].copy()
        self.ensemble_error_event_detail(errors, column, screen_id)

    def apply_screen_is_lower_than(self, column, reference_column):
        
        screen_id = 7        
        self.data['__screen__'] = self.screen_is_lower_than.apply_validation(self.data[column], self.data[reference_column])
        
        errors = self.data.loc[self.data['__screen__'] == True][[self.identifier, column]].copy()
        self.ensemble_error_event_detail(errors, column, screen_id)

#############################################################################

## SUBSYSTEM 05 FOR LOGGING ERROR EVENTS
class AbtractErrorEventLogsGenerator(ABC):

    @abstractmethod
    def export_error_events(self):
        pass

class ErrorEventLogsGenerator(AbtractErrorEventLogsGenerator):

    def __init__(self, error_events_detail: pd.DataFrame):
        self.error_events_detail = error_events_detail

    def export_error_events(self):

        if self.error_events_detail is not None:
            if self.error_events_detail.shape[0] > 0:
                
                error_events = self.error_events_detail[['error_event_id', 'batch_id']].drop_duplicates().reset_index(drop=True)
                error_events_detail = self.error_events_detail

                conn = duckdb.connect("insurance_case.db")
                conn.sql("INSERT INTO governance.fact_error_event SELECT * FROM error_events")
                conn.sql("INSERT INTO governance.fact_error_event_detail SELECT * FROM error_events_detail")
                conn.close()

#############################################################################

## SUBSYSTEM 06 FOR AUDIT DIMENSION ASSEMBLE
class AuditDimensionAssembler():

    def __init__(self, error_detail_table, table_name):
        self.error_detail_table = error_detail_table
        self.table_name = table_name
        self.etl_version = None

    def get_unsolved_rows(self):

        if self.error_detail_table is None:
            return []

        unsolved_rows = self.error_detail_table.loc[self.error_detail_table['error_condition'] == 'Sin Resolver']
        unsolved_rows = unsolved_rows.loc[unsolved_rows['table_name'] == self.table_name]
        unsolved_rows = unsolved_rows['record_identifier'].unique().tolist()

        return unsolved_rows

#############################################################################

## SUBSYSTEM 10 FOR SURROGATE KEY GENERATION
class AbstractSurrogateKeyGenerator(ABC):

    @abstractmethod
    def generate_surrogated_keys(self, n_rows: int):
        pass

class SurrogateKeyGenerator(AbstractSurrogateKeyGenerator):

    def generate_surrogated_keys(self, n_rows):
        return np.arange(1, n_rows + 1)

#############################################################################

## SUBSYSTEM 12 FOR SPECIAL DIMENSIONS MANAGEMENT
class AbstractSpecialDimensionManager:
    
    @abstractmethod
    def create_date_dimension(self):
        pass

    @abstractmethod
    def create_screen_dimension(self):
        pass

class SpecialDimensionManager(AbstractSpecialDimensionManager):

    def __init__(self):
        self.dim_date = None
        self.dim_screen = None

    def create_date_dimension(self):
        
        array_dates = pd.date_range(start='1900-01-01', end='1900-01-01')
        array_dates = array_dates.append(pd.date_range(start='1925-01-01', end='2029-12-31'))
        array_dates = array_dates.append(pd.date_range(start='2200-01-01', end='2200-01-01'))

        dim_date = pd.DataFrame({'date_id': array_dates, 'date': array_dates})
        dim_date['date_name'] = dim_date['date'].dt.strftime('%Y-%m-%d')

        dim_date['day'] = dim_date['date'].dt.day
        dim_date['month'] = dim_date['date'].dt.month
        dim_date['year'] = dim_date['date'].dt.year
        dim_date['day_of_week'] = dim_date['date'].dt.dayofweek + 1
        dim_date['day_name'] = dim_date['date'].dt.day_name()
        dim_date['month_name'] = dim_date['date'].dt.month_name()
        dim_date['quarter'] = dim_date['date'].dt.quarter
        dim_date['weekday_indicator'] = np.where(dim_date['day_of_week'].isin([6, 7]), 'Fin de Semana', 'Entre Semana')
        dim_date['week_of_year'] = dim_date['date'].dt.isocalendar().week
        dim_date['is_end_of_month'] = np.where(dim_date['date'] == dim_date['date'] + pd.offsets.MonthEnd(0), True, False)
        dim_date['is_end_of_quarter'] = np.where(dim_date['date'] == dim_date['date'] + pd.offsets.QuarterEnd(0), True, False)
        dim_date['is_end_of_year'] = np.where(dim_date['date'] == dim_date['date'] + pd.offsets.YearEnd(0), True, False)

        dim_date['date_id'] = dim_date['date_id'].dt.strftime('%Y%m%d').astype(int)
        dim_date.loc[dim_date['day_name'] == 'Monday', 'day_name'] = 'Lunes'
        dim_date.loc[dim_date['day_name'] == 'Tuesday', 'day_name'] = 'Martes'
        dim_date.loc[dim_date['day_name'] == 'Wednesday', 'day_name'] = 'Miércoles'
        dim_date.loc[dim_date['day_name'] == 'Thursday', 'day_name'] = 'Jueves'
        dim_date.loc[dim_date['day_name'] == 'Friday', 'day_name'] = 'Viernes'
        dim_date.loc[dim_date['day_name'] == 'Saturday', 'day_name'] = 'Sábado'
        dim_date.loc[dim_date['day_name'] == 'Sunday', 'day_name'] = 'Domingo'
        dim_date.loc[dim_date['month_name'] == 'January', 'month_name'] = 'Enero'
        dim_date.loc[dim_date['month_name'] == 'February', 'month_name'] = 'Febrero'
        dim_date.loc[dim_date['month_name'] == 'March', 'month_name'] = 'Marzo'
        dim_date.loc[dim_date['month_name'] == 'April', 'month_name'] = 'Abril'
        dim_date.loc[dim_date['month_name'] == 'May', 'month_name'] = 'Mayo'
        dim_date.loc[dim_date['month_name'] == 'June', 'month_name'] = 'Junio'
        dim_date.loc[dim_date['month_name'] == 'July', 'month_name'] = 'Julio'
        dim_date.loc[dim_date['month_name'] == 'August', 'month_name'] = 'Agosto'
        dim_date.loc[dim_date['month_name'] == 'September', 'month_name'] = 'Septiembre'
        dim_date.loc[dim_date['month_name'] == 'October', 'month_name'] = 'Octubre'
        dim_date.loc[dim_date['month_name'] == 'November', 'month_name'] = 'Noviembre'
        dim_date.loc[dim_date['month_name'] == 'December', 'month_name'] = 'Diciembre'

        dim_date['quarter_name'] = 'T' + dim_date['quarter'].astype(str)
        dim_date['year_month'] = dim_date['year'].astype(str) + '-' + dim_date['month'].astype(str).str.zfill(2)
        dim_date['quarter_year'] = dim_date['quarter_name'] + ' ' + dim_date['year'].astype(str)

        dim_date['holiday_indicator'] = 'No Festivo'
        dim_date.loc[dim_date['date'].isin(['2025-01-01', '2025-02-05', '2025-03-21', '2025-05-01',
                                            '2025-09-16', '2025-11-20', '2025-12-25']), 'holiday_indicator'] = 'Festivo'

        special_dates = pd.DataFrame({'date_id': [-1, -2],
                                      'date': [None, None],
                                      'date_name': ['No Aplica', 'No Disponible'],
                                      'day': [0, 0],
                                      'month': [0, 0],
                                      'year': [0, 0],
                                      'day_of_week': [0, 0],
                                      'day_name': ['No Aplica', 'No Disponible'],
                                      'month_name': ['No Aplica', 'No Disponible'],
                                      'quarter': [0, 0],
                                      'weekday_indicator': ['No Aplica', 'No Disponible'],
                                      'week_of_year': [0, 0],
                                      'is_end_of_month': [False, False],
                                      'is_end_of_quarter': [False, False],
                                      'is_end_of_year': [False, False],
                                      'quarter_name': ['No Aplica', 'No Disponible'],
                                      'year_month': ['0000-00', '0000-00'],
                                      'quarter_year': ['No Aplica', 'No Disponible'],
                                      'holiday_indicator': ['No Aplica', 'No Disponible']})

        self.dim_date = pd.concat([dim_date, special_dates], ignore_index=True)

    def create_screen_dimension(self):

        dim_screen = pd.DataFrame({
            'screen_id': [1, 2, 3, 4, 5, 6, 7],
            'screen_type': ['Columnar', 'Columnar', 'Estructural', 'Columnar', 'Columnar',
                             'Estructural', 'Negocio'],
            'screen_name': ['Valor nulo', 'Valor fuera de intervalo', 'Valor fuera de lista', 
                            'Cadena no compuesta por dígitos ', 'Fecha con formato incorrecto', 
                            'Valor no único', 'Valor menor que otro valor'],
            'screen_description': ['Valor nulo', 'Valor fuera de intervalo', 'Valor fuera de lista', 
                            'Cadena no compuesta por dígitos ', 'Fecha con formato incorrecto', 
                            'Valor no único', 'Valor menor que otro valor'],
            'etl_module': ['v0.0.1'] * 7
        })

        self.dim_screen = dim_screen 

#############################################################################

## SUBSYSTEM 17 FOR DIMENSION MANAGEMENT
class AbtractDimensionBuilder(ABC):

    def __init__(self):
        self.silver_dimension = None
        self.surrogate_key_generator = SurrogateKeyGenerator()

    @abstractmethod
    def build_dimension(self):
        pass

    @abstractmethod
    def export_dimension(self):
        pass

class CertificatesDimensionBuilder(AbtractDimensionBuilder):
    
    def build_dimension(self):

        conn = duckdb.connect("insurance_case.db")
        silver_customer_birth_date = conn.sql("SELECT * FROM silver.dim_birth_date").df()
        bronze_dimension = conn.sql("SELECT * FROM bronze.dim_certificate").df()
        conn.close()

        silver_dimension = bronze_dimension.copy()
        silver_dimension['name'] = silver_dimension['name'].apply(lambda x: x[0:3] + '****')
        silver_dimension['email'] = silver_dimension['email'].apply(lambda x: '****' + x[x.find('@'):])

        silver_dimension = silver_dimension.merge(silver_customer_birth_date[['birth_date_id', 'date']],
                                                  left_on='birth_date',
                                                  right_on='date',
                                                  how='inner')

        if not silver_dimension.shape[0] == bronze_dimension.shape[0]:
            raise Exception('Data loss detected during the dimension merge process')

        keys = self.surrogate_key_generator.generate_surrogated_keys(silver_dimension.shape[0])
        silver_dimension['surrogated_id'] = keys

        columns_order = ['surrogated_id', 'certificate_id', 'name', 'email', 'age', 'city',
                        'gender', 'certificate_number', 'birth_date_id']
        self.silver_dimension = silver_dimension[columns_order]

    def export_dimension(self):

        silver_dimension = self.silver_dimension
        
        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE silver.dim_certificate AS SELECT * FROM silver_dimension")
        conn.close()

class CieDimensionBuilder(AbtractDimensionBuilder):
    
    def build_dimension(self):

        conn = duckdb.connect("insurance_case.db")
        bronze_dimension = conn.sql("SELECT * FROM bronze.dim_cie").df()
        conn.close()

        silver_dimension = bronze_dimension.copy()

        keys = self.surrogate_key_generator.generate_surrogated_keys(silver_dimension.shape[0])
        silver_dimension['surrogated_id'] = keys

        if not silver_dimension.shape[0] == bronze_dimension.shape[0]:
            raise Exception('Data loss detected during the dimension merge process')

        columns_order = ['surrogated_id', 'cie_id', 'cie_name']
        self.silver_dimension = silver_dimension[columns_order]

    def export_dimension(self):

        silver_dimension = self.silver_dimension
        
        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE silver.dim_cie AS SELECT * FROM silver_dimension")
        conn.close()

class AbstractDimensionManager(ABC):
    
    @abstractmethod
    def init_date_dimension(self):
        pass

    @abstractmethod
    def init_screen_dimension(self):
        pass

    @abstractmethod
    def build_dimension(self, dimension_builder: AbtractDimensionBuilder):
        pass

class DimensionManager(AbstractDimensionManager):

    def __init__(self):
        self.special_dimension_manager = SpecialDimensionManager()

    def init_date_dimension(self):
        
        self.special_dimension_manager.create_date_dimension()
        dim_date = self.special_dimension_manager.dim_date

        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE silver.dim_date AS SELECT * FROM dim_date")
        conn.close()

    def init_date_view_dimensions(self):

        conn = duckdb.connect("insurance_case.db")

        conn.sql("""CREATE OR REPLACE VIEW silver.dim_birth_date AS
            SELECT 
                date_id AS birth_date_id,
                date,
                date_name,
                day,
                month,
                year,
                year_month,
                quarter_year
            FROM silver.dim_date
            WHERE date_id NOT IN (-1, -2)""")

        conn.sql("""CREATE OR REPLACE VIEW silver.dim_term_begin_date AS
            SELECT
                date_id AS term_begin_date_id,
                date, date_name,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date
            WHERE date_id NOT IN (-1, -2)""")

        conn.sql("""CREATE OR REPLACE VIEW silver.dim_term_end_date AS
            SELECT
                date_id AS term_end_date_id,
                date, date_name,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date
            WHERE date_id NOT IN (-1, -2)""")

        conn.sql("""CREATE OR REPLACE VIEW silver.dim_consultation_date AS
            SELECT
                date_id AS consultation_date_id,
                date, date_name,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date
            WHERE date_id NOT IN (-1, -2)""")

        conn.sql("""CREATE OR REPLACE VIEW silver.dim_incident_date AS
            SELECT
                date_id AS incident_date_id,
                date, date_name,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date""")

        conn.sql("""CREATE OR REPLACE VIEW silver.dim_payment_date AS
            SELECT
                date_id AS payment_date_id,
                date, date_name,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date""")

        conn.sql("""CREATE OR REPLACE VIEW silver.dim_first_expense_date AS
            SELECT
                date_id AS first_expense_date_id,
                date, date_name,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date""")

        conn.sql("""CREATE OR REPLACE VIEW silver.dim_month_cont_date AS
            SELECT
                date_id AS month_cont_date_id,
                date, date_name,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date""")

        conn.close()

    def init_screen_dimension(self):
        
        self.special_dimension_manager.create_screen_dimension()
        dim_screen = self.special_dimension_manager.dim_screen

        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE governance.dim_screen AS SELECT * FROM dim_screen")
        conn.close()

    def build_dimension(self, dimension_builder: AbtractDimensionBuilder):
        
        dimension_builder.build_dimension()
        dimension_builder.export_dimension()

#############################################################################

## SUBSYSTEM 18 FOR FACT MANAGEMENT
class AbtractFactBuilder(ABC):

    def __init__(self):
        self.silver_fact = None

    @abstractmethod
    def build_fact(self):
        pass

    @abstractmethod
    def export_fact(self):
        pass

class TermFactBuilder(AbtractFactBuilder):
    
    def build_fact(self):

        conn = duckdb.connect("insurance_case.db")
        silver_term_begin_date = conn.sql("SELECT * FROM silver.dim_term_begin_date").df()
        silver_term_end_date = conn.sql("SELECT * FROM silver.dim_term_end_date").df()
        silver_certificate_id = conn.sql("SELECT surrogated_id AS surrogated_certificate_id, certificate_id FROM silver.dim_certificate").df()
        bronze_fact = conn.sql("SELECT * FROM bronze.fact_term").df()
        conn.close()

        silver_fact = bronze_fact.copy()
        silver_fact = silver_fact.loc[silver_fact['audit_passed'] == 'Sí']

        shape_01 = silver_fact.shape[0]

        silver_fact = silver_fact.merge(silver_term_begin_date[['term_begin_date_id', 'date']],
                                        left_on='term_begin_date',
                                        right_on='date',
                                        how='inner')

        silver_fact = silver_fact.drop(columns=['term_begin_date', 'date'])

        silver_fact = silver_fact.merge(silver_term_end_date[['term_end_date_id', 'date']],
                                        left_on='term_end_date',
                                        right_on='date',
                                        how='inner')

        silver_fact = silver_fact.drop(columns=['term_end_date', 'date'])

        silver_fact = silver_fact.merge(silver_certificate_id, on='certificate_id', how='inner')

        silver_fact = silver_fact.drop(columns=['certificate_id'])
        shape_02 = silver_fact.shape[0]

        if not shape_01 == shape_02:
            raise Exception('Data loss detected during the fact merge process')

        columns_order = ['term_id', 'surrogated_certificate_id', 'term_begin_date_id', 'term_end_date_id', 'audit_passed']
        self.silver_fact = silver_fact[columns_order]

    def export_fact(self):

        silver_fact = self.silver_fact
        
        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE silver.fact_term AS SELECT * FROM silver_fact")
        conn.close()

class ConsultationFactBuilder(AbtractFactBuilder):
    
    def build_fact(self):

        conn = duckdb.connect("insurance_case.db")
        silver_consultation_date = conn.sql("SELECT * FROM silver.dim_consultation_date").df()
        silver_certificate_id = conn.sql("SELECT surrogated_id AS surrogated_certificate_id, certificate_id FROM silver.dim_certificate").df()
        bronze_fact = conn.sql("SELECT * FROM bronze.fact_consultation").df()
        conn.close()

        silver_fact = bronze_fact.copy()
        silver_fact = silver_fact.loc[silver_fact['audit_passed'] == 'Sí']

        shape_01 = silver_fact.shape[0]

        silver_fact = silver_fact.merge(silver_consultation_date[['consultation_date_id', 'date']],
                                        left_on='consultation_date',
                                        right_on='date',
                                        how='inner')

        silver_fact = silver_fact.drop(columns=['consultation_date', 'date'])

        silver_fact = silver_fact.merge(silver_certificate_id, on='certificate_id', how='inner')

        silver_fact = silver_fact.drop(columns=['certificate_id'])
        shape_02 = silver_fact.shape[0]

        if not shape_01 == shape_02:
            raise Exception('Data loss detected during the fact merge process')

        columns_order = ['consultation_id', 'surrogated_certificate_id', 'consultation_date_id', 'specialty',
                    	 'placed_by', 'consultation_observation', 'next_consultation_pending', 'prescription_or_medical_order',
                         'patient_goal', 'specialist_goal', 'pause_consultations', 'audit_passed']
        self.silver_fact = silver_fact[columns_order]

    def export_fact(self):

        silver_fact = self.silver_fact
        
        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE silver.fact_consultation AS SELECT * FROM silver_fact")
        conn.close()

class ConsultationDiagnosesBridgeBuilder(AbtractFactBuilder):
    
    def build_fact(self):

        conn = duckdb.connect("insurance_case.db")
        silver_cie_id = conn.sql("SELECT surrogated_id AS surrogated_cie_id, cie_id FROM silver.dim_cie").df()
        bronze_fact = conn.sql("SELECT * FROM bronze.bridge_consultation_diagnosis").df()
        conn.close()

        silver_fact = bronze_fact.copy()
        silver_fact = silver_fact.loc[silver_fact['audit_passed'] == 'Sí']

        shape_01 = silver_fact.shape[0]

        silver_fact = silver_fact.merge(silver_cie_id, on='cie_id', how='inner')
        silver_fact = silver_fact.drop(columns=['cie_id'])

        shape_02 = silver_fact.shape[0]

        if not shape_01 == shape_02:
            raise Exception('Data loss detected during the fact merge process')

        columns_order = ['consultation_id', 'surrogated_cie_id']
        self.silver_fact = silver_fact[columns_order]

    def export_fact(self):

        silver_fact = self.silver_fact
        
        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE silver.bridge_consultation_diagnosis AS SELECT * FROM silver_fact")
        conn.close()


class ClaimFactBuilder(AbtractFactBuilder):
    
    def build_fact(self):

        conn = duckdb.connect("insurance_case.db")
        silver_incident_date = conn.sql("SELECT incident_date_id FROM silver.dim_incident_date").df()
        silver_payment_date = conn.sql("SELECT payment_date_id FROM silver.dim_payment_date").df()
        silver_first_expense_date = conn.sql("SELECT first_expense_date_id FROM silver.dim_first_expense_date").df()
        silver_month_cont_date = conn.sql("SELECT month_cont_date_id FROM silver.dim_month_cont_date").df()
        silver_certificate_id = conn.sql("SELECT surrogated_id AS surrogated_certificate_id, certificate_number FROM silver.dim_certificate").df()
        silver_cie_id = conn.sql("SELECT surrogated_id AS surrogated_cie_id, cie_id FROM silver.dim_cie").df()
        bronze_fact = conn.sql("SELECT * FROM bronze.fact_claim").df()
        conn.close()

        silver_fact = bronze_fact.copy()
        silver_fact = silver_fact.loc[silver_fact['audit_passed'] == 'Sí']

        shape_01 = silver_fact.shape[0]

        silver_fact = silver_fact.merge(silver_incident_date, on='incident_date_id', how='inner')
        silver_fact = silver_fact.drop(columns=['incident_date'])

        silver_fact = silver_fact.merge(silver_payment_date, on='payment_date_id', how='inner')
        silver_fact = silver_fact.drop(columns=['payment_date'])

        silver_fact = silver_fact.merge(silver_first_expense_date, on='first_expense_date_id', how='inner')
        silver_fact = silver_fact.drop(columns=['first_expense_date'])

        silver_fact = silver_fact.merge(silver_month_cont_date, on='month_cont_date_id', how='inner')
        silver_fact = silver_fact.drop(columns=['month_cont_date'])

        silver_fact = silver_fact.merge(silver_certificate_id, on='certificate_number', how='inner')
        silver_fact = silver_fact.drop(columns=['certificate_number'])

        silver_fact = silver_fact.merge(silver_cie_id, on='cie_id', how='inner')
        silver_fact = silver_fact.drop(columns=['cie_id'])

        shape_02 = silver_fact.shape[0]

        if not shape_01 == shape_02:
            raise Exception('Data loss detected during the fact merge process')

        columns_order = ['claim_id', 'state', 'surrogated_cie_id', 'incident_date_id', 
                         'payment_date_id', 'first_expense_date_id', 'ocurrido',
                         'payments', 'coinsurance', 'ivarec', 'deductible',
                         'incident_reason', 'month_cont_date_id', 'payment_type',
                         'provider', 'surrogated_certificate_id', 'audit_passed']
        self.silver_fact = silver_fact[columns_order]

    def export_fact(self):

        silver_fact = self.silver_fact
        
        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE silver.fact_claim AS SELECT * FROM silver_fact")
        conn.close()



class AbstractFactManager(ABC):
    
    @abstractmethod
    def build_fact(self, fact_builder: AbtractFactBuilder):
        pass

class FactManager(AbstractFactManager):

    def build_fact(self, fact_builder: AbtractFactBuilder):
        
        fact_builder.build_fact()
        fact_builder.export_fact()

#############################################################################



@dg.asset(name='init_datawarehouse_resourses', group_name='governance')
def init_datawarehouse_resources():

    datawarehouse_resources_creator = DatawarehouseResourcesCreator()
    datawarehouse_resources_creator.init_schemas()
    datawarehouse_resources_creator.init_error_tables()

    logging.info("Datawarehouse resources initialized")

    return


@dg.asset(name='generate_dim_date', group_name='silver', deps=['init_datawarehouse_resourses'])
def generate_dim_date():

    dimension_manager = DimensionManager()

    dimension_manager.init_date_dimension()
    logging.info("Date dimension generated and stored in silver.dim_date table")

    dimension_manager.init_date_view_dimensions()
    logging.info("Date view dimensions generated in silver schema")

    return

@dg.asset(name='generate_dim_screen', group_name='governance', deps=['init_datawarehouse_resourses'])
def generate_dim_screen():

    dimension_manager = DimensionManager()
    dimension_manager.init_screen_dimension()

    logging.info("Screen dimension generated and stored in governance.dim_screen table")

    return

## CERTIFICATES
@dg.asset(name='extract_certificates', group_name='bronze', 
          deps=['generate_dim_date', 'generate_dim_screen'])
def extract_certificates():

    path = '../datalake/raw/certificate_dummy.csv'
    extractor = CertificatesCsvExtractor()
    extractor.extract_data(path)
    extractor.screen_data()

    error_events_generator = ErrorEventLogsGenerator(error_events_detail=extractor.errors)
    error_events_generator.export_error_events()

    extractor.export_data()

    return

@dg.asset(name='load_dim_certificate', group_name='silver', 
          deps=['extract_certificates'])
def load_dim_certificate():

    certificate_dimension_builder = CertificatesDimensionBuilder()

    dimension_manager = DimensionManager()
    dimension_manager.build_dimension(certificate_dimension_builder)

    return

## CIE
@dg.asset(name='extract_cie', group_name='bronze', 
          deps=['generate_dim_date', 'generate_dim_screen'])
def extract_cie():

    path = '../datalake/raw/cat_cie_10.csv'
    extractor = CieCsvExtractor()
    extractor.extract_data(path)
    extractor.screen_data()

    error_events_generator = ErrorEventLogsGenerator(error_events_detail=extractor.errors)
    error_events_generator.export_error_events()

    extractor.export_data()

    return

@dg.asset(name='load_dim_cie', group_name='silver', 
          deps=['extract_cie'])
def load_dim_cie():

    cie_dimension_builder = CieDimensionBuilder()

    dimension_manager = DimensionManager()
    dimension_manager.build_dimension(cie_dimension_builder)

    return


## TERMS
@dg.asset(name='extract_terms', group_name='bronze', 
          deps=['load_dim_cerfificate'])
def extract_terms():

    path = '../datalake/raw/terms_dummy.csv'
    extractor = TermsCsvExtractor()
    extractor.extract_data(path)
    extractor.screen_data()

    error_events_generator = ErrorEventLogsGenerator(error_events_detail=extractor.errors)
    error_events_generator.export_error_events()

    extractor.export_data()

    return

@dg.asset(name='load_fact_term', group_name='silver', 
          deps=['extract_terms'])
def load_fact_term():

    term_fact_builder = TermFactBuilder()

    fact_manager = FactManager()
    fact_manager.build_fact(term_fact_builder)

    return


## CONSULTATIONS
@dg.asset(name='extract_consultations', group_name='bronze', 
          deps=['load_dim_cerfificate', 'generate_dim_date'])
def extract_consultations():

    path = '../datalake/preprocessed/consultations_dummy.csv'
    extractor = ConsultationsCsvExtractor()
    extractor.extract_data(path)
    extractor.screen_data()

    error_events_generator = ErrorEventLogsGenerator(error_events_detail=extractor.errors)
    error_events_generator.export_error_events()

    extractor.export_data()

    return

@dg.asset(name='load_fact_consultation', group_name='silver', 
          deps=['extract_consultations'])
def load_fact_consultation():

    consultation_fact_builder = ConsultationFactBuilder()

    fact_manager = FactManager()
    fact_manager.build_fact(consultation_fact_builder)

    return

## CONSULTATION DIAGNOSES
@dg.asset(name='extract_consultation_diagnoses', group_name='bronze', 
          deps=['load_fact_consultation', 'load_dim_cie'])
def extract_consultation_diagnoses():

    path = '../datalake/preprocessed/consultation_diagnoses_dummy.csv'
    extractor = ConsultationDiagnosesCsvExtractor()
    extractor.extract_data(path)
    extractor.screen_data()

    error_events_generator = ErrorEventLogsGenerator(error_events_detail=extractor.errors)
    error_events_generator.export_error_events()

    extractor.export_data()

    return

@dg.asset(name='load_bridge_consultation_diagnosis', group_name='silver', 
          deps=['extract_consultation_diagnoses'])
def load_bridge_consultation_diagnosis():

    builder = ConsultationDiagnosesBridgeBuilder()

    fact_manager = FactManager()
    fact_manager.build_fact(builder)

    return

## CLAIMS
@dg.asset(name='extract_claims', group_name='bronze', 
          deps=['load_dim_cerfificate', 'generate_dim_date'])
def extract_claims():

    path = '../datalake/raw/claims_dummy.csv'
    extractor = ClaimsCsvExtractor()
    extractor.extract_data(path)
    extractor.screen_data()

    error_events_generator = ErrorEventLogsGenerator(error_events_detail=extractor.errors)
    error_events_generator.export_error_events()

    extractor.export_data()

    return

@dg.asset(name='load_fact_claim', group_name='silver', 
          deps=['extract_claims'])
def load_claim_consultation():

    claim_fact_builder = ClaimFactBuilder()

    fact_manager = FactManager()
    fact_manager.build_fact(claim_fact_builder)

    return
