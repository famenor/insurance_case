import pandas as pd
import numpy as np
import datetime
from abc import ABC, abstractmethod

from dagster_quickstart.lib.error_event import *
from dagster_quickstart.lib.cleansing import *
from dagster_quickstart.lib.audit import *

## SUBSYSTEM 03 FOR DATA EXTRACTION

class DatawarehouseResourcesCreator(ABC):
    
    def __init__(self):

        self.schema_governance_definition = 'CREATE SCHEMA IF NOT EXISTS governance'
        self.schema_bronze_definition = 'CREATE SCHEMA IF NOT EXISTS bronze'
        self.schema_silver_definition = 'CREATE SCHEMA IF NOT EXISTS silver'
        self.schema_gold_definition = 'CREATE SCHEMA IF NOT EXISTS gold'
        
        self.table_fact_error_event_definition = """
            CREATE OR REPLACE TABLE governance.fact_error_event (
                error_event_id INTEGER,
                batch_id INTEGER
            )
        """

        self.table_fact_error_event_detail_definition = """
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
        """

#SOURCE EXTRACTOR DEFINITION
class AbstractSourceExtractorTemplate(ABC):

    def __init__(self, context=None):
        self.data = None
        self.facade_screens = FacadeValidationScreens()
        self.errors = None
        self.context = context

    @abstractmethod
    def process(self):
        pass

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def screen_data(self):
        pass

    def generate_error_events(self):

        error_events_generator = ErrorEventLogsGenerator(error_events_detail=self.errors)
        error_events_generator.generate_error_inputs()

        self.error_events = error_events_generator.error_events
        self.error_events_detail = error_events_generator.error_events_detail

    @abstractmethod
    def generate_export_table(self):
        pass

class AbstractFileExtractor(AbstractSourceExtractorTemplate):

    def process(self):
        self.extract_data()
        self.screen_data()
        self.generate_error_events()
        self.generate_export_table()

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def screen_data(self):
        pass

    @abstractmethod
    def generate_export_table(self):
        pass

class AbstractDatabaseExtractor(AbstractSourceExtractorTemplate):

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def screen_data(self):
        pass

    @abstractmethod
    def generate_export_table(self):
        pass

class GenericCsvExtractor(AbstractFileExtractor):

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def screen_data(self):
        pass

    @abstractmethod
    def generate_export_table(self):
        pass
    
class CertificatesCsvExtractor(GenericCsvExtractor):

    def extract_data(self):
        print('Reading file: ', self.context['file_path'])
        self.data = pd.read_csv(self.context['file_path'])
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

    def generate_export_table(self):
        pass
            

class TermsCsvExtractor(GenericCsvExtractor):

    def extract_data(self):
        print('Reading file: ', self.context['file_path'])
        self.data = pd.read_csv(self.context['file_path'])
        self.data = self.data.rename(columns={'id': 'term_id', 
                                              'certificate_number': 'certificate_id',
                                              'fecha_inicio_vigencia': 'term_begin_date',
                                              'fecha_fin_periodo': 'term_end_date'})

    def screen_data(self):
        
        print('Screening data...')
        certificate_ids = self.context['list_values']['certificate_ids']

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
        self.facade_screens.apply_screen_is_out_of_list_value('certificate_id', certificate_ids)

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])

        #ADD AUDIT FACT COLUMN
        self.data['audit_passed'] = 'Sí'
        audit_dim_assembler = AuditDimensionAssembler(self.errors, 'term_dummy')
        unsolved_rows = audit_dim_assembler.get_unsolved_rows()
        self.data.loc[self.data['term_id'].isin(unsolved_rows), 'audit_passed'] = 'No'

    def generate_export_table(self):
        pass 


class CieCsvExtractor(GenericCsvExtractor):

    def extract_data(self):
        print('Reading file: ', self.context['file_path'])
        self.data = pd.read_csv(self.context['file_path'])
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

    def generate_export_table(self):
        pass

class ConsultationsCsvExtractor(GenericCsvExtractor):

    def extract_data(self):
        print('Reading file: ', self.context['file_path'])
        self.data = pd.read_csv(self.context['file_path'])
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
        certificate_ids = self.context['list_values']['certificate_ids']

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
        self.facade_screens.apply_screen_is_out_of_list_value('certificate_id', certificate_ids)
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

    def generate_export_table(self):
        pass


class ConsultationDiagnosesCsvExtractor(GenericCsvExtractor):

    def extract_data(self):

        path_pathologies = '../datalake/raw/pathologies.csv'
        pathologies = pd.read_csv(path_pathologies, usecols=['code', 'id'])
        pathologies = pathologies.rename(columns={'code': 'cie_id', 'id': 'pathology_id'})

        self.data = pd.read_csv(self.context['file_path'])
        self.data = self.data.rename(columns={'consultation_id': 'consultation_id',
                                              'diagnosis': 'pathology_id'}) 
        self.data['consultation_diagnosis_id'] = np.arange(1, self.data.shape[0] + 1)

        self.data = self.data.merge(pathologies, how='inner', on='pathology_id')
        self.data = self.data.drop(columns=['pathology_id'])
    

    def screen_data(self):

        consultation_identifiers = self.context['list_values']['consultation_ids']
        cie_identifiers = self.context['list_values']['cie_ids']
        
        self.facade_screens.setup(data=self.data, table_name='consultation_diagnoses', identifier='consultation_diagnosis_id')
        
        #CHECK NULL VALUES
        columns = ['consultation_id', 'cie_id']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('consultation_diagnosis_id')

        #CHECK OUT OF LIST VALUES
        self.facade_screens.apply_screen_is_out_of_list_value('consultation_id', consultation_identifiers)
        self.facade_screens.apply_screen_is_out_of_list_value('cie_id', cie_identifiers)

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])

        #ADD AUDIT FACT COLUMN
        self.data['audit_passed'] = 'Sí'
        audit_dim_assembler = AuditDimensionAssembler(self.errors, 'consultation_diagnoses')
        unsolved_rows = audit_dim_assembler.get_unsolved_rows()
        self.data.loc[self.data['consultation_diagnosis_id'].isin(unsolved_rows), 'audit_passed'] = 'No'

    def generate_export_table(self):
        pass 


class ClaimsCsvExtractor(GenericCsvExtractor):

    def extract_data(self):
        print('Reading file: ', self.context['file_path'])
        self.data = pd.read_csv(self.context['file_path'])
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

        certificate_numbers = self.context['list_values']['certificate_numbers']
        cie_identifiers = self.context['list_values']['cie_identifiers']
        
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
        self.facade_screens.apply_screen_is_out_of_list_value('certificate_number', certificate_numbers)
        self.facade_screens.apply_screen_is_out_of_list_value('incident_reason', ['Enfermedad', 'Accidente'])
        self.facade_screens.apply_screen_is_out_of_list_value('payment_type', ['Pago Directo'])
        self.facade_screens.apply_screen_is_out_of_list_value('cie_id', cie_identifiers)

        self.errors = self.facade_screens.get_error_events_detail()
        self.data = self.data.drop(columns=['__screen__'])

        #ADD AUDIT FACT COLUMN
        self.data['audit_passed'] = 'Sí'
        audit_dim_assembler = AuditDimensionAssembler(self.errors, 'claims')
        unsolved_rows = audit_dim_assembler.get_unsolved_rows()
        self.data.loc[self.data['claim_id'].isin(unsolved_rows), 'audit_passed'] = 'No'

    def generate_export_table(self):
        pass