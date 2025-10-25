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
                record_identifier INTEGER,
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
            conn.sql("CREATE OR REPLACE TABLE bronze.dim_certificates AS SELECT * FROM data")

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

        try:
            datetime.datetime.strptime(value, date_format)
            result = False
        
        except ValueError:
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
    
    
#class ValidationScreenHasIncongruentInverses(AbstractValidationScreen):

#    def apply_validation(self, array_a, array_b):

 #       results = pd.Series(zip(array_a, array_b)).apply(lambda x: self.validator.validate(value_a=x[0], value_b=x[1]))
 #       return results

class FacadeValidationScreens():

    def __init__(self):
        self.event_errors = []
        self.screen_is_not_unique = ValidationScreenIsNotUnique()
        self.screen_is_missing_value = ValidationScreenIsMisssingValue()
        self.screen_is_out_of_bounds_value = ValidationScreenIsOutOfBoundsValue()
        self.screen_is_out_of_list_value = ValidationScreenIsOutOfListValue()
        self.screen_is_not_digit_string = ValidationScreenIsNotDigitString()
        self.screen_is_not_date_format = ValidationScreenIsNotDateFormat()

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
            'screen_id': [1, 2, 3, 4, 5, 6],
            'screen_name': ['Valor nulo', 'Valor fuera de intervalo', 'Valor fuera de lista', 
                            'Cadena no compuesta por dígitos ', 'Fecha con formato incorrecto', 
                            'Valor no único'],
            'screen_description': ['Valor nulo', 'Valor fuera de intervalo', 'Valor fuera de lista', 
                            'Cadena no compuesta por dígitos ', 'Fecha con formato incorrecto', 
                            'Valor no único'],
            'etl_module': ['v0.0.1'] * 6
        })

        self.dim_screen = dim_screen 

#############################################################################

## SUBSYSTEM 17 FOR DIMENSION MANAGEMENT
class AbstractDimensionManager(ABC):
    
    @abstractmethod
    def init_date_dimension(self):
        pass

    @abstractmethod
    def init_screen_dimension(self):
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

    def init_screen_dimension(self):
        
        self.special_dimension_manager.create_screen_dimension()
        dim_screen = self.special_dimension_manager.dim_screen

        conn = duckdb.connect("insurance_case.db")
        conn.sql("CREATE OR REPLACE TABLE governance.dim_screen AS SELECT * FROM dim_screen")
        conn.close()

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

    return

@dg.asset(name='generate_dim_screen', group_name='governance', deps=['init_datawarehouse_resourses'])
def generate_dim_screen():

    dimension_manager = DimensionManager()
    dimension_manager.init_screen_dimension()

    logging.info("Screen dimension generated and stored in governance.dim_screen table")

    return

@dg.asset(name='extract_certificates', group_name='bronze', 
          deps=['generate_dim_date', 'generate_dim_screen'])
def extract_certificates():

    path = '/home/armando/git/insurance_case/datalake/raw/certificate_dummy.csv'
    extractor = CertificatesCsvExtractor()
    extractor.extract_data(path)
    extractor.screen_data()

    error_events_generator = ErrorEventLogsGenerator(error_events_detail=extractor.errors)
    error_events_generator.export_error_events()

    extractor.export_data()

    return