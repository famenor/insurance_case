import pandas as pd
import numpy as np
import datetime
from abc import ABC, abstractmethod

from dagster_quickstart.lib.special_dimensions import *
from dagster_quickstart.lib.surrogate_key_generation import *

## SUBSYSTEM 17 FOR DIMENSION MANAGEMENT
class AbtractDimensionBuilder(ABC):

    def __init__(self):
        self.silver_dimension = None
        self.surrogate_key_generator = SurrogateKeyGenerator()

    @abstractmethod
    def build_dimension(self, inputs: dict):
        pass


class CertificatesDimensionBuilder(AbtractDimensionBuilder):
    
    def build_dimension(self, inputs: dict):

        silver_customer_birth_date = inputs['dim_birth_date']
        bronze_dimension = inputs['bronze_dimension']

        silver_dimension = bronze_dimension.copy()
        silver_dimension['name'] = silver_dimension['name'].apply(lambda x: x[0:3] + '****')
        silver_dimension['email'] = silver_dimension['email'].apply(lambda x: '****' + x[x.find('@'):])

        silver_dimension = silver_dimension.merge(silver_customer_birth_date,
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
        

class CieDimensionBuilder(AbtractDimensionBuilder):
    
    def build_dimension(self, inputs: dict):

        bronze_dimension = inputs['bronze_dimension']

        silver_dimension = bronze_dimension.copy()

        keys = self.surrogate_key_generator.generate_surrogated_keys(silver_dimension.shape[0])
        silver_dimension['surrogated_id'] = keys

        if not silver_dimension.shape[0] == bronze_dimension.shape[0]:
            raise Exception('Data loss detected during the dimension merge process')

        columns_order = ['surrogated_id', 'cie_id', 'cie_name']
        self.silver_dimension = silver_dimension[columns_order]


class InterfaceDimensionManager(ABC):
    
    @abstractmethod
    def init_date_dimension(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def create_date_view_definitions(self):
        pass

    @abstractmethod
    def init_screen_dimension(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def build_dimension(self, dimension_builder: AbtractDimensionBuilder, inputs: dict):
        pass

class DimensionManager(InterfaceDimensionManager):

    def __init__(self):
        self.special_dimension_manager = SpecialDimensionManager()

    def init_date_dimension(self) -> pd.DataFrame:
        
        self.special_dimension_manager.create_date_dimension()
        dim_date = self.special_dimension_manager.dim_date

        return dim_date

    def create_date_view_definitions(self):

        self.dim_birth_date_definition = """
            CREATE OR REPLACE VIEW silver.dim_birth_date AS
            SELECT 
                date_id AS birth_date_id,
                date,
                date_type,
                day,
                month,
                year,
                year_month,
                quarter_year
            FROM silver.dim_date
            WHERE date_id NOT IN (-1, -2, -3, -4)"""
        
        self.dim_term_begin_date_definition = """
            CREATE OR REPLACE VIEW silver.dim_term_begin_date AS
            SELECT
                date_id AS term_begin_date_id,
                date, date_type,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date
            WHERE date_id NOT IN (-1, -2, -3, -4)"""
        
        self.dim_term_end_date_definition = """
            CREATE OR REPLACE VIEW silver.dim_term_end_date AS
            SELECT
                date_id AS term_end_date_id,
                date, date_type,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date
            WHERE date_id NOT IN (-1, -2, -3, -4)"""
        
        self.dim_consultation_date_definition = """
            CREATE OR REPLACE VIEW silver.dim_consultation_date AS
            SELECT
                date_id AS consultation_date_id,
                date, date_type,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date
            WHERE date_id NOT IN (-1, -2, -3, -4)"""
        
        self.dim_incident_date_definition = """
            CREATE OR REPLACE VIEW silver.dim_incident_date AS
            SELECT
                date_id AS incident_date_id,
                date, date_type,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date"""
        
        self.dim_payment_date_definition = """
            CREATE OR REPLACE VIEW silver.dim_payment_date AS
            SELECT
                date_id AS payment_date_id,
                date, date_type,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date"""
        
        self.dim_first_expense_date_definition = """
            CREATE OR REPLACE VIEW silver.dim_first_expense_date AS
            SELECT
                date_id AS first_expense_date_id,
                date, date_type,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date"""
        
        self.dim_month_cont_date_definition = """
            CREATE OR REPLACE VIEW silver.dim_month_cont_date AS
            SELECT
                date_id AS month_cont_date_id,
                date, date_type,
                day, month, year, day_of_week,
                day_name, month_name, quarter,
                weekday_indicator, week_of_year,
                is_end_of_month, is_end_of_quarter,
                is_end_of_year, quarter_name,
                year_month, quarter_year,
                holiday_indicator
            FROM silver.dim_date"""

    def init_screen_dimension(self):
        
        self.special_dimension_manager.create_screen_dimension()
        dim_screen = self.special_dimension_manager.dim_screen

        return dim_screen
    
    def build_dimension(self, dimension_builder: AbtractDimensionBuilder, inputs: dict):

        dimension_builder.build_dimension(inputs)
        self.silver_dimension = dimension_builder.silver_dimension

    
