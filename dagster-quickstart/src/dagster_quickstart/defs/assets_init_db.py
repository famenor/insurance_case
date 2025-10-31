import pandas as pd
import dagster as dg
import logging

from dagster_quickstart.lib.extraction import *
from dagster_quickstart.lib.dimension_management import *

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


# INIT DB

@dg.op(required_resource_keys={"insurance_db"},
       name='init_datawarehouse_resourses')
def init_datawarehouse_resources(context):

    insurance_db = context.resources.insurance_db
    datawarehouse_resources_creator = DatawarehouseResourcesCreator()

    with insurance_db.get_connection() as conn:
        conn.execute(datawarehouse_resources_creator.schema_governance_definition)
        conn.execute(datawarehouse_resources_creator.schema_bronze_definition)
        conn.execute(datawarehouse_resources_creator.schema_silver_definition)
        conn.execute(datawarehouse_resources_creator.schema_gold_definition)
        conn.execute(datawarehouse_resources_creator.table_fact_error_event_definition)
        conn.execute(datawarehouse_resources_creator.table_fact_error_event_detail_definition)

    logging.info("Datawarehouse resources initialized")

    return


# SILVER - DIMENSION DATE

@dg.asset(name='silver_dim_date', group_name='silver', 
          deps=['init_datawarehouse_resourses'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Dimensión fecha en nivel plata"),
            'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_date(context):

    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    result = dimension_manager.init_date_dimension()

    with insurance_db.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE silver.dim_date AS SELECT * FROM result")

    logging.info("Successful execution")
    return


# SILVER - DIMENSION BIRTH DATE

@dg.asset(name='silver_dim_birth_date', group_name='silver', 
          deps=['silver_dim_date'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Dimensión fecha de nacimiento en nivel plata"),
            'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_birth_date(context):

    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    dimension_manager.create_date_view_definitions()
    result = dimension_manager.dim_birth_date_definition

    with insurance_db.get_connection() as conn:
        conn.execute(result)

    logging.info("Successful execution")
    return


# SILVER - DIMENSION TERM BEGIN DATE

@dg.asset(name='silver_dim_term_begin_date', group_name='silver',
          deps=['silver_dim_date'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Dimensión fecha de inicio de término en nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_term_begin_date(context):
        
    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    dimension_manager.create_date_view_definitions()
    result = dimension_manager.dim_term_begin_date_definition

    with insurance_db.get_connection() as conn:
        conn.execute(result)
    
    logging.info("Successful execution")
    return


# SILVER - DIMENSION TERM END DATE

@dg.asset(name='silver_dim_term_end_date', group_name='silver',
            deps=['silver_dim_date'],
            required_resource_keys={"insurance_db"}, 
            owners=['armando.n90@gmail.com', 'team:data'],
            metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Dimensión fecha de fin de término en nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
            tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_term_end_date(context):

    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    dimension_manager.create_date_view_definitions()
    result = dimension_manager.dim_term_end_date_definition

    with insurance_db.get_connection() as conn:
        conn.execute(result)

    logging.info("Successful execution")
    return 
        
   
# SILVER - DIMENSION CONSULTATION DATE

@dg.asset(name='silver_dim_consultation_date', group_name='silver',
            deps=['silver_dim_date'],
            required_resource_keys={"insurance_db"},
            owners=['armando.n90@gmail.com', 'team:data'],
            metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Dimensión fecha de consulta en nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
            tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_consultation_date(context):
        
    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    dimension_manager.create_date_view_definitions()
    result = dimension_manager.dim_consultation_date_definition

    with insurance_db.get_connection() as conn:
        conn.execute(result)

    logging.info("Successful execution")
    return
        

# SILVER - DIMENSION INCIDENT DATE

@dg.asset(name='silver_dim_incident_date', group_name='silver',
            deps=['silver_dim_date'],
            required_resource_keys={"insurance_db"},
            owners=['armando.n90@gmail.com', 'team:data'],
            metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Dimensión fecha de incidente en nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
            tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_incident_date(context):
    
    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    dimension_manager.create_date_view_definitions()
    result = dimension_manager.dim_incident_date_definition

    with insurance_db.get_connection() as conn:
        conn.execute(result)

    logging.info("Successful execution")
    return
        

# SILVER - DIMENSION PAYMENT DATE

@dg.asset(name='silver_dim_payment_date', group_name='silver',
            deps=['silver_dim_date'],
            required_resource_keys={"insurance_db"},
            owners=['armando.n90@gmail.com', 'team:data'],
            metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Dimensión fecha de pago en nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
            tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_payment_date(context):
        
    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    dimension_manager.create_date_view_definitions()
    result = dimension_manager.dim_payment_date_definition

    with insurance_db.get_connection() as conn:
        conn.execute(result)

    logging.info("Successful execution")
    return
        

# SILVER - DIMENSION FIRST EXPENSE DATE

@dg.asset(name='silver_dim_first_expense_date', group_name='silver',
            deps=['silver_dim_date'],
            required_resource_keys={"insurance_db"},
            owners=['armando.n90@gmail.com', 'team:data'],
            metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Dimensión fecha de primer gasto en nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
            tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_first_expense_date(context):
    
    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    dimension_manager.create_date_view_definitions()
    result = dimension_manager.dim_first_expense_date_definition

    with insurance_db.get_connection() as conn:
        conn.execute(result)

    logging.info("Successful execution")
    return
        

# SILVER - DIMENSION MONTH CONT DATE

@dg.asset(name='silver_dim_month_cont_date', group_name='silver',
            deps=['silver_dim_date'],
            required_resource_keys={"insurance_db"},
            owners=['armando.n90@gmail.com', 'team:data'],
            metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Dimensión fecha de continuidad mensual en nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
            tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'},)
def silver_dim_month_cont_date(context):
    
    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    dimension_manager.create_date_view_definitions()
    result = dimension_manager.dim_month_cont_date_definition

    with insurance_db.get_connection() as conn:
        conn.execute(result)

    logging.info("Successful execution")
    return
        

# GOVERNANCE - DIMENSION SCREEN

@dg.asset(name='generate_dim_screen', group_name='governance', 
          deps=['init_datawarehouse_resourses'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Dimensión de politicas de validacion"),
                'filepath': dg.MetadataValue.path("duckdb.governance")},
          tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'})
def generate_dim_screen(context):

    insurance_db = context.resources.insurance_db
    dimension_manager = DimensionManager()

    result = dimension_manager.init_screen_dimension()

    with insurance_db.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE governance.dim_screen AS SELECT * FROM result")

    logging.info("Successful execution")
    return


# GOVERNANCE - FACT ERROR EVENT

@dg.asset(name='generate_fact_error_event', group_name='governance', 
          deps=['generate_dim_screen'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Hechos de eventos de error"),
                'filepath': dg.MetadataValue.path("duckdb.governance")},
          tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'})
def generate_fact_error_event(context):

    insurance_db = context.resources.insurance_db

    sql = """CREATE OR REPLACE TABLE governance.fact_error_event (
                error_event_id INTEGER, 
                batch_id INTEGER
             )"""
    
    with insurance_db.get_connection() as conn:
        conn.execute(sql)

    logging.info("Successful execution")
    return


# GOVERNANCE - FACT ERROR EVENT DETAIL

@dg.asset(name='generate_fact_error_event_detail', group_name='governance', 
          deps=['generate_fact_error_event'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Hechos de eventos de error detallados"),
                'filepath': dg.MetadataValue.path("duckdb.governance")},
          tags={'domain': 'governance', 'sensitive': 'no', 'quality': 'silver'})
def generate_fact_error_event_detail(context):

    insurance_db = context.resources.insurance_db

    sql = """CREATE OR REPLACE TABLE governance.fact_error_event_detail (
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
            )"""
    
    with insurance_db.get_connection() as conn:
        conn.execute(sql)

    logging.info("Successful execution")
    return


# JOB FOR INIT DB
@dg.job
def init_resources():
    init_datawarehouse_resources()
    silver_dim_date()
    silver_dim_birth_date()
    silver_dim_term_begin_date()
    silver_dim_term_end_date()
    silver_dim_consultation_date()
    silver_dim_incident_date()
    silver_dim_payment_date()
    silver_dim_first_expense_date()
    silver_dim_month_cont_date()
    generate_dim_screen()
    generate_fact_error_event()
    generate_fact_error_event_detail()