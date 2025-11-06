import dagster as dg
import logging

import pandas as pd
import numpy as np
import datetime
import duckdb
from abc import ABC, abstractmethod

from dagster_quickstart.lib.extraction import *
from dagster_quickstart.lib.dimension_management import *
from dagster_quickstart.lib.fact_management import *

from dagster_dbt import DbtProjectComponent, dbt_assets

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')



## BRONZE - CERTIFICATES
@dg.asset(name='extract_certificates', group_name='bronze', 
          deps=['raw_certificates_data', 'silver_dim_birth_date'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Certificados a nivel bronce"),
                'filepath': dg.MetadataValue.path("duckdb.bronze")},
          tags={'domain': 'medical', 'sensitive': 'yes', 'quality': 'bronze'})
def extract_certificates(context):

    path = '../datalake/raw/certificate_dummy.csv'
    extractor = CertificatesCsvExtractor(context={'file_path': path})
    extractor.process()

    data = extractor.data
    error_events = extractor.error_events
    error_events_detail = extractor.error_events_detail

    insurance_db = context.resources.insurance_db
    with insurance_db.get_connection() as conn:

        if error_events is not None and error_events.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event SELECT * FROM error_events")

        if error_events_detail is not None and error_events_detail.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event_detail SELECT * FROM error_events_detail")
            raise Exception('Dimension data contains errors. Cannot export data to datawarehouse.')

        if data is not None and data.shape[0] > 0:
            conn.sql("CREATE OR REPLACE TABLE bronze.dim_certificate AS SELECT * FROM data")

    return

## SILVER - CERTIFICATES
@dg.asset(name='load_dim_certificate', group_name='silver', 
          deps=['extract_certificates'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Certificados a nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'medical', 'sensitive': 'yes', 'quality': 'silver'})
def load_dim_certificate(context, extract_certificates):

    insurance_db = context.resources.insurance_db
    inputs = {}
    with insurance_db.get_connection() as conn:
        inputs['dim_birth_date'] = conn.sql("SELECT birth_date_id, date FROM silver.dim_birth_date").df()
        inputs['bronze_dimension'] = conn.sql("SELECT * FROM bronze.dim_certificate").df()

    certificate_dimension_builder = CertificatesDimensionBuilder()
 
    dimension_manager = DimensionManager()
    dimension_manager.build_dimension(certificate_dimension_builder, inputs)

    silver_dimension = dimension_manager.silver_dimension
    with insurance_db.get_connection() as conn:
        conn.sql("CREATE OR REPLACE TABLE silver.dim_certificate AS SELECT * FROM silver_dimension")

    return


## BRONZE - CIE
@dg.asset(name='extract_cie', group_name='bronze', 
          deps=['raw_cie_data'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("CIE a nivel bronce"),
                'filepath': dg.MetadataValue.path("duckdb.bronze")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'bronze'})
def extract_cie(context):

    path = '../datalake/raw/cat_cie_10.csv'
    insurance_db = context.resources.insurance_db

    extractor = CieCsvExtractor(context={'file_path': path})
    extractor.process()

    data = extractor.data
    error_events = extractor.error_events
    error_events_detail = extractor.error_events_detail

    with insurance_db.get_connection() as conn:

        if error_events is not None and error_events.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event SELECT * FROM error_events")

        if error_events_detail is not None and error_events_detail.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event_detail SELECT * FROM error_events_detail")
            raise Exception('Dimension data contains errors. Cannot export data to datawarehouse.')

        if data is not None and data.shape[0] > 0:
            conn.sql("CREATE OR REPLACE TABLE bronze.dim_cie AS SELECT * FROM data")


    return


## SILVER - CIE
@dg.asset(name='load_dim_cie', group_name='silver', 
          deps=['extract_cie'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("CIE a nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'silver'})
def load_dim_cie(context, extract_cie):

    insurance_db = context.resources.insurance_db
    inputs = {}
    with insurance_db.get_connection() as conn:
        inputs['bronze_dimension'] = conn.sql("SELECT * FROM bronze.dim_cie").df()

    cie_dimension_builder = CieDimensionBuilder()
 
    dimension_manager = DimensionManager()
    dimension_manager.build_dimension(cie_dimension_builder, inputs)

    silver_dimension = dimension_manager.silver_dimension
    with insurance_db.get_connection() as conn:
        conn.sql("CREATE OR REPLACE TABLE silver.dim_cie AS SELECT * FROM silver_dimension")

    return


## TERMS
@dg.asset(name='extract_terms', group_name='bronze', 
          deps=['load_dim_certificate', 'silver_dim_term_begin_date', 'silver_dim_term_end_date'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Términos a nivel bronce"),
                'filepath': dg.MetadataValue.path("duckdb.bronze")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'bronze'})
def extract_terms(context, load_dim_certificate):

    path = '../datalake/raw/terms_dummy.csv'
    insurance_db = context.resources.insurance_db

    with insurance_db.get_connection() as conn:
        certificate_ids = conn.sql("SELECT certificate_id FROM silver.dim_certificate").df()
    
    extractor_context = {'file_path': path, 
                         'list_values': {'certificate_ids': certificate_ids['certificate_id'].tolist()}}

    extractor = TermsCsvExtractor(context=extractor_context)
    extractor.process()

    data = extractor.data
    error_events = extractor.error_events
    error_events_detail = extractor.error_events_detail

    with insurance_db.get_connection() as conn:

        if error_events is not None and error_events.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event SELECT * FROM error_events")

        if error_events_detail is not None and error_events_detail.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event_detail SELECT * FROM error_events_detail")

        if data is not None and data.shape[0] > 0:
            conn.sql("CREATE OR REPLACE TABLE bronze.fact_term AS SELECT * FROM data")

    return


@dg.asset(name='load_fact_term', group_name='silver', 
          deps=['extract_terms'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Términos a nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'silver'})
def load_fact_term(context, extract_terms):
            
    insurance_db = context.resources.insurance_db
    inputs = {}
    with insurance_db.get_connection() as conn:
        
        inputs['dim_term_begin_date'] = conn.sql("SELECT term_begin_date_id, date FROM silver.dim_term_begin_date").df()
        inputs['dim_term_end_date'] = conn.sql("SELECT term_end_date_id, date FROM silver.dim_term_end_date").df()
        inputs['dim_certificate'] = conn.sql("SELECT surrogated_id AS surrogated_certificate_id, certificate_id FROM silver.dim_certificate").df()
        inputs['fact_term'] = conn.sql("SELECT * FROM bronze.fact_term").df()

    term_fact_builder = TermFactBuilder()

    fact_manager = FactManager()
    fact_manager.build_fact(term_fact_builder, inputs)

    silver_fact = fact_manager.silver_fact
    with insurance_db.get_connection() as conn:
        conn.sql("CREATE OR REPLACE TABLE silver.fact_term AS SELECT * FROM silver_fact")

    return


## CONSULTATIONS
@dg.asset(name='extract_consultations', group_name='bronze', 
          deps=['load_dim_certificate', 'silver_dim_consultation_date', 'raw_consultations_data'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Consultas a nivel bronce"),
                'filepath': dg.MetadataValue.path("duckdb.bronze")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'bronze'})
def extract_consultations(context, load_dim_certificate):

    path = '../datalake/preprocessed/consultations_dummy.csv'
    insurance_db = context.resources.insurance_db

    with insurance_db.get_connection() as conn:
        certificate_ids = conn.sql("SELECT certificate_id FROM silver.dim_certificate").df()
    
    extractor_context = {'file_path': path, 
                         'list_values': {'certificate_ids': certificate_ids['certificate_id'].tolist()}}

    extractor = ConsultationsCsvExtractor(context=extractor_context)
    extractor.process()

    data = extractor.data
    error_events = extractor.error_events
    error_events_detail = extractor.error_events_detail

    with insurance_db.get_connection() as conn:

        if error_events is not None and error_events.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event SELECT * FROM error_events")

        if error_events_detail is not None and error_events_detail.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event_detail SELECT * FROM error_events_detail")

        if data is not None and data.shape[0] > 0:
            conn.sql("CREATE OR REPLACE TABLE bronze.fact_consultation AS SELECT * FROM data")

    return


## SILVER - CONSULTATIONS
@dg.asset(name='load_fact_consultation', group_name='silver', 
          deps=['extract_consultations'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Consultas a nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'silver'})
def load_fact_consultation(context, extract_consultations):

    insurance_db = context.resources.insurance_db
    inputs = {}
    with insurance_db.get_connection() as conn:
        
        inputs['dim_certificate'] = conn.sql("SELECT surrogated_id AS surrogated_certificate_id, certificate_id FROM silver.dim_certificate").df()
        inputs['dim_consultation_date'] = conn.sql("SELECT consultation_date_id, date FROM silver.dim_consultation_date").df()
        inputs['fact_consultation'] = conn.sql("SELECT * FROM bronze.fact_consultation").df()

    consultation_fact_builder = ConsultationFactBuilder()

    fact_manager = FactManager()
    fact_manager.build_fact(consultation_fact_builder, inputs)

    silver_fact = fact_manager.silver_fact
    with insurance_db.get_connection() as conn:
        conn.sql("CREATE OR REPLACE TABLE silver.fact_consultation AS SELECT * FROM silver_fact")

    return


## BRONZE - CONSULTATION DIAGNOSES
@dg.asset(name='extract_consultation_diagnoses', group_name='bronze', 
          deps=['load_fact_consultation', 'load_dim_cie', 'parse_consultations_diagnosis_data'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Diagnósticos a nivel bronce"),
                'filepath': dg.MetadataValue.path("duckdb.bronze")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'bronze'})
def extract_consultation_diagnoses(context, load_fact_consultation, load_dim_cie):

    path = '../datalake/preprocessed/consultation_diagnoses_dummy.csv'
    insurance_db = context.resources.insurance_db   

    with insurance_db.get_connection() as conn:
        consultation_ids = conn.sql("SELECT consultation_id FROM silver.fact_consultation").df()
        cie_ids = conn.sql("SELECT cie_id FROM silver.dim_cie").df()
    
    extractor_context = {'file_path': path, 
                         'list_values': {'consultation_ids': consultation_ids['consultation_id'].tolist(),
                                         'cie_ids': cie_ids['cie_id'].tolist()}}

    extractor = ConsultationDiagnosesCsvExtractor(context=extractor_context)
    extractor.process()

    data = extractor.data
    error_events = extractor.error_events
    error_events_detail = extractor.error_events_detail

    with insurance_db.get_connection() as conn:

        if error_events is not None and error_events.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event SELECT * FROM error_events")

        if error_events_detail is not None and error_events_detail.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event_detail SELECT * FROM error_events_detail")

        if data is not None and data.shape[0] > 0:
            conn.sql("CREATE OR REPLACE TABLE bronze.bridge_consultation_diagnosis AS SELECT * FROM data")

    return

## SILVER - CONSULTATION DIAGNOSES
@dg.asset(name='load_bridge_consultation_diagnosis', group_name='silver', 
          deps=['extract_consultation_diagnoses'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Diagnósticos a nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'silver'})
def load_bridge_consultation_diagnosis(context, extract_consultation_diagnoses):   

    insurance_db = context.resources.insurance_db
    inputs = {}
    with insurance_db.get_connection() as conn:
        
        inputs['dim_cie'] = conn.sql("SELECT surrogated_id AS surrogated_cie_id, cie_id FROM silver.dim_cie").df()
        inputs['bronze_fact'] = conn.sql("SELECT * FROM bronze.bridge_consultation_diagnosis").df()

    builder = ConsultationDiagnosesBridgeBuilder()

    fact_manager = FactManager()
    fact_manager.build_fact(builder, inputs)

    silver_fact = fact_manager.silver_fact
    with insurance_db.get_connection() as conn:
        conn.sql("CREATE OR REPLACE TABLE silver.bridge_consultation_diagnosis AS SELECT * FROM silver_fact")

    return

    
## BRONZE - CLAIMS
@dg.asset(name='extract_claims', group_name='bronze', 
          deps=['load_dim_certificate', 'silver_dim_incident_date', 'silver_dim_payment_date',
                'silver_dim_first_expense_date', 'silver_dim_month_cont_date', 'raw_claims_data',
                'load_dim_cie'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Reclamos a nivel bronce"),
                'filepath': dg.MetadataValue.path("duckdb.bronze")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'bronze'})
def extract_claims(context, load_dim_certificate, load_dim_cie):

    path = '../datalake/raw/claims_dummy.csv'
    insurance_db = context.resources.insurance_db

    with insurance_db.get_connection() as conn:
        certificate_numbers = conn.sql("SELECT certificate_number FROM silver.dim_certificate").df()
        cie_identifiers = conn.sql("SELECT cie_id FROM silver.dim_cie").df()
    
    extractor_context = {'file_path': path, 
                         'list_values': {'certificate_numbers': certificate_numbers['certificate_number'].tolist(),
                                         'cie_identifiers': cie_identifiers['cie_id'].tolist()}}

    extractor = ClaimsCsvExtractor(context=extractor_context)
    extractor.process()

    data = extractor.data
    error_events = extractor.error_events
    error_events_detail = extractor.error_events_detail

    with insurance_db.get_connection() as conn:

        if error_events is not None and error_events.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event SELECT * FROM error_events")

        if error_events_detail is not None and error_events_detail.shape[0] > 0:
            conn.sql("INSERT INTO governance.fact_error_event_detail SELECT * FROM error_events_detail")

        if data is not None and data.shape[0] > 0:
            conn.sql("CREATE OR REPLACE TABLE bronze.fact_claim AS SELECT * FROM data")

    return


## SILVER - CLAIMS
@dg.asset(name='load_fact_claim', group_name='silver', 
          deps=['extract_claims'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Reclamos a nivel plata"),
                'filepath': dg.MetadataValue.path("duckdb.silver")},
          tags={'domain': 'medical', 'sensitive': 'no', 'quality': 'silver'})
def load_claim_consultation(context, extract_claims):

    insurance_db = context.resources.insurance_db
    inputs = {}
    with insurance_db.get_connection() as conn:
        
        inputs['silver_incident_date'] = conn.sql("SELECT incident_date_id FROM silver.dim_incident_date").df()
        inputs['silver_payment_date'] = conn.sql("SELECT payment_date_id FROM silver.dim_payment_date").df()
        inputs['silver_first_expense_date'] = conn.sql("SELECT first_expense_date_id FROM silver.dim_first_expense_date").df()
        inputs['silver_month_cont_date'] = conn.sql("SELECT month_cont_date_id FROM silver.dim_month_cont_date").df()
        inputs['silver_certificate_id'] = conn.sql("SELECT surrogated_id AS surrogated_certificate_id, certificate_number FROM silver.dim_certificate").df()
        inputs['silver_cie_id'] = conn.sql("SELECT surrogated_id AS surrogated_cie_id, cie_id FROM silver.dim_cie").df()
        inputs['bronze_fact'] = conn.sql("SELECT * FROM bronze.fact_claim").df()

    builder = ClaimFactBuilder()

    fact_manager = FactManager()
    fact_manager.build_fact(builder, inputs)

    silver_fact = fact_manager.silver_fact
    with insurance_db.get_connection() as conn:
        conn.sql("CREATE OR REPLACE TABLE silver.fact_claim AS SELECT * FROM silver_fact")

    return

## JOB FOR BRONZE AND SILVER TABLES
@dg.job
def generate_insurance_tables():

    br_cert = extract_certificates()
    sl_cert = load_dim_certificate(br_cert)

    br_cie = extract_cie()
    sl_cie = load_dim_cie(br_cie)

    br_term = extract_terms(sl_cert)
    sl_term = load_fact_term(br_term)

    br_cons = extract_consultations(sl_cert)
    sl_cons = load_fact_consultation(br_cons)

    br_diag = extract_consultation_diagnoses(sl_cons, sl_cie)
    sl_diag = load_bridge_consultation_diagnosis(br_diag)

    br_claim = extract_claims(sl_cert, sl_cie)
    load_claim_consultation(br_claim)

#from dagster import AssetSelection, define_asset_job

## JOB FOR GOLD TABLES
#generate_gold_tables = define_asset_job(
#    name="generate_gold_tables",
#    selection=AssetSelection.assets("report_customer_interaction", "report_age_at_diagnosis")
#)




