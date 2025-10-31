import dagster as dg
import logging

import pandas as pd
import numpy as np
import datetime
import duckdb
from abc import ABC, abstractmethod

from dagster_quickstart.lib.extraction import *
from dagster_dbt import DbtProjectComponent, dbt_assets

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')



## BRONZE - CERTIFICATES
@dg.asset(name='extract_certificates', group_name='bronze', 
          deps=['raw_certificates_data'],
          required_resource_keys={"insurance_db"},
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
                'link_to_docs': dg.MetadataValue.url("www.google.com"),
                'snippet': dg.MetadataValue.md("Certificados a nivel bronce"),
                'filepath': dg.MetadataValue.path("duckdb.bronze")},
          tags={'domain': 'medical', 'sensitive': 'yes', 'quality': 'bronze'})
def extract_certificates(context):

    path = '../datalake/raw/certificate_dummy.csv'
    extractor = CertificatesCsvExtractor()
    extractor.set_path(path)
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

"""

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
          deps=['load_dim_cerfificate', 'generate_dim_date', 'parse_consultations_data'])
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
          deps=['load_fact_consultation', 'load_dim_cie', 'parse_consultations_diagnosis_data'])
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
"""
