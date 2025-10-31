import json
import pandas as pd
import dagster as dg
import logging

from dagster_quickstart.lib.preprocessing_tools import get_field
from .resources import BaseConfig

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


# RAW - CONSULTATIONS

@dg.asset(name='raw_consultations_data', group_name='raw',
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Datos originales de diagnósticos de las consultas médicas"),
            'filepath': dg.MetadataValue.path("datalake/raw/")},
          tags={'domain': 'medicine', 'sensitive': 'no', 'quality': 'raw'},
          )
def raw_consultations_data() -> pd.DataFrame:
    #config: BaseConfig
    #{config.date}
    path = f"../datalake/raw/consultas/20251029/consultas_dummy.csv"
    consultations = pd.read_csv(path, sep=';')
    return consultations


# RAW - CERTIFICATES

@dg.asset(name='raw_certificates_data', group_name='raw',
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Datos originales de certificados"),
            'filepath': dg.MetadataValue.path("datalake/raw/")},
          tags={'domain': 'medicine', 'sensitive': 'yes', 'quality': 'raw'},
          )
def raw_certificates_data() -> pd.DataFrame:
    path = f"../datalake/raw/certificate_dummy.csv"
    certificates = pd.read_csv(path)    
    return certificates


# RAW - TERMS

@dg.asset(name='raw_terms_data', group_name='raw',
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Términos originales de los certificados"),
            'filepath': dg.MetadataValue.path("datalake/raw/")},
          tags={'domain': 'medicine', 'sensitive': 'no', 'quality': 'raw'},
          )
def raw_terms_data() -> pd.DataFrame:
    path = f"../datalake/raw/terms_dummy.csv"
    terms = pd.read_csv(path)
    return terms


# RAW - CIE

@dg.asset(name='raw_cie_data', group_name='raw',
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Datos originales CIE"),
            'filepath': dg.MetadataValue.path("datalake/raw/")},
          tags={'domain': 'medicine', 'sensitive': 'no', 'quality': 'raw'},
          )
def raw_cie_data() -> pd.DataFrame:
    path = f"../datalake/raw/cat_cie_10.csv"
    cie = pd.read_csv(path)
    return cie


# RAW - CLAIMS

@dg.asset(name='raw_claims_data', group_name='raw',
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Datos originales de los reclamos"),
            'filepath': dg.MetadataValue.path("datalake/raw/")},
          tags={'domain': 'insurance', 'sensitive': 'no', 'quality': 'raw'},
          )
def raw_claims_data() -> pd.DataFrame:
    path = f"../datalake/raw/claims_dummy.csv"
    claims = pd.read_csv(path)
    return claims


# PREPROCESSED - CONSULTATIONS

@dg.asset(name='parse_consultations_data', group_name='preprocessed',
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Datos de consultas médicas"),
            'filepath': dg.MetadataValue.path("path/to/file")},
          tags={'domain': 'medicine', 'sensitive': 'no', 'quality': 'preprocessed'})
def parse_consultations_data(raw_consultations_data: pd.DataFrame) -> pd.DataFrame:

    consultations = raw_consultations_data
    consultations['closure'] = consultations['closure'].apply(lambda x: json.loads(x))
    consultations['day_note_consultation_observation'] = consultations['closure'].apply(lambda x: get_field(x, 'consultationObservation'))
    consultations['day_note_next_consultation_pending'] = consultations['closure'].apply(lambda x: get_field(x, 'nextConsultationPending'))
    consultations['day_note_needs_prescription_or_medical_order'] = consultations['closure'].apply(lambda x: get_field(x, 'needsPrescriptionOrMedicalOrder'))
    consultations['patiend_goal'] = consultations['closure'].apply(lambda x: get_field(x, 'patientGoal'))
    consultations['specialist_goal'] = consultations['closure'].apply(lambda x: get_field(x, 'specialistGoal'))
    consultations['pause_consultations'] = consultations['closure'].apply(lambda x: get_field(x, 'pauseConsultations'))
    
    consultations = consultations.drop(columns=['closure'])
    consultations.to_csv('../datalake/preprocessed/consultations_dummy.csv', index=False)

    return consultations


# PREPROCESSED - DIAGNOSIS

@dg.asset(name='parse_consultations_diagnosis_data', group_name='preprocessed',
          owners=['armando.n90@gmail.com', 'team:data'],
          metadata={
            'link_to_docs': dg.MetadataValue.url("www.google.com"),
            'snippet': dg.MetadataValue.md("Datos de diagnósticos de las consultas médicas"),
            'filepath': dg.MetadataValue.path("path/to/file")},
          tags={'domain': 'medicine', 'sensitive': 'no', 'quality': 'preprocessed'}
          )
def parse_consultations_diagnosis_data(raw_consultations_data: pd.DataFrame) -> pd.DataFrame:
    
    consultations = raw_consultations_data
    consultations['closure'] = consultations['closure'].apply(lambda x: json.loads(x))

    array_id = []
    array_diagnosis = []

    for index, row in consultations.iterrows():
        closure = row['closure']   

        if 'patientDiagnoses' in closure.keys():
            diagnoses = closure['patientDiagnoses']

            if diagnoses == []:
                continue

            for diagnosis in diagnoses:

                if diagnosis == []:
                    continue

                array_id.append(row['id'])
                array_diagnosis.append(diagnosis['patientDiagnose'])

    diagnoses_df = pd.DataFrame({'consultation_id': array_id, 'diagnosis': array_diagnosis})
    diagnoses_df.to_csv('../datalake/preprocessed/consultation_diagnoses_dummy.csv', index=False)

    return diagnoses_df


# JOB FOR RAW EXTRACTION

@dg.job
def extract_raw_data():
    raw_consultations = raw_consultations_data()
    raw_certificates = raw_certificates_data()
    raw_terms = raw_terms_data()
    raw_claims = raw_claims_data()
    raw_cie = raw_cie_data()
    parsed_consultations = parse_consultations_data(raw_consultations)
    parsed_diagnoses = parse_consultations_diagnosis_data(raw_consultations)
    
@dg.schedule(cron_schedule="30 00 * * *", job=extract_raw_data, name="extract_raw_data_schedule")
def extract_raw_data_schedule():
    return {}

#    return {
#        "ops": {
#            "raw_consultations_data": {
#                "config": {
#                    "date": "20231101"
#                }
#            }
#        }
#    }