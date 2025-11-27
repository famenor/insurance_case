import pandas as pd
import numpy as np
import datetime
from abc import ABC, abstractmethod

## SUBSYSTEM 18 FOR FACT MANAGEMENT
class AbtractFactBuilder(ABC):

    def __init__(self):
        self.silver_fact = None

    @abstractmethod
    def build_fact(self, inputs: dict):
        pass

class TermFactBuilder(AbtractFactBuilder):
    
    def build_fact(self, inputs: dict):

        silver_term_begin_date = inputs['dim_term_begin_date']
        silver_term_end_date = inputs['dim_term_end_date']
        silver_certificate_id = inputs['dim_certificate']
        bronze_fact = inputs['fact_term']

        silver_fact = bronze_fact.copy()
        silver_fact = silver_fact.loc[silver_fact['audit_passed'] == 'Sí']

        shape_01 = silver_fact.shape[0]

        silver_fact = silver_fact.merge(silver_term_begin_date,
                                        left_on='term_begin_date',
                                        right_on='date',
                                        how='inner')

        silver_fact = silver_fact.drop(columns=['term_begin_date', 'date'])

        silver_fact = silver_fact.merge(silver_term_end_date,
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
        

class ConsultationFactBuilder(AbtractFactBuilder):
    
    def build_fact(self, inputs: dict):

        silver_certificate_id = inputs['dim_certificate']
        silver_consultation_date = inputs['dim_consultation_date']
        bronze_fact = inputs['fact_consultation']

        silver_fact = bronze_fact.copy()
        silver_fact = silver_fact.loc[silver_fact['audit_passed'] == 'Sí']

        shape_01 = silver_fact.shape[0]

        silver_fact = silver_fact.merge(silver_consultation_date,
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


class ConsultationDiagnosesBridgeBuilder(AbtractFactBuilder):
    
    def build_fact(self, inputs: dict):

        silver_cie_id = inputs['dim_cie']
        bronze_fact = inputs['bronze_fact']

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


class ClaimFactBuilder(AbtractFactBuilder):
    
    def build_fact(self, inputs: dict):

        silver_incident_date = inputs['silver_incident_date']
        silver_payment_date = inputs['silver_payment_date']
        silver_first_expense_date = inputs['silver_first_expense_date']
        silver_month_cont_date = inputs['silver_month_cont_date']
        silver_certificate_id = inputs['silver_certificate_id']
        silver_cie_id = inputs['silver_cie_id']
        bronze_fact = inputs['bronze_fact']

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


class InterfaceFactManager(ABC):
    
    @abstractmethod
    def build_fact(self, fact_builder: AbtractFactBuilder, inputs: dict):
        pass

class FactManager(InterfaceFactManager):

    def build_fact(self, fact_builder: AbtractFactBuilder, inputs: dict):       
        fact_builder.build_fact(inputs)
        self.silver_fact = fact_builder.silver_fact
