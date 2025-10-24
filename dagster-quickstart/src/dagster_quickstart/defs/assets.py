import json
import pandas as pd
import dagster as dg

def get_field(x, field):

    result = None

    if field in ['consultationObservation', 'nextConsultationPending', 'needsPrescriptionOrMedicalOrder']:
        if not 'dayNote' in x.keys():
            return result
        
        if x['dayNote'] == []:
            return result

        if not field in x['dayNote'].keys():
            return result
            
        result = x['dayNote'][field]

    elif field in ['patientGoal', 'specialistGoal', 'pauseConsultations']:

        if not field in x.keys():
            return result
            
        result = x[field]

    return result

@dg.asset
def parse_consultations_data():

    consultations = pd.read_csv('/home/armando/git/insurance_case/datalake/raw/consultas_dummy.csv', sep=';')
    consultations['closure'] = consultations['closure'].apply(lambda x: json.loads(x))
    consultations['day_note_consultation_observation'] = consultations['closure'].apply(lambda x: get_field(x, 'consultationObservation'))
    consultations['day_note_next_consultation_pending'] = consultations['closure'].apply(lambda x: get_field(x, 'nextConsultationPending'))
    consultations['day_note_needs_prescription_or_medical_order'] = consultations['closure'].apply(lambda x: get_field(x, 'needsPrescriptionOrMedicalOrder'))
    consultations['patiend_goal'] = consultations['closure'].apply(lambda x: get_field(x, 'patientGoal'))
    consultations['specialist_goal'] = consultations['closure'].apply(lambda x: get_field(x, 'specialistGoal'))
    consultations['pause_consultations'] = consultations['closure'].apply(lambda x: get_field(x, 'pauseConsultations'))
    
    consultations = consultations.drop(columns=['closure'])

    consultations.to_csv('/home/armando/git/insurance_case/datalake/processed/consultations_dummy.csv', index=False)

    return "Data parsed successfully"

#deps=[parse_consultations_data]
@dg.asset
def parse_consultations_diagnosis_data():
    
    consultations = pd.read_csv('/home/armando/git/insurance_case/datalake/raw/consultas_dummy.csv', sep=';')
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
    diagnoses_df.to_csv('/home/armando/git/insurance_case/datalake/processed/consultation_diagnoses_dummy.csv', index=False)

    return consultations