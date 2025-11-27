import json

#EXTRACT SPECIAL FIELDS FROM CONSULTATION CLOSURE JSON
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
