import pandas as pd
import numpy as np
import datetime
from abc import ABC, abstractmethod

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

        if pd.isnull(value):
            return False     

        try:
            datetime.datetime.strptime(value, date_format)
            result = False
        
        except ValueError:
            result = True
        
        return result

class ValidatorIsLowerThan(AbstractValidator):

    def validate(self, **kwargs):

        value_a = kwargs.get('value_a', None)
        value_b = kwargs.get('value_b', None) 
      
        result = False

        if pd.isnull(value_a) or pd.isnull(value_b):
            return result

        if value_a < value_b:
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

class ValidatorScreenIsLowerThan(AbstractValidationScreen):

    def apply_validation(self, array_a, array_b):
        validator = ValidatorIsLowerThan()
        results = pd.Series(zip(array_a, array_b)).apply(lambda x: validator.validate(value_a=x[0], value_b=x[1]))
        return results
    
    
#FACADE FOR SCREENS

class FacadeValidationScreens():

    def __init__(self):
        self.event_errors = []
        self.screen_is_not_unique = ValidationScreenIsNotUnique()
        self.screen_is_missing_value = ValidationScreenIsMisssingValue()
        self.screen_is_out_of_bounds_value = ValidationScreenIsOutOfBoundsValue()
        self.screen_is_out_of_list_value = ValidationScreenIsOutOfListValue()
        self.screen_is_not_digit_string = ValidationScreenIsNotDigitString()
        self.screen_is_not_date_format = ValidationScreenIsNotDateFormat()
        self.screen_is_lower_than = ValidatorScreenIsLowerThan()

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

    def apply_screen_is_lower_than(self, column, reference_column):
        
        screen_id = 7        
        self.data['__screen__'] = self.screen_is_lower_than.apply_validation(self.data[column], self.data[reference_column])
        
        errors = self.data.loc[self.data['__screen__'] == True][[self.identifier, column]].copy()
        self.ensemble_error_event_detail(errors, column, screen_id)
