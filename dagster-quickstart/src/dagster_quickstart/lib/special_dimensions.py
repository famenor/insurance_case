import pandas as pd
import numpy as np
import datetime

from abc import ABC, abstractmethod

## SUBSYSTEM 12 FOR SPECIAL DIMENSIONS MANAGEMENT
class InterfaceSpecialDimensionManager(ABC):
    
    @abstractmethod
    def create_date_dimension(self):
        pass

    @abstractmethod
    def create_screen_dimension(self):
        pass

class SpecialDimensionManager(InterfaceSpecialDimensionManager):

    def __init__(self):
        self.dim_date = None
        self.dim_screen = None

    def create_date_dimension(self):
        
        array_dates = pd.date_range(start='1900-01-01', end='1900-01-01')
        array_dates = array_dates.append(pd.date_range(start='1925-01-01', end='2029-12-31'))
        array_dates = array_dates.append(pd.date_range(start='2200-01-01', end='2200-01-01'))

        dim_date = pd.DataFrame({'date_id': array_dates, 'date': array_dates})
        dim_date['date_type'] = 'Fecha Disponible'

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

        special_dates = pd.DataFrame({'date_id': [-1, -2, -3, -4],
                                      'date': [None, None, None, None],
                                      'date_type': ['No Aplica', 'No Disponible', 'Por Definir', 'Fecha Inválida'],
                                      'day': [0, 0, 0, 0],
                                      'month': [0, 0, 0, 0],
                                      'year': [0, 0, 0, 0],
                                      'day_of_week': [0, 0, 0, 0],
                                      'day_name': ['No Aplica', 'No Disponible', 'Por Definir', 'Fecha Inválida'],
                                      'month_name': ['No Aplica', 'No Disponible', 'Por Definir', 'Fecha Inválida'],
                                      'quarter': [0, 0, 0, 0],
                                      'weekday_indicator': ['No Aplica', 'No Disponible', 'Por Definir', 'Fecha Inválida'],
                                      'week_of_year': [0, 0, 0, 0],
                                      'is_end_of_month': [False, False, False, False],
                                      'is_end_of_quarter': [False, False, False, False],
                                      'is_end_of_year': [False, False, False, False],
                                      'quarter_name': ['No Aplica', 'No Disponible', 'Por Definir', 'Fecha Inválida'],
                                      'year_month': ['0000-00', '0000-00', '0000-00', '0000-00'],
                                      'quarter_year': ['No Aplica', 'No Disponible', 'Por Definir', 'Fecha Inválida'],
                                      'holiday_indicator': ['No Aplica', 'No Disponible', 'Por Definir', 'Fecha Inválida']})

        self.dim_date = pd.concat([dim_date, special_dates], ignore_index=True)

    def create_screen_dimension(self):

        dim_screen = pd.DataFrame({
            'screen_id': [1, 2, 3, 4, 5, 6, 7],
            'screen_type': ['Columnar', 'Columnar', 'Estructural', 'Columnar', 'Columnar',
                             'Estructural', 'Negocio'],
            'screen_name': ['Valor nulo', 'Valor fuera de intervalo', 'Valor fuera de lista', 
                            'Cadena no compuesta por dígitos ', 'Fecha con formato incorrecto', 
                            'Valor no único', 'Valor menor que otro valor'],
            'screen_description': ['Valor nulo', 'Valor fuera de intervalo', 'Valor fuera de lista', 
                            'Cadena no compuesta por dígitos ', 'Fecha con formato incorrecto', 
                            'Valor no único', 'Valor menor que otro valor'],
            'etl_module': ['v0.0.1'] * 7
        })

        self.dim_screen = dim_screen 