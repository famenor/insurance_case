import pandas as pd
import numpy as np
import datetime
from abc import ABC, abstractmethod

## SUBSYSTEM 05 FOR LOGGING ERROR EVENTS
class InterfaceErrorEventLogsGenerator(ABC):

    @abstractmethod
    def generate_error_inputs(self):
        pass

class ErrorEventLogsGenerator(InterfaceErrorEventLogsGenerator):

    def __init__(self, error_events_detail: pd.DataFrame):
        self.error_events_detail = error_events_detail
        self.error_events = None

    def generate_error_inputs(self):

        if self.error_events_detail is not None:
            if self.error_events_detail.shape[0] > 0:
                
                self.error_events = self.error_events_detail[['error_event_id', 'batch_id']].drop_duplicates().reset_index(drop=True)
                self.error_events_detail = self.error_events_detail