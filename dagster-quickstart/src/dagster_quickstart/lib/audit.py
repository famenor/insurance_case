import pandas as pd
import numpy as np
import datetime
from abc import ABC, abstractmethod

## SUBSYSTEM 06 FOR AUDIT DIMENSION ASSEMBLE
class AuditDimensionAssembler():

    def __init__(self, error_detail_table, table_name):
        self.error_detail_table = error_detail_table
        self.table_name = table_name
        self.etl_version = None

    def get_unsolved_rows(self):

        if self.error_detail_table is None:
            return []

        unsolved_rows = self.error_detail_table.loc[self.error_detail_table['error_condition'] == 'Sin Resolver']
        unsolved_rows = unsolved_rows.loc[unsolved_rows['table_name'] == self.table_name]
        unsolved_rows = unsolved_rows['record_identifier'].unique().tolist()

        return unsolved_rows