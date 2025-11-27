import pandas as pd
import numpy as np
import datetime
from abc import ABC, abstractmethod

## SUBSYSTEM 10 FOR SURROGATE KEY GENERATION
class InterfaceSurrogateKeyGenerator(ABC):

    @abstractmethod
    def generate_surrogated_keys(self, n_rows: int):
        pass

class SurrogateKeyGenerator(InterfaceSurrogateKeyGenerator):

    def generate_surrogated_keys(self, n_rows):
        return np.arange(1, n_rows + 1)