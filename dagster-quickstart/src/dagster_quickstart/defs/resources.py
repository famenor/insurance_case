from dagster_duckdb import DuckDBResource
import dagster as dg

class BaseConfig(dg.Config):
    date: str

@dg.definitions
def resources() -> dg.Definitions:
    resources = {
        "base_config": BaseConfig(date="20251029"),
        "insurance_db": DuckDBResource(database="insurance_case.db"),
    }
    return dg.Definitions(resources=resources)


