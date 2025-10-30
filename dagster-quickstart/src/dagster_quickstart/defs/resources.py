from dagster_duckdb import DuckDBResource
import dagster as dg

class BaseConfig(dg.Config):
    date: str

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"base_config": BaseConfig(date="20251029")})


#database_resource = DuckDBResource(database="/tmp/jaffle_platform.duckdb")

#@dg.definitions
#def resources():
#    return dg.Definitions(
#        resources={
#            "duckdb": database_resource,
#        }
#    )
