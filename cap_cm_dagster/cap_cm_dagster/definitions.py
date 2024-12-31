from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import cap_cm_md_dbt_assets
from .project import cap_cm_md_project
from .schedules import schedules

defs = Definitions(
    assets=[cap_cm_md_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=cap_cm_md_project),
    },
)