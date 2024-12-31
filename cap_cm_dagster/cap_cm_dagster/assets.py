from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import cap_cm_md_project


@dbt_assets(manifest=cap_cm_md_project.manifest_path)
def cap_cm_md_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    