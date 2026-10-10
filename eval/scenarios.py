"""The failures the study measures against, and what is actually true about each one.

Ground truth comes from how the harness breaks the pipeline, not from anyone's reading
of the diagnosis. `root_cause` is where the problem lives; `fix_in_repo` says whether a
change to this repository can correct it. Those two are the yardstick: a diagnosis is
right when it names the right place, and a patch is useful only when `fix_in_repo`.

The two `data` cases are deliberately contestable, and the paper should say so: code
*could* be changed to tolerate duplicate keys or to raise a DQ threshold, and that is
exactly the change we don't want an agent making on its own.
"""

from typing import NamedTuple, Optional


class Scenario(NamedTuple):
    id: str
    dag_id: str
    task_id: str
    setup: Optional[str]        # `python -m` module that breaks it, None if broken by default
    root_cause: str             # code | config | data | environment
    fix_in_repo: bool           # can a change in this repository fix it?
    sensor: bool                # slow: waits for a sensor timeout
    summary: str


SCENARIOS = [
    Scenario(
        id="schema_drift",
        dag_id="orders_refresh",
        task_id="load_to_warehouse",
        setup="scripts.scenarios.add_column_to_orders_extract",
        root_cause="config",
        fix_in_repo=True,
        sensor=False,
        summary="The orders extract gains an effective_ts column that stg_orders doesn't have. "
                "A migration adding the column is a correct fix.",
    ),
    Scenario(
        id="export_path",
        dag_id="daily_sales_export",
        task_id="load_to_warehouse",
        setup=None,
        root_cause="config",
        fix_in_repo=True,
        sensor=False,
        summary="export_path in pipelines.yaml points at a directory that doesn't exist. "
                "Correcting the path fixes it.",
    ),
    Scenario(
        id="duplicate_keys",
        dag_id="product_dim_upsert",
        task_id="load_to_warehouse",
        setup="scripts.scenarios.inject_duplicate_product_keys",
        root_cause="data",
        fix_in_repo=False,
        sensor=False,
        summary="The product feed repeats product_id, so the upsert raises CardinalityViolation. "
                "The merge SQL is correct; the feed is wrong.",
    ),
    Scenario(
        id="dq_threshold",
        dag_id="product_deactivation_check",
        task_id="load_to_warehouse",
        setup="scripts.scenarios.bulk_deactivate_products",
        root_cause="data",
        fix_in_repo=False,
        sensor=False,
        summary="More products are deactivated than the configured threshold allows. The check is "
                "doing its job; a person must validate the volume.",
    ),
    Scenario(
        id="dropped_table",
        dag_id="legacy_hierarchy_load",
        task_id="load_to_warehouse",
        setup="scripts.scenarios.drop_hierarchy_table",
        root_cause="environment",
        fix_in_repo=False,
        sensor=False,
        summary="dim_hierarchy_v1 was dropped outside the repository. The DAG is unchanged and "
                "correct; the table has to come back.",
    ),
    Scenario(
        id="file_never_lands",
        dag_id="vendor_feed_ingest",
        task_id="wait_for_upstream_file",
        setup=None,
        root_cause="environment",
        fix_in_repo=False,
        sensor=True,
        summary="The vendor file never arrives and the sensor times out. Nothing in the repository "
                "can make the file appear.",
    ),
    Scenario(
        id="api_throttled",
        dag_id="vendor_contact_sync",
        task_id="load_to_warehouse",
        setup="scripts.scenarios.throttle_vendor_api",
        root_cause="environment",
        fix_in_repo=False,
        sensor=False,
        summary="The vendor API rate-limits us at 15 requests/minute. The extract is wasteful "
                "(a token per record) but no repository change gets 81 requests under 15.",
    ),
    Scenario(
        id="api_moved",
        dag_id="pricing_api_pull",
        task_id="load_to_warehouse",
        setup="scripts.scenarios.move_api_endpoint",
        root_cause="environment",
        fix_in_repo=False,
        sensor=False,
        summary="The vendor moved the API under /v2 and the old paths 404. The new contract has to "
                "be confirmed before any config is edited.",
    ),
]

BY_ID = {s.id: s for s in SCENARIOS}
ROOT_CAUSES = ["code", "config", "data", "environment"]
