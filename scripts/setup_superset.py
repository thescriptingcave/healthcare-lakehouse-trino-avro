#!/usr/bin/env python3
"""
Provision Superset with the Healthcare Lakehouse dashboards.

Idempotent: creates the Trino database connection, four datasets, example
charts, and a dashboard if they are missing. Safe to re-run.

Usage:  uv run python scripts/setup_superset.py
Requires: the Superset service running at SUPERSET_URL with admin credentials
(SUPERSET_ADMIN_USERNAME / SUPERSET_ADMIN_PASSWORD, default admin/admin).
"""

import json
import logging
import os
import sys
from typing import Any, cast

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088").rstrip("/")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USERNAME", "admin")
ADMIN_PASS = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

DB_NAME = "Trino (Iceberg)"
DB_URI = "trino://trino@trino:8080/nessie"

DATASETS = [
    {"schema": "healthcare", "table_name": "patients"},
    {"schema": "healthcare", "table_name": "encounters"},
    {"schema": "healthcare", "table_name": "observations"},
    {"schema": "clinical_archive", "table_name": "high_vitals_history"},
]

DASHBOARD_TITLE = "Healthcare Lakehouse"

# Superset REST API filter expressions used to list resources without their columns.
LIST_Q = "(columns:!(),filters:!(),keys:!(none))"
DETAIL_Q = "(columns:!(advanced,columns))"

# Shared session keeps cookies (and therefore the CSRF token) across requests.
_SESSION = requests.Session()
_CSRF: str = ""


def api(token: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    """Perform an authenticated API call against Superset and return JSON."""
    headers = {"Authorization": f"Bearer {token}"}
    if method.lower() not in {"get"}:
        headers["X-CSRFToken"] = _CSRF
    if "headers" in kwargs:
        kwargs["headers"].update(headers)
    else:
        kwargs["headers"] = headers
    resp = _SESSION.request(method, f"{SUPERSET_URL}{path}", timeout=60, **kwargs)
    if not resp.ok:
        raise RuntimeError(f"{method} {path} -> {resp.status_code}: {resp.text[:500]}")
    return cast("dict[str, Any]", resp.json())


def get_token() -> str:
    """Obtain a bearer token and the CSRF token using the admin credentials."""
    global _CSRF
    login = _SESSION.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": ADMIN_USER, "password": ADMIN_PASS, "provider": "db", "refresh": True},
        timeout=30,
    )
    login.raise_for_status()
    token = cast("str", login.json().get("access_token"))
    if not token:
        raise RuntimeError("Superset login returned no access token")
    _CSRF = (
        _SESSION.get(
            f"{SUPERSET_URL}/api/v1/security/csrf_token/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        .json()
        .get("result", "")
    )
    if not _CSRF:
        raise RuntimeError("Superset login returned no CSRF token")
    return token


def get_or_create_database(token: str) -> int:
    """Create (or reuse) the Trino database connection."""
    payload = api(token, "GET", "/api/v1/database/", params={"q": LIST_Q})
    for db in cast("list[dict[str, Any]]", payload.get("result", [])):
        if db["database_name"] == DB_NAME:
            logger.info("Database %r already exists (id=%d)", DB_NAME, db["id"])
            return cast("int", db["id"])
    created = api(
        token,
        "POST",
        "/api/v1/database/",
        json={
            "database_name": DB_NAME,
            "sqlalchemy_uri": DB_URI,
            "expose_in_sqllab": True,
            "allow_file_upload": False,
        },
    )
    db_id = cast("int", created["id"])
    logger.info("Created database %r (id=%d)", DB_NAME, db_id)
    return db_id


def get_or_create_dataset(token: str, db_id: int, dataset: dict[str, str]) -> dict[str, Any]:
    """Create (or reuse) an explore dataset backed by a Nessie table."""
    payload = api(token, "GET", "/api/v1/dataset/", params={"q": LIST_Q})
    for ds in cast("list[dict[str, Any]]", payload.get("result", [])):
        if ds["table_name"] == dataset["table_name"] and ds.get("schema") == dataset["schema"]:
            logger.info("Dataset %r already exists (id=%d)", dataset["table_name"], ds["id"])
            ds["columns"] = columns_for(token, ds["id"])
            return ds
    created = api(
        token,
        "POST",
        "/api/v1/dataset/",
        json={"database": db_id, "schema": dataset["schema"], "table_name": dataset["table_name"]},
    )
    ds_id = cast("int", created["id"])
    logger.info("Created dataset %r (id=%d)", dataset["table_name"], ds_id)
    updated = api(token, "PUT", f"/api/v1/dataset/{ds_id}", json={"default_endpoint": ""})
    return cast("dict[str, Any]", updated)


def columns_for(token: str, ds_id: int) -> list[dict[str, Any]]:
    """Return the dataset's column definitions (with ids) from its detail."""
    full = api(token, "GET", f"/api/v1/dataset/{ds_id}", params={"q": DETAIL_Q})
    cols = cast("list[dict[str, Any]]", full.get("result", {}).get("columns", []))
    logger.info("Dataset %d has %d columns", ds_id, len(cols))
    return cols


def column_for(cols: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    """Return the column object matching the given column name."""
    return next((c for c in cols if c["column_name"] == name), None)


def column_ref(col: dict[str, Any]) -> dict[str, Any]:
    """Column reference suitable for groupby/filters."""
    return {
        "id": col["id"],
        "column_name": col["column_name"],
        "type": col.get("type"),
        "label": col["column_name"],
        "sqlExpression": col["column_name"],
    }


def metric_count(col: dict[str, Any]) -> dict[str, Any]:
    """Build a COUNT metric definition for a dataset column."""
    return {
        "expressionType": "SIMPLE",
        "column": column_ref(col),
        "aggregate": "COUNT",
        "label": f"COUNT({col['column_name']})",
        "optionName": f"metric_{col['column_name']}",
    }


def get_or_create_chart(
    token: str, slice_name: str, viz_type: str, datasource_id: int, params: dict[str, Any]
) -> int:
    """Create (or reuse) a chart."""
    payload = api(token, "GET", "/api/v1/chart/", params={"q": LIST_Q})
    for chart in cast("list[dict[str, Any]]", payload.get("result", [])):
        if chart["slice_name"] == slice_name:
            logger.info("Chart %r already exists (id=%d)", slice_name, chart["id"])
            return cast("int", chart["id"])
    created = api(
        token,
        "POST",
        "/api/v1/chart/",
        json={
            "slice_name": slice_name,
            "viz_type": viz_type,
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        },
    )
    chart_id = cast("int", created["id"])
    logger.info("Created chart %r (id=%d)", slice_name, chart_id)
    return chart_id


def get_or_create_dashboard(token: str) -> int:
    """Create (or reuse) the dashboard shell."""
    payload = api(token, "GET", "/api/v1/dashboard/", params={"q": LIST_Q})
    for dash in cast("list[dict[str, Any]]", payload.get("result", [])):
        if dash["dashboard_title"] == DASHBOARD_TITLE:
            logger.info("Dashboard %r already exists (id=%d)", DASHBOARD_TITLE, dash["id"])
            return cast("int", dash["id"])
    created = api(token, "POST", "/api/v1/dashboard/", json={"dashboard_title": DASHBOARD_TITLE})
    dash_id = cast("int", created["id"])
    logger.info("Created dashboard %r (id=%d)", DASHBOARD_TITLE, dash_id)
    return dash_id


def attach_charts(token: str, dash_id: int, charts: list[tuple[str, int, int]]) -> None:
    """Attach charts to the dashboard using a tabbed v2 layout."""
    chart_ids = [cid for _, cid, _ in charts]

    tab_id = "TAB-0"
    row_ids = [f"ROW-{i}" for i in range(len(charts))]

    positions: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "children": ["TABS_ID"], "id": "ROOT_ID"},
        "TABS_ID": {
            "type": "TABS",
            "children": [tab_id],
            "id": "TABS_ID",
            "meta": {"defaultActiveTab": tab_id},
        },
        tab_id: {
            "type": "TAB",
            "meta": {
                "text": "Overview",
                "defaultText": "Overview",
                "placeholder": "Tab title",
                "width": 12,
                "height": 12,
                "headerHeight": 0,
                "currentChart": charts[0][1],
                "sliceIds": chart_ids,
            },
            "id": tab_id,
            "children": row_ids,
            "parents": ["ROOT_ID", "TABS_ID"],
        },
    }

    for i, (name, cid, _) in enumerate(charts):
        row_id = row_ids[i]
        positions[row_id] = {
            "type": "ROW",
            "children": [f"CHART-{cid}"],
            "id": row_id,
            "meta": {"0": "ROOT_ID", "1": "TABS_ID", "2": tab_id},
            "parents": ["ROOT_ID", "TABS_ID", tab_id],
        }
        positions[f"CHART-{cid}"] = {
            "type": "CHART",
            "id": f"CHART-{cid}",
            "children": [],
            "meta": {
                "chartId": cid,
                "sliceName": name,
                "sliceId": cid,
                "width": 12,
                "height": 12,
                "row": i,
                "col": 0,
            },
            "parents": ["ROOT_ID", "TABS_ID", tab_id, row_id],
            "parent": row_id,
        }

    api(
        token,
        "PUT",
        f"/api/v1/dashboard/{dash_id}",
        json={
            "dashboard_title": DASHBOARD_TITLE,
            "json_metadata": json.dumps({}),
            "position_json": json.dumps(positions),
        },
    )
    logger.info("Attached %d charts to dashboard %d", len(charts), dash_id)


def verify_charts(token: str, charts: list[tuple[str, int, int]]) -> None:
    """Run each chart's query through /api/v1/chart/data to confirm it returns rows."""
    for name, cid, dsid in charts:
        chart = api(token, "GET", f"/api/v1/chart/{cid}")["result"]
        params = json.loads(chart["params"])
        queries: list[dict[str, Any]] = [
            {
                "datasource_id": dsid,
                "datasource_type": "table",
                "metrics": params.get("metrics", []),
                "groupby": params.get("groupby", []),
                "columns": [],
                "filters": [],
                "time_range": "No filter",
                "row_limit": 5,
                "annotation_layers": [],
                "post_processing": [],
                "extras": {"having": "", "where": ""},
                "url_params": {},
                "custom_params": {},
            }
        ]
        if params["viz_type"] == "table":
            queries[0]["columns"] = [c["column_name"] for c in params.get("all_columns", [])]
            queries[0]["metrics"] = []
            queries[0]["groupby"] = []
        resp = api(
            token,
            "POST",
            "/api/v1/chart/data",
            json={
                "datasource": {"id": dsid, "type": "table"},
                "queries": queries,
                "force": False,
                "result_format": "json",
                "result_type": "full",
            },
        )
        data = resp.get("result", [])
        rows = data[0].get("data", []) if data else []
        if not rows:
            logger.error("Chart %r returned no rows", name)
            sys.exit(1)
        logger.info("Chart %r verified: %d rows", name, len(rows))


def main() -> None:
    """Provision the database, datasets, charts, and dashboard."""
    token = get_token()
    logger.info("Authenticated as %r", ADMIN_USER)

    db_id = get_or_create_database(token)

    datasets: dict[str, dict[str, Any]] = {}
    for spec in DATASETS:
        dataset = get_or_create_dataset(token, db_id, spec)
        datasets[spec["table_name"]] = dataset

    charts: list[tuple[str, int, int]] = []

    # Patients by gender
    cols = columns_for(token, datasets["patients"]["id"])
    if col := column_for(cols, "gender"):
        charts.append(
            (
                "Patients by Gender",
                get_or_create_chart(
                    token,
                    "Patients by Gender",
                    "dist_bar",
                    datasets["patients"]["id"],
                    {
                        "viz_type": "dist_bar",
                        "color_scheme": "bnbColors",
                        "groupby": [column_ref(col)],
                        "metrics": [metric_count(col)],
                    },
                ),
                datasets["patients"]["id"],
            )
        )

    # Encounters by class
    cols = columns_for(token, datasets["encounters"]["id"])
    if col := column_for(cols, "encounterclass"):
        charts.append(
            (
                "Encounters by Class",
                get_or_create_chart(
                    token,
                    "Encounters by Class",
                    "dist_bar",
                    datasets["encounters"]["id"],
                    {
                        "viz_type": "dist_bar",
                        "color_scheme": "bnbColors",
                        "groupby": [column_ref(col)],
                        "metrics": [metric_count(col)],
                        "row_limit": 10,
                    },
                ),
                datasets["encounters"]["id"],
            )
        )

    # Patients by city
    cols = columns_for(token, datasets["patients"]["id"])
    if col := column_for(cols, "city"):
        charts.append(
            (
                "Patients by City",
                get_or_create_chart(
                    token,
                    "Patients by City",
                    "dist_bar",
                    datasets["patients"]["id"],
                    {
                        "viz_type": "dist_bar",
                        "color_scheme": "bnbColors",
                        "groupby": [column_ref(col)],
                        "metrics": [metric_count(col)],
                        "row_limit": 10,
                    },
                ),
                datasets["patients"]["id"],
            )
        )

    # High heart-rate archive
    cols = columns_for(token, datasets["high_vitals_history"]["id"])
    charts.append(
        (
            "High Heart Rate Archive",
            get_or_create_chart(
                token,
                "High Heart Rate Archive",
                "table",
                datasets["high_vitals_history"]["id"],
                {
                    "viz_type": "table",
                    "all_columns": [
                        c
                        for c in cols
                        if c["column_name"] in ("patient_id", "first", "heart_rate", "timestamp")
                    ],
                    "row_limit": 25,
                },
            ),
            datasets["high_vitals_history"]["id"],
        )
    )

    if not charts:
        logger.error("No charts created; aborting dashboard assembly")
        sys.exit(1)

    dash_id = get_or_create_dashboard(token)
    attach_charts(token, dash_id, charts)
    verify_charts(token, charts)
    logger.info(
        "Done. Open http://localhost:8088 and log in as %r to see '%s'.",
        ADMIN_USER,
        DASHBOARD_TITLE,
    )


if __name__ == "__main__":
    main()
