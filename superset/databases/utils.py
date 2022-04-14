# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from typing import Any, Dict, List, Optional

from superset import app, db
from superset.connectors.connector_registry import ConnectorRegistry
from superset.models.core import Database
from superset.connectors.sqla import models

custom_password_store = app.config["SQLALCHEMY_CUSTOM_PASSWORD_STORE"]


def get_foreign_keys_metadata(
    database: Database, table_name: str, schema_name: Optional[str]
) -> List[Dict[str, Any]]:
    foreign_keys = database.get_foreign_keys(table_name, schema_name)
    for fk in foreign_keys:
        fk["column_names"] = fk.pop("constrained_columns")
        fk["type"] = "fk"
    return foreign_keys


def get_indexes_metadata(
    database: Database, table_name: str, schema_name: Optional[str]
) -> List[Dict[str, Any]]:
    indexes = database.get_indexes(table_name, schema_name)
    for idx in indexes:
        idx["type"] = "index"
    return indexes


def get_col_type(col: Dict[Any, Any]) -> str:
    try:
        dtype = f"{col['type']}"
    except Exception:  # pylint: disable=broad-except
        # sqla.types.JSON __str__ has a bug, so using __class__.
        dtype = col["type"].__class__.__name__
    return dtype


def get_table_metadata(
    database: Database, table_name: str, schema_name: Optional[str]
) -> Dict[str, Any]:
    """
    Get table metadata information, including type, pk, fks.
    This function raises SQLAlchemyError when a schema is not found.

    :param database: The database model
    :param table_name: Table name
    :param schema_name: schema name
    :return: Dict table metadata ready for API response
    """
    print('*'*20, 'databases/utils.py->get_table_metadata()')
    keys = []
    if 'flattable' in table_name:
        db_id = database.__dict__['id']
        table_data = db.session.query(models.SqlaTable).filter_by(database_id=db_id).first()
        table_id = table_data.__dict__['id']
        datasource = ConnectorRegistry.get_datasource(
            'table', table_id, db.session
        )
        payload_columns: List[Dict[str, Any]] = []
        payload_columns = [{"comment": None, "keys": [], "longType": col["type"], "name": col["column_name"], "type": col["type"]} for col in datasource.data['columns']]
        select_star = ""
        primary_key = ""
        foreign_keys = []
        keys = []
        table_comment = None
    else:
        columns = database.get_columns(table_name, schema_name)
        primary_key = database.get_pk_constraint(table_name, schema_name)
        if primary_key and primary_key.get("constrained_columns"):
            primary_key["column_names"] = primary_key.pop("constrained_columns")
            primary_key["type"] = "pk"
            keys += [primary_key]
        foreign_keys = get_foreign_keys_metadata(database, table_name, schema_name)
        indexes = get_indexes_metadata(database, table_name, schema_name)
        keys += foreign_keys + indexes
        payload_columns: List[Dict[str, Any]] = []
        table_comment = database.get_table_comment(table_name, schema_name)
        for col in columns:
            dtype = get_col_type(col)
            payload_columns.append(
                {
                    "name": col["name"],
                    "type": dtype.split("(")[0] if "(" in dtype else dtype,
                    "longType": dtype,
                    "keys": [k for k in keys if col["name"] in k["column_names"]],
                    "comment": col.get("comment"),
                }
            )
        select_star = database.select_star(
            table_name,
            schema=schema_name,
            show_cols=True,
            indent=True,
            cols=columns,
            latest_partition=True,
        )
    return {
        "name": table_name,
        "columns": payload_columns,
        "selectStar": select_star,
        "primaryKey": primary_key,
        "foreignKeys": foreign_keys,
        "indexes": keys,
        "comment": table_comment,
    }
