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
from __future__ import annotations

import time
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.microsoft.fabric.hooks.fabric import (
    FabricRunItemException,
    MSFabricHook,
)
from airflow.providers.microsoft.fabric.triggers.fabric import MSFabricRunItemTrigger

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


class MSFabricRunItemLink(BaseOperatorLink):
    """Link to the Fabric item run details page."""

    name = "Monitor Item Run"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ):
        self.item_run_id = XCom.get_value(key="run_id", ti_key=ti_key)
        self.workspace_id = operator.workspace_id  # type: ignore
        self.item_id = operator.item_id  # type: ignore
        self.base_url = "https://app.fabric.microsoft.com"
        conn_id = operator.fabric_conn_id  # type: ignore
        self.hook = MSFabricHook(fabric_conn_id=conn_id)

        item_details = self.hook.get_item_details(self.workspace_id, self.item_id)
        self.item_name = item_details.get("displayName")
        url = None

        if operator.job_type == "RunNotebook":  # type: ignore
            url = f"{self.base_url}/groups/{self.workspace_id}/synapsenotebooks/{self.item_id}?experience=data-factory"

        elif operator.job_type == "Pipeline":  # type: ignore
            url = f"{self.base_url}/workloads/data-pipeline/monitoring/workspaces/{self.workspace_id}/pipelines/{self.item_name}/{self.item_run_id}?experience=data-factory"

        return url


class MSFabricRunItemOperator(BaseOperator):
    """
    A Microsoft Fabric API operator which allows you execute REST API call to run a Fabric item (pipeline or notebook) in a Fabric workspace.

    https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/run-on-demand-item-job?tabs=HTTP

    :param workspace_id: Microsoft Fabric workspace ID where your item is located.
    :param item_id: Microsoft Fabric Item ID you want to run.
    :param fabric_conn_id: A MS Fabric Connection ID to run the operator against.
    :param job_type: Set to `Pipeline` if you are running a pipeline. Set to `RunNotebook` if you are running a notebook.
    :param job_params: Additional parameters to pass along with REST API call.
    :param timeout: Time in seconds when triggers will timeout.
    :param check_interval: Time in seconds to check on a item run's status.
    """

    template_fields: Sequence[str] = (
        "workspace_id",
        "item_id",
        "job_type",
        "fabric_conn_id",
        "job_params",
    )
    template_fields_renderers = {"parameters": "json"}

    operator_extra_links = (MSFabricRunItemLink(),)

    def __init__(
        self,
        *,
        workspace_id: str,
        item_id: str,
        fabric_conn_id: str,
        job_type: str,
        job_params: dict | None = None,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_type = job_type
        self.timeout = timeout
        self.check_interval = check_interval
        self.job_params = job_params

    @cached_property
    def hook(self) -> MSFabricHook:
        """Create and return the MSFabricHook (cached)."""
        return MSFabricHook(fabric_conn_id=self.fabric_conn_id)

    def execute(self, context: Context) -> None:
        # Execute the item run
        self.location = self.hook.run_fabric_item(
            workspace_id=self.workspace_id,
            item_id=self.item_id,
            job_type=self.job_type,
            job_params=self.job_params,
        )
        item_run_details = self.hook.get_item_run_details(self.location)

        self.item_run_status = item_run_details["status"]
        self.item_run_id = item_run_details["id"]

        # Push the run id to XCom regardless of what happen during execution
        context["ti"].xcom_push(key="run_id", value=self.item_run_id)
        context["ti"].xcom_push(key="location", value=self.location)

        self.defer(
            trigger=MSFabricRunItemTrigger(
                fabric_conn_id=self.fabric_conn_id,
                item_run_id=self.item_run_id,
                workspace_id=self.workspace_id,
                item_id=self.item_id,
                job_type=self.job_type,
                check_interval=self.check_interval,
                end_time=time.monotonic() + self.timeout,
            ),
            method_name="execute_complete",
        )
        return

    def execute_complete(self, context: Context, event) -> None:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            self.log.info(event["message"])
            if event["status"] == "error":
                raise FabricRunItemException(event["message"])
            context["ti"].xcom_push(key="run_status", value=event["item_run_status"])
