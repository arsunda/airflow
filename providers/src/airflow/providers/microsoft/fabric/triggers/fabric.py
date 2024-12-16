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

import asyncio
import time
from collections.abc import AsyncIterator
from typing import (
    Any,
)

from airflow.providers.microsoft.fabric.hooks.fabric import FabricRunItemStatus, MSFabricAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class MSFabricRunItemTrigger(BaseTrigger):
    """
    A Microsoft Fabric API trigger which allows you to execute an async REST call to run Fabric pipelines or notebooks.

    :param fabric_conn_id: A MS Fabric Connection ID to run the operator against.
    :param item_run_id: Unique Item Run ID associated with specific item run.
    :param workspace_id: Microsoft Fabric workspace ID where your item is located.
    :param item_id: Microsoft Fabric Item ID you want to run.
    :param job_type: Set to `Pipeline` if you are running a pipeline. Set to `RunNotebook` if you are running a notebook.
    :param end_time: Time in seconds when triggers will timeout.
    :param check_interval: Time in seconds to check on a item run's status.
    """

    def __init__(
        self,
        fabric_conn_id: str,
        item_run_id: str,
        workspace_id: str,
        item_id: str,
        job_type: str,
        end_time: float = 60 * 60 * 24 * 7,
        check_interval: int = 60,
    ):
        super().__init__()
        self.fabric_conn_id = fabric_conn_id
        self.item_run_id = item_run_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.job_type = job_type
        self.end_time = end_time
        self.check_interval = check_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the MSFabricRunItemTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "fabric_conn_id": self.fabric_conn_id,
                "item_run_id": self.item_run_id,
                "workspace_id": self.workspace_id,
                "item_id": self.item_id,
                "job_type": self.job_type,
                "end_time": self.end_time,
                "check_interval": self.check_interval,
            },
        )

    def _get_async_hook(self) -> MSFabricAsyncHook:
        return MSFabricAsyncHook(fabric_conn_id=self.fabric_conn_id)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current MS Fabric item run status (Pipeline or notebook) and yields a TriggerEvent."""
        hook = self._get_async_hook()
        item_run_status = None
        try:
            while self.end_time > time.monotonic():
                item_run_details = await hook.async_get_item_run_details(
                    item_run_id=self.item_run_id,
                    workspace_id=self.workspace_id,
                    item_id=self.item_id,
                )
                item_run_status = item_run_details["status"]

                if item_run_status == FabricRunItemStatus.COMPLETED:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"The item run {self.item_run_id} has status {item_run_status}.",
                            "run_id": self.item_run_id,
                            "item_run_status": item_run_status,
                        }
                    )
                    return
                elif item_run_status in FabricRunItemStatus.FAILURE_STATES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"The item run {self.item_run_id} has status {item_run_status}.",
                            "run_id": self.item_run_id,
                            "item_run_status": item_run_status,
                        }
                    )
                    return

                self.log.info(
                    "Sleeping for %s. The item state is %s.",
                    self.check_interval,
                    item_run_status,
                )
                await asyncio.sleep(self.check_interval)
            # Timeout reached
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": f"Timeout reached: Unable to confirm final status of the item run {self.item_run_id}. Last known status: {item_run_status}.",
                    "run_id": self.item_run_id,
                }
            )
        except Exception as error:
            try:
                self.log.info(
                    "Unexpected error %s caught. Cancel pipeline run %s",
                    error,
                    self.item_run_id,
                )
                await hook.async_cancel_item_run(
                    item_run_id=self.item_run_id,
                    workspace_id=self.workspace_id,
                    item_id=self.item_id,
                )
                yield TriggerEvent(
                    {
                        "status": "cancelled",
                        "message": str(error),
                        "run_id": self.item_run_id,
                        "item_run_status": FabricRunItemStatus.CANCELLED,
                    }
                )
                return
            except Exception as error:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": str(error),
                        "run_id": self.item_run_id,
                    }
                )
                return
