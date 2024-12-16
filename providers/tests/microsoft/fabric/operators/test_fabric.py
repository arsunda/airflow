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

from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.microsoft.fabric.operators.fabric import MSFabricRunItemOperator
from airflow.providers.microsoft.fabric.triggers.fabric import MSFabricRunItemTrigger
from airflow.utils import timezone

default_config = {
    "task_id": "run_fabric_operator",
    "workspace_id": "workspace_id",
    "item_id": "item_id",
    "fabric_conn_id": "fabric_conn",
    "job_type": "Pipeline",
    "job_params": {"param1": "value1"},
    "timeout": 300,
    "check_interval": 10,
}

SUCCESS_TRIGGER_EVENT = {
    "status": "success",
    "message": "Run completed successfully",
    "run_id": "test_run_id",
    "item_run_status": "Completed",
}

ERROR_TRIGGER_EVENT = {
    "status": "error",
    "message": "Run failed",
    "run_id": "test_run_id",
    "item_run_status": "Failed",
}

IN_PROGRESS_EVENT = {
    "status": "in_progress",
    "message": "Run is still in progress",
    "run_id": "test_run_id",
    "item_run_status": "InProgress",
}

DEFAULT_DATE = timezone.datetime(2021, 1, 1)


class TestMSFabricRunItemOperator:
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_execute_defers(self, mock_get_connection):
        operator = MSFabricRunItemOperator(**default_config)
        context = MagicMock()

        with (
            mock.patch.object(
                operator.hook, "run_fabric_item", return_value="mock_location"
            ) as mock_run_item,
            mock.patch.object(
                operator.hook,
                "get_item_run_details",
                return_value={"status": "InProgress", "id": "mock_run_id"},
            ),
        ):
            with pytest.raises(TaskDeferred) as exc:
                operator.execute(context)

            assert isinstance(exc.value.trigger, MSFabricRunItemTrigger)
            mock_run_item.assert_called_once_with(
                workspace_id=default_config["workspace_id"],
                item_id=default_config["item_id"],
                job_type=default_config["job_type"],
                job_params=default_config["job_params"],
            )

    def test_execute_complete_success(self):
        operator = MSFabricRunItemOperator(**default_config)
        context = {"ti": MagicMock()}

        operator.execute_complete(context=context, event=SUCCESS_TRIGGER_EVENT)

        assert context["ti"].xcom_push.call_count == 1
        context["ti"].xcom_push.assert_any_call(key="run_status", value="Completed")

    def test_execute_complete_error(self):
        operator = MSFabricRunItemOperator(**default_config)
        context = {"ti": MagicMock()}

        with pytest.raises(AirflowException, match="Run failed") as exc:
            operator.execute_complete(context=context, event=ERROR_TRIGGER_EVENT)

        assert "Run failed" in str(exc.value)
        context["ti"].xcom_push.assert_not_called()

    def test_execute_complete_no_event(self):
        operator = MSFabricRunItemOperator(**default_config)
        context = {"ti": MagicMock()}

        operator.execute_complete(context=context, event=None)

        context["ti"].xcom_push.assert_not_called()

    def test_missing_required_param(self):
        invalid_config = default_config.copy()
        del invalid_config["workspace_id"]

        with pytest.raises(TypeError):
            MSFabricRunItemOperator(**invalid_config)

    @pytest.mark.db_test
    def test_fabric_link(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            MSFabricRunItemOperator,
            dag_id="test_fabric_run_op_link",
            **default_config,
        )

        ti.xcom_push(key="run_id", value="test_run_id")
        url = ti.task.get_extra_links(ti, "Monitor Item Run")

        expected_url = (
            f"https://app.fabric.microsoft.com/workloads/data-pipeline/monitoring/workspaces/"
            f"{default_config['workspace_id']}/pipelines/{default_config['item_id']}/test_run_id?experience=data-factory"
        )

        assert url == expected_url
