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
from unittest import mock

import pytest

from airflow.providers.microsoft.fabric.triggers.fabric import MSFabricRunItemTrigger
from providers.src.airflow.providers.microsoft.fabric.hooks.fabric import FabricRunItemStatus


class TestMSFabricRunItemTrigger:
    MODULE = "airflow.providers.microsoft.fabric"
    FABRIC_CONN_ID = "fabric_conn_id_example"
    ITEM_RUN_ID = "example_item_run_id"
    WORKSPACE_ID = "example_workspace_id"
    ITEM_ID = "example_item_id"
    JOB_TYPE_PIPELINE = "Pipeline"
    END_TIME_SECONDS = time.monotonic() + 60
    CHECK_INTERVAL_SECONDS = 10

    @pytest.fixture
    def trigger(self):
        """Fixture to create an instance of MSFabricRunItemTrigger"""
        return MSFabricRunItemTrigger(
            fabric_conn_id=self.FABRIC_CONN_ID,
            item_run_id=self.ITEM_RUN_ID,
            workspace_id=self.WORKSPACE_ID,
            item_id=self.ITEM_ID,
            job_type=self.JOB_TYPE_PIPELINE,
            end_time=self.END_TIME_SECONDS,
            check_interval=self.CHECK_INTERVAL_SECONDS,
        )

    def test_trigger_initialization(self, trigger):
        """Test that the trigger initializes correctly."""
        assert trigger.fabric_conn_id == self.FABRIC_CONN_ID
        assert trigger.item_run_id == self.ITEM_RUN_ID
        assert trigger.workspace_id == self.WORKSPACE_ID
        assert trigger.item_id == self.ITEM_ID
        assert trigger.job_type == self.JOB_TYPE_PIPELINE
        assert trigger.end_time == self.END_TIME_SECONDS
        assert trigger.check_interval == self.CHECK_INTERVAL_SECONDS

    def test_serialization(self, trigger):
        """Test that the trigger serializes correctly."""
        expected_serialized = {
            "fabric_conn_id": self.FABRIC_CONN_ID,
            "item_run_id": self.ITEM_RUN_ID,
            "workspace_id": self.WORKSPACE_ID,
            "item_id": self.ITEM_ID,
            "job_type": self.JOB_TYPE_PIPELINE,
            "end_time": self.END_TIME_SECONDS,
            "check_interval": self.CHECK_INTERVAL_SECONDS,
        }
        assert trigger.serialize() == (
            "airflow.providers.microsoft.fabric.triggers.fabric.MSFabricRunItemTrigger",
            expected_serialized,
        )

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.fabric.MSFabricAsyncHook.async_get_item_run_details")
    async def test_run_complete(self, mock_get_item_run_details, trigger):
        """Test that the trigger yields a success event when the run completes successfully."""
        mock_get_item_run_details.return_value = {"status": FabricRunItemStatus.COMPLETED}

        gen = trigger.run()
        event = await gen.asend(None)

        assert event.payload["status"] == "success"
        assert f"The item run {self.ITEM_RUN_ID} has status Completed." in event.payload["message"]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.fabric.MSFabricAsyncHook.async_get_item_run_details")
    async def test_run_failed(self, mock_get_item_run_details, trigger):
        """Test that the trigger yields a failure event when the run fails."""
        mock_get_item_run_details.return_value = {"status": FabricRunItemStatus.FAILED}

        generator = trigger.run()
        event = await generator.asend(None)

        assert event.payload["status"] == "error"
        assert f"The item run {self.ITEM_RUN_ID} has status Failed." in event.payload["message"]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.fabric.MSFabricAsyncHook.async_get_item_run_details")
    async def test_run_timeout(self, mock_get_item_run_details, trigger):
        """Test that the trigger yields an error event when it times out."""
        mock_get_item_run_details.return_value = {"status": FabricRunItemStatus.IN_PROGRESS}

        trigger.end_time = 0  # Simulate timeout

        gen = trigger.run()
        event = await gen.asend(None)

        assert event.payload["status"] == "error"
        assert "Timeout reached" in event.payload["message"]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.fabric.MSFabricAsyncHook.async_cancel_item_run")
    @mock.patch(f"{MODULE}.hooks.fabric.MSFabricAsyncHook.async_get_item_run_details")
    async def test_run_cancellation_success(self, mock_get_item_run_details, mock_cancel_item_run, trigger):
        """Test that the trigger handles successful cancellation."""
        mock_get_item_run_details.side_effect = Exception("Simulated error")
        mock_cancel_item_run.return_value = None

        gen = trigger.run()
        event = await gen.asend(None)

        assert event.payload["status"] == "cancelled"
        assert "Simulated error" in event.payload["message"]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.fabric.MSFabricAsyncHook.async_cancel_item_run")
    async def test_run_cancellation_failure(self, mock_cancel_item_run, trigger):
        """Test that the trigger handles cancellation failure."""
        mock_cancel_item_run.side_effect = Exception("Cancellation error")

        gen = trigger.run()
        event = await gen.asend(None)

        assert event.payload["status"] == "error"
        assert "Cancellation error" in event.payload["message"]
