import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
from airflow.providers.microsoft.powerbi.hooks.fabric import FabricHook

@pytest.fixture
def fabric_hook():
    fabric_conn_id = "fabric_conn"
    return FabricHook(fabric_conn_id=fabric_conn_id)

def test_get_connection_form_widgets(fabric_hook):
    form_widgets = FabricHook.get_connection_form_widgets()
    assert isinstance(form_widgets, dict)
    assert "refresh_token" in form_widgets
    assert "extra__microsoft__tenant_id" in form_widgets

def test_get_ui_field_behaviour(fabric_hook):
    field_behaviour = FabricHook.get_ui_field_behaviour()
    assert isinstance(field_behaviour, dict)
    assert "hidden_fields" in field_behaviour
    assert "relabeling" in field_behaviour

def test_init(fabric_hook):
    assert fabric_hook.fabric_conn_id == "fabric_conn"

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._get_token")
def test_get_headers(mock_get_token, fabric_hook):
    mock_get_token.return_value = "access_token"
    headers = fabric_hook.get_headers()
    assert isinstance(headers, dict)
    assert "Authorization" in headers
    assert headers["Authorization"] == "Bearer access_token"

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._send_request")
def test_get_item_run_details(mock_send_request, fabric_hook):
    location = "https://api.fabric.microsoft.com/runs/123"
    fabric_hook.get_item_run_details(location)
    mock_send_request.assert_called_once_with("GET", location)

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._send_request")
def test_get_item_details(mock_send_request, fabric_hook):
    workspace_id = "workspace_id"
    item_id = "item_id"
    fabric_hook.get_item_details(workspace_id, item_id)
    expected_url = f"https://api.fabric.microsoft.com/workspaces/{workspace_id}/items/{item_id}"
    mock_send_request.assert_called_once_with("GET", expected_url)

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._send_request")
def test_run_fabric_item(mock_send_request, fabric_hook):
    workspace_id = "workspace_id"
    item_id = "item_id"
    job_type = "job_type"
    fabric_hook.run_fabric_item(workspace_id, item_id, job_type)
    expected_url = f"https://api.fabric.microsoft.com/workspaces/{workspace_id}/items/{item_id}/runs"
    mock_send_request.assert_called_once_with("POST", expected_url, json={"jobType": job_type})

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook.wait_for_item_run_status")
@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._send_request")
def test_wait_for_item_run_status(mock_send_request, mock_wait_for_status, fabric_hook):
    location = "https://api.fabric.microsoft.com/runs/123"
    target_status = "Completed"
    fabric_hook.wait_for_item_run_status(location, target_status)
    mock_wait_for_status.assert_called_once_with(location, target_status)

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._send_request")
def test_send_request(mock_send_request, fabric_hook):
    request_type = "GET"
    url = "https://api.fabric.microsoft.com/test"
    fabric_hook._send_request(request_type, url)
    mock_send_request.assert_called_once_with(request_type, url)

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._get_token")
def test_send_request_with_token(mock_get_token, mock_send_request, fabric_hook):
    mock_get_token.return_value = "access_token"
    request_type = "GET"
    url = "https://api.fabric.microsoft.com/test"
    fabric_hook._send_request(request_type, url, use_token=True)
    mock_get_token.assert_called_once()
    mock_send_request.assert_called_once_with(request_type, url, headers={"Authorization": "Bearer access_token"})

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._get_token")
def test_send_request_with_token_expired(mock_get_token, fabric_hook):
    mock_get_token.side_effect = AirflowException("Token expired")
    request_type = "GET"
    url = "https://api.fabric.microsoft.com/test"
    with pytest.raises(AirflowException):
        fabric_hook._send_request(request_type, url, use_token=True)
    mock_get_token.assert_called_once()

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._get_token")
def test_send_request_without_token(mock_get_token, mock_send_request, fabric_hook):
    request_type = "GET"
    url = "https://api.fabric.microsoft.com/test"
    fabric_hook._send_request(request_type, url, use_token=False)
    mock_get_token.assert_not_called()
    mock_send_request.assert_called_once_with(request_type, url)

@patch("airflow.providers.microsoft.powerbi.hooks.fabric.FabricHook._get_token")
def test_send_request_with_custom_headers(mock_get_token, mock_send_request, fabric_hook):
    mock_get_token.return_value = "access_token"
    request_type = "GET"
    url = "https://api.fabric.microsoft.com/test"
    headers = {"Content-Type": "application/json"}
    fabric_hook._send_request(request_type, url, headers=headers)
    mock_get_token.assert_called_once()
    mock_send_request.assert_called_once_with(
        request_type, url, headers={"Content-Type": "application/json", "Authorization": "Bearer access_token"}
    )
