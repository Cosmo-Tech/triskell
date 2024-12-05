import os
from pathlib import Path
import sys
import polling2
import pathlib
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
import requests


def update_credentials(workspace_id: str, dataset_id: str):
    token = os.environ.get("TOKEN")
    # First step, get datasources
    get_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/datasources"
    response = requests.get(
        url=get_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    if response is None:
        return None
    output_data = response.json().get("value")
    credential_details = {
        "credentialDetails": {
            "credentialType": "OAuth2",
            "useCallerAADIdentity": True,
            "encryptedConnection": "Encrypted",
            "encryptionAlgorithm": "None",
            "privacyLevel": "Organizational",
        }
    }
    for datasource in output_data:
        if datasource.get("datasourceType") != "Extension":
            continue
        gateway_id = datasource.get("gatewayId")
        datasource_id = datasource.get("datasourceId")
        update_url = f"https://api.powerbi.com/v1.0/myorg/gateways/{gateway_id}/datasources/{datasource_id}"
        response = requests.patch(
            url=update_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json=credential_details,
        )


def update_param(workspace_id: str, params: list[tuple[str, str]], dataset_id: str):
    if not params:
        return None
    token = os.environ.get("TOKEN")
    # Preparing parameter data
    details = {
        "updateDetails": [
            {"name": param.get("id"), "newValue": param.get("value")}
            for param in params
        ]
    }
    try:
        update_url = (
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
            f"/datasets/{dataset_id}/Default.UpdateParameters"
        )
        response = requests.post(
            url=update_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json=details,
        )
        print("[powerbi] parameters successfully updated")
        return response.json()
    except Exception as e:
        print(e)


def add_user(workspace_id: str, user: dict):
    token = os.environ.get("TOKEN")
    url_users = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users"
    try:
        res = requests.post(
            url=url_users,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json={
                "identifier": user.get("identifier"),
                "groupUserAccessRight": user.get("rights"),
                "principalType": user.get("principalType"),
            },
        )
        data = res.json()
        print(f"identifier successfully added {data}")
    except Exception as e:
        print(e)


def upload(workspace_id: str, pbix_file: Path, name: str):
    token = os.environ.get("TOKEN")
    header = {
        "Content-Type": "multipart/form-data",
        "Authorization": f"Bearer {token}",
    }
    route = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/imports?datasetDisplayName={name}&nameConflict=CreateOrOverwrite"
    )
    session = requests.Session()
    import_data = {}
    output_data = {}
    if pbix_file.exists():
        with open(pbix_file, "rb") as _f:
            response = session.post(url=route, headers=header, files={"file": _f})
            import_data = response.json()
            route_ = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/imports/{import_data.get('id')}"
            handler = polling2.poll(
                lambda: requests.get(
                    url=route_,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {token}",
                    },
                ),
                check_success=is_correct_response_app,
                step=1,
                timeout=60,
            )
            output_data = handler.json()
    return output_data


def is_correct_response_app(response):
    output_data = response.json()
    if output_data.get("importState") == "Succeeded":
        return output_data


def get_org_id_by_name(organization_name: str):
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = os.environ.get(
        "NAMESPACE"
    )  # Assuming custom resource is in default namespace
    plural = "organizations"
    try:
        api_response = api_instance.get_namespaced_custom_object(
            group=group,
            namespace=namespace,
            name=organization_name,
            plural=plural,
            version=version,
        )
        return api_response.get("spec").get("id")
    except Exception as e:
        print("Exception: %s\n" % e)


def get_work_id_by_name(workspace_name: str):
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = os.environ.get(
        "NAMESPACE"
    )  # Assuming custom resource is in default namespace
    plural = "workspaces"
    try:
        api_response = api_instance.get_namespaced_custom_object(
            group=group,
            namespace=namespace,
            name=workspace_name,
            plural=plural,
            version=version,
        )
        return api_response.get("spec").get("id")
    except Exception as e:
        print("Exception: %s\n" % e)


def get_by_name_or_id(name: str, workspace_id: str = ""):
    token = os.environ.get("TOKEN")
    url_groups = "https://api.powerbi.com/v1.0/myorg/groups"
    params = (
        {"$filter": f"id eq '{workspace_id}'"}
        if workspace_id
        else {"$filter": f"name eq '{name}'"}
    )
    try:
        response = requests.get(
            url=url_groups,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            params=params,
        )
        workspace_data = response.json().get("value")
        if workspace_data and len(workspace_data):
            return workspace_data[0]["id"]
    except Exception as e:
        print(e)


def delete_report(workspace_id: str, report_id: str):
    token = os.environ.get("TOKEN")
    urls_reports = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}"
    )
    try:
        response = requests.delete(
            url=urls_reports,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        return response
    except Exception as e:
        print(e)


def delete_dataset(workspace_id: str, dataset_id: str):
    token = os.environ.get("TOKEN")
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}"
    try:
        response = requests.delete(
            url=url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        return response
    except Exception as e:
        print(e)


def main():
    api_instance = client.CustomObjectsApi()
    group = "powerbi.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = os.environ.get(
        "NAMESPACE"
    )  # Assuming custom resource is in default namespace
    plural = "reports"

    # Watch for events on custom resource
    resource_version = ""
    while True:
        stream = watch.Watch().stream(
            api_instance.list_namespaced_custom_object,
            group,
            version,
            namespace,
            plural,
            resource_version=resource_version,
        )
        for event in stream:
            custom_resource = event["object"]
            event_type = event["type"]
            # Extract custom resource name
            resource_name = custom_resource["metadata"]["name"]
            # Extract key-value pairs from the custom resource spec
            resource_data = custom_resource.get("spec", {})
            # Handle events of type ADDED (resource created)
            if event_type == "ADDED":
                params = resource_data.get("parameters", [])
                report_obj = upload(
                    workspace_id=resource_data.get("workspaceId"),
                    pbix_file=pathlib.Path(resource_data.get("path")),
                    name=resource_data.get("name"),
                )
                if report_obj:
                    for d in report_obj.get("datasets", []):
                        update_param(
                            workspace_id=resource_data.get("workspaceId"),
                            dataset_id=d.get("id"),
                            params=params,
                        )
                        update_credentials(
                            workspace_id=resource_data.get("workspaceId"),
                            dataset_id=d.get("id"),
                        )
                        custom_resource["spec"]["datasetId"] = d.get("id")
                    custom_resource["spec"]["id"] = report_obj.get("reports")[0].get(
                        "id"
                    )
                    link = "https://app.powerbi.com/"
                    link += f"groups/{resource_data.get('workspaceId')}/"
                    link += f"reports/{custom_resource['spec']['id']}/"
                    link += "ReportSection?experience=power-bi"
                    custom_resource["spec"]["link"] = link
                try:
                    api_instance.patch_namespaced_custom_object(
                        group,
                        version,
                        namespace,
                        plural,
                        resource_name,
                        custom_resource,
                    )
                except ApiException as e:
                    print("Exception when calling patch: %s\n" % e)
            elif event_type == "MODIFIED":
                pass
            # Handle events of type DELETED (resource deleted)
            elif event_type == "DELETED":
                delete_dataset(
                    workspace_id=resource_data.get("workspaceId"),
                    dataset_id=resource_data.get("datasetId"),
                )
            # Update resource_version to resume watching from the last event
            resource_version = custom_resource["metadata"]["resourceVersion"]


def check_env():
    for e in ["TOKEN", "NAMESPACE"]:
        if e not in os.environ:
            print(f"{e} is missing in triskell secret")
            sys.exit(1)


if __name__ == "__main__":
    check_env()
    config.load_incluster_config()  # Use in-cluster configuration
    main()
