import sys
import hashlib
import requests
import os
from kubernetes import client, config, watch
from azure.identity import ClientSecretCredential
from kubernetes.client.rest import ApiException


def get_azure_token(scope: str = "default") -> str:
    """Returns an azure token"""
    credentials = ClientSecretCredential(
        client_id=os.environ.get("AZURE_CLIENT_ID"),
        tenant_id=os.environ.get("AZURE_TENANT_ID"),
        client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
    )
    try:
        token = credentials.get_token(os.environ.get("API_SCOPE"))
        return token.token
    except Exception as e:
        print(e)


def get_by_id(org_id: str):
    headers = get_headers()
    url = os.environ.get("API_URL")
    try:
        response = requests.get(
            url=f"{url}/organizations/{org_id}",
            headers=headers,
        )
        if response.status_code == 404:
            return None
        return response.json()
    except Exception as e:
        print(e)
        return None


def delete_obj(org_id: str):
    headers = get_headers()
    url = os.environ.get("API_URL")
    try:
        requests.delete(
            url=f"{url}/organizations/{org_id}",
            headers=headers,
        )
    except Exception as e:
        print(e)


def update(org_id: str, data: dict):
    headers = get_headers()
    url = os.environ.get("API_URL")
    try:
        requests.patch(
            url=f"{url}/organizations/{org_id}",
            headers=headers,
            json=data,
        )
    except Exception as e:
        print(e)


def get_headers():
    api_key = os.environ.get("API_KEY", False)
    if not api_key:
        token = get_azure_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
    else:
        token = api_key
        headers = {
            "Content-Type": "application/json",
            "X-CSM-API-KEY": f"{api_key}",
        }
    return headers


def create(data: dict):
    headers = get_headers()
    url = os.environ.get("API_URL")
    try:
        response = requests.post(
            url=f"{url}/organizations",
            headers=headers,
            json=data,
        )
        if response.status_code == 404:
            return None
        return response.json()
    except Exception as e:
        print(e)

    return response.json()


def main():
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = os.environ.get(
        "NAMESPACE"
    )  # Assuming custom resource is in default namespace
    plural = "organizations"

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
            resource_name = custom_resource["metadata"]["name"]
            myuid = custom_resource["metadata"]["uid"]
            resource_data = custom_resource.get("spec", {})
            if event_type == "ADDED":
                o = get_by_id(org_id=resource_data.get("id", ""))
                if not o:
                    res_ = create(data=resource_data)
                    p = hashlib.sha1(str(res_.get("id")).encode("utf-8")).hexdigest()
                    custom_resource["spec"]["id"] = res_.get("id")
                    custom_resource["spec"]["uid"] = myuid
                    custom_resource["spec"]["sha"] = p
                    custom_resource["spec"]["name"] = res_.get("name")
                    custom_resource["metadata"] = dict(
                        labels=dict(challenge=res_.get("id")),
                        **custom_resource["metadata"],
                    )
                else:
                    custom_resource["spec"]["id"] = o.get("id")
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
            # Handle events of type DELETED (resource deleted)
            elif event_type == "DELETED":
                if resource_data.get("id"):
                    challenge = hashlib.sha1(
                        str(resource_data.get("id")).encode("utf-8")
                    ).hexdigest()
                    if custom_resource["spec"]["sha"] == challenge:
                        delete_obj(org_id=resource_data.get("id"))
            elif event_type == "MODIFIED":
                if resource_data.get("id"):
                    challenge = hashlib.sha1(
                        str(resource_data.get("id")).encode("utf-8")
                    ).hexdigest()
                    if custom_resource["spec"]["sha"] == challenge:
                        update(org_id=resource_data.get("id"), data=resource_data)
            # Update resource_version to resume watching from the last event
            resource_version = custom_resource["metadata"]["resourceVersion"]


def check_env():
    for e in [
        "API_KEY",
        "API_SCOPE",
        "NAMESPACE",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_TENANT_ID",
        "AZURE_SUBSCRIPTION",
        "AZURE_RESOURCE_GROUP_NAME",
        "AZURE_LOCATION",
        "AZURE_PLATFORM_PRINCIPAL_ID",
        "AZURE_ADX_CLUSTER_NAME",
    ]:
        if e not in os.environ:
            print(f"{e} is missing in triskell secret")
            sys.exit(1)


if __name__ == "__main__":
    check_env()
    config.load_incluster_config()  # Use in-cluster configuration
    main()
