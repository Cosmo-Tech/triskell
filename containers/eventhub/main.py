import os
import sys
from kubernetes import client, config, watch
from azure.identity import ClientSecretCredential
from azure.mgmt.eventhub import EventHubManagementClient
from kubernetes.client.rest import ApiException


def get_azure_token(scope: str = "default") -> str:
    """Returns an azure token"""
    credentials = ClientSecretCredential(
        client_id=os.environ.get("AZURE_CLIENT_ID"),
        tenant_id=os.environ.get("AZURE_TENANT_ID"),
        client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
    )
    scope = os.environ.get("API_SCOPE")
    try:
        token = credentials.get_token(scope)
        return token.token
    except Exception:
        pass


def delete_obj(orga_id: str, work_key: str):
    resource_group_name = os.environ.get("AZURE_RESOURCE_GROUP_NAME")
    namespace_name = f"{orga_id}-{work_key}"
    eventhub_client = EventHubManagementClient(
        credential=ClientSecretCredential(
            client_id=os.environ.get("AZURE_CLIENT_ID"),
            tenant_id=os.environ.get("AZURE_TENANT_ID"),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
        ),
        subscription_id=os.environ.get("AZURE_SUBSCRIPTION"),
    )
    eventhub_client.namespaces.begin_delete(
        resource_group_name=resource_group_name, namespace_name=namespace_name
    ).result()


def get_org_id_by_name(organization_name: str):
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = os.environ.get("NAMESPACE")  # Assuming custom resource is in default namespace
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


def get_work_key_by_name(workspace_name: str):
    api_instance = client.CustomObjectsApi()
    group = "api.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = os.environ.get("NAMESPACE")  # Assuming custom resource is in default namespace
    plural = "workspaces"
    try:
        api_response = api_instance.get_namespaced_custom_object(
            group=group,
            namespace=namespace,
            name=workspace_name,
            plural=plural,
            version=version,
        )
        return api_response.get("spec").get("key")
    except Exception as e:
        print("Exception: %s\n" % e)


def main():
    api_instance = client.CustomObjectsApi()
    group = "azure.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = os.environ.get("NAMESPACE")  # Assuming custom resource is in default namespace
    plural = "eventhubs"
    # Watch for events on custom resource
    resource_version = ""
    credential = ClientSecretCredential(
        client_id=os.environ.get("AZURE_CLIENT_ID"),
        tenant_id=os.environ.get("AZURE_TENANT_ID"),
        client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
    )
    eventhub_client = EventHubManagementClient(
        credential=credential,
        subscription_id=os.environ.get("AZURE_SUBSCRIPTION"),
    )
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
            organization_name = (
                custom_resource["spec"].get("selector", {}).get("organization", "")
            )
            workspace_name = (
                custom_resource["spec"].get("selector", {}).get("workspace", "")
            )
            # Extract key-value pairs from the custom resource spec
            resource_data = custom_resource.get("spec", {})
            # Handle events of type ADDED (resource created)
            if event_type == "ADDED":
                orga_id = get_org_id_by_name(organization_name=organization_name)
                work_key = get_work_key_by_name(workspace_name=workspace_name)
                if orga_id and work_key:
                    resource_group_name = os.environ.get("AZURE_RESOURCE_GROUP_NAME")
                    namespace_name = f"{orga_id}-{work_key}"
                    location = os.environ.get("AZURE_LOCATION")
                    # Create Namespace
                    eventhub_client.namespaces.begin_create_or_update(
                        resource_group_name=resource_group_name,
                        namespace_name=namespace_name,
                        parameters={
                            "sku": {"name": "Standard", "tier": "Standard"},
                            "location": location,
                            "tags": {"tag1": "value1", "tag2": "value2"},
                        },
                    ).result()

                    # Create EventHubs
                    for ev in resource_data.get("consumers"):
                        eventhub_name = ev.get("entity")
                        eventhub = eventhub_client.event_hubs.create_or_update(
                            resource_group_name=resource_group_name,
                            namespace_name=namespace_name,
                            event_hub_name=eventhub_name,
                            parameters={
                                "message_retention_in_days": "4",
                                "partition_count": "4",
                                "status": "Active",
                            },
                        )
                        print("Create EventHub: {}".format(eventhub))
                        # Create Consumer Group
                        consumer_group_name = ev.get("displayName")
                        consumer_group = (
                            eventhub_client.consumer_groups.create_or_update(
                                resource_group_name=resource_group_name,
                                namespace_name=namespace_name,
                                event_hub_name=eventhub_name,
                                consumer_group_name=consumer_group_name,
                                parameters={"user_metadata": "New consumergroup"},
                            )
                        )
                        print("Create consumer group:\n{}".format(consumer_group))

                    try:
                        del resource_data["selector"]
                        custom_resource["spec"]["id"] = namespace_name
                        api_response = api_instance.patch_namespaced_custom_object(
                            group,
                            version,
                            namespace,
                            plural,
                            resource_name,
                            custom_resource,
                        )
                        print(api_response)
                    except ApiException as e:
                        print("Exception when calling patch: %s\n" % e)
            # Handle events of type DELETED (resource deleted)
            elif event_type == "DELETED":
                orga_id = get_org_id_by_name(organization_name=organization_name)
                work_key = get_work_key_by_name(workspace_name=workspace_name)
                delete_obj(orga_id=orga_id, work_key=work_key)
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
