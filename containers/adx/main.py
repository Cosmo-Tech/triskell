from datetime import timedelta

import json
import os
import sys
from uuid import uuid4
from kubernetes import client, config, watch
from azure.identity import ClientSecretCredential
from kubernetes.client.rest import ApiException
from azure.mgmt.kusto.models import ReadWriteDatabase
from azure.mgmt.kusto import KustoManagementClient
from azure.mgmt.kusto.models import DatabasePrincipalAssignment
from azure.mgmt.kusto.models import EventHubDataConnection
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters


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


def delete_obj(database_name: str):
    kusto_client = KustoManagementClient(
        credential=ClientSecretCredential(
            client_id=os.environ.get("AZURE_CLIENT_ID"),
            tenant_id=os.environ.get("AZURE_TENANT_ID"),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
        ),
        subscription_id=os.environ.get("AZURE_SUBSCRIPTION"),
    )
    resource_group_name = os.environ.get("AZURE_RESOURCE_GROUP_NAME")
    adx_cluster_name = os.environ.get("AZURE_ADX_CLUSTER_NAME")
    kusto_client.databases.begin_delete(
        resource_group_name=resource_group_name,
        cluster_name=adx_cluster_name,
        database_name=database_name,
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
        print(api_response)
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


def delete_permission(principal_id: str, database_name: str):
    subscription = os.environ.get("AZURE_SUBSCRIPTION")
    kusto_client = KustoManagementClient(
        credential=ClientSecretCredential(
            client_id=os.environ.get("AZURE_CLIENT_ID"),
            tenant_id=os.environ.get("AZURE_TENANT_ID"),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
        ),
        subscription_id=subscription,
    )
    resource_group_name = os.environ.get("AZURE_RESOURCE_GROUP_NAME")
    adx_cluster_name = os.environ.get("AZURE_ADX_CLUSTER_NAME")
    assignments = kusto_client.database_principal_assignments.list(
        resource_group_name, adx_cluster_name, database_name
    )
    entity_assignments = [
        assign for assign in assignments if assign.principal_id == principal_id
    ]
    if not entity_assignments:
        return None
    for assign in entity_assignments:
        assign_name: str = str(assign.name).split("/")[-1]
        kusto_client.database_principal_assignments.begin_delete(
            resource_group_name=resource_group_name,
            cluster_name=adx_cluster_name,
            database_name=database_name,
            principal_assignment_name=assign_name,
        ).result()


def main():
    api_instance = client.CustomObjectsApi()
    group = "azure.cosmotech.com"  # Update to the correct API group
    version = "v1"  # Update to the correct API version
    namespace = os.environ.get("NAMESPACE")  # Assuming custom resource is in default namespace
    plural = "adxdatabases"

    # Watch for events on custom resource
    resource_version = ""
    subscription = os.environ.get("AZURE_SUBSCRIPTION")
    credential = ClientSecretCredential(
        client_id=os.environ.get("AZURE_CLIENT_ID"),
        tenant_id=os.environ.get("AZURE_TENANT_ID"),
        client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
    )
    kusto_client = KustoManagementClient(
        credential=credential,
        subscription_id=subscription,
    )
    iam_client = AuthorizationManagementClient(
        credential=credential,
        subscription_id=subscription,
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
                    database_name = f"{orga_id}-{work_key}"
                    location = os.environ.get("AZURE_LOCATION")
                    adx_cluster_name = os.environ.get("AZURE_ADX_CLUSTER_NAME")
                    retention = resource_data.get("retention", 365)

                    # cache period by default 31 days
                    params_database = ReadWriteDatabase(
                        location=location,
                        soft_delete_period=timedelta(days=retention),
                        hot_cache_period=timedelta(days=31),
                    )
                    kusto_client.databases.begin_create_or_update(
                        resource_group_name=resource_group_name,
                        cluster_name=adx_cluster_name,
                        database_name=database_name,
                        parameters=params_database,
                        content_type="application/json",
                    ).result()
                    if resource_data.get("permissions"):
                        for per in resource_data.get("permissions"):
                            delete_permission(
                                principal_id=per.get("principalId"),
                                database_name=database_name,
                            )
                            name = str(uuid4())
                            parameters = DatabasePrincipalAssignment(
                                principal_id=per.get("principalId", ""),
                                principal_type=per.get("principalType", ""),
                                role=per.get("role", ""),
                                tenant_id=os.environ.get("AZURE_TENANT_ID"),
                            )
                            kusto_client.database_principal_assignments.begin_create_or_update(
                                principal_assignment_name=name,
                                cluster_name=adx_cluster_name,
                                resource_group_name=resource_group_name,
                                database_name=database_name,
                                parameters=parameters,
                            ).result()
                            print("permission added")
                    principal_id = os.environ.get("AZURE_ADX_CLUSTER_PRINCIPAL_ID")
                    resource_type = "Microsoft.EventHub/Namespaces"
                    role_id = os.environ.get("AZURE_EVENTHUB_BUILT_DATA_RECEIVER", 'a638d3c7-ab3a-418d-83e6-5f17a39d4fde')
                    prefix = f"/subscriptions/{subscription}"
                    scope = f"{prefix}/resourceGroups/{resource_group_name}/providers/{resource_type}/{orga_id}-{work_key}"
                    role = f"{prefix}/providers/Microsoft.Authorization/roleDefinitions/{role_id}"
                    try:

                        iam_client.role_assignments.create(
                            scope=scope,
                            role_assignment_name=str(uuid4()),
                            parameters=RoleAssignmentCreateParameters(
                                role_definition_id=role,
                                principal_id=principal_id,
                                principal_type="ServicePrincipal",
                            ),
                        )
                    except Exception as e:
                        print(e)

                    principal_id = os.environ.get("AZURE_PLATFORM_PRINCIPAL_ID")
                    resource_type = "Microsoft.EventHub/Namespaces"
                    role_id = os.environ.get("AZURE_EVENTHUB_BUILT_DATA_SENDER", "2b629674-e913-4c01-ae53-ef4638d8f975")
                    prefix = f"/subscriptions/{subscription}"
                    scope = f"{prefix}/resourceGroups/{resource_group_name}/providers/{resource_type}/{orga_id}-{work_key}"
                    role = f"{prefix}/providers/Microsoft.Authorization/roleDefinitions/{role_id}"
                    try:
                        iam_client.role_assignments.create(
                            scope=scope,
                            role_assignment_name=str(uuid4()),
                            parameters=RoleAssignmentCreateParameters(
                                role_definition_id=role,
                                principal_id=principal_id,
                                principal_type="ServicePrincipal",
                            ),
                        )
                    except Exception as e:
                        print(e)

                    database_uri = resource_data.get("uri")
                    kbsc = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                        aad_app_id=os.environ.get("AZURE_CLIENT_ID"),
                        app_key=os.environ.get("AZURE_CLIENT_SECRET"),
                        authority_id=os.environ.get("AZURE_TENANT_ID"),
                        connection_string=database_uri,
                    )
                    kusto_client_new = KustoClient(kcsb=kbsc)
                    batching_policy = json.dumps(
                        {"MaximumBatchingTimeSpan": "00:00:10"}
                    )
                    script_content = f"""
    .execute database script <|
    //
    .alter database ['{database_name}'] policy streamingingestion disable
    //
    .alter-merge database ['{database_name}'] policy retention softdelete = {retention}d
    //
    .alter database ['{database_name}'] policy ingestionbatching '{batching_policy}'
    """
                    ss = kusto_client_new.execute_mgmt(
                        database=database_name, query=script_content
                    )
                    ss.primary_results
                    print("script alter database ran successfully")
                    for sc in resource_data.get("scripts"):
                        try:
                            s = kusto_client_new.execute_mgmt(
                                database=database_name, query=sc.get("content")
                            )
                            s.primary_results
                            print("script ran successfully")
                        except Exception as e:
                            print(e)

                    for cn in resource_data.get("connectors"):
                        eventhub_id = f"/subscriptions/{subscription}/"
                        eventhub_id += f"resourceGroups/{resource_group_name}/"
                        eventhub_id += f"providers/Microsoft.EventHub/namespaces/{orga_id}-{work_key}/"
                        eventhub_id += (
                            f"eventhubs/{cn.get('connectionName', '').lower()}"
                        )
                        managed_id = f"/subscriptions/{subscription}/resourceGroups/{resource_group_name}"
                        managed_id += (
                            f"/providers/Microsoft.Kusto/clusters/{adx_cluster_name}"
                        )
                        random_ = str(uuid4())
                        kusto_client.data_connections.begin_create_or_update(
                            resource_group_name=resource_group_name,
                            cluster_name=adx_cluster_name,
                            database_name=database_name,
                            data_connection_name=f"{orga_id}-{random_[0:3]}-{cn.get('connectionName', '')}".lower(),
                            parameters=EventHubDataConnection(
                                consumer_group=cn.get("consumerGroup", ""),
                                location=os.environ.get("AZURE_LOCATION"),
                                event_hub_resource_id=eventhub_id,
                                data_format=cn.get("format"),
                                compression=str(cn.get("compression", "")),
                                table_name=cn.get("tableName", ""),
                                managed_identity_resource_id=managed_id,
                                mapping_rule_name=cn.get("mapping", ""),
                            ),
                        ).result()

                    try:
                        del resource_data["selector"]
                        custom_resource["spec"]["id"] = database_name
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
                orga_id = get_org_id_by_name(organization_name=organization_name)
                work_key = get_work_key_by_name(workspace_name=workspace_name)
                delete_obj(database_name=f"{orga_id}-{work_key}")
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
