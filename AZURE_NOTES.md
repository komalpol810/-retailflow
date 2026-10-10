# Azure phase notes

## Architecture
- Storage: ADLS Gen2 account `retailflowstorage` (containers: raw, bronze, silver, gold)
- Transform: Azure Databricks notebooks 00 to 05 (Bronze, Silver, Gold Delta tables)
- Orchestration: Azure Data Factory pipeline `pl_retailflow_gold`, daily 6 AM trigger `tr_daily_6am`, failure alert via Azure Monitor
- Serving: Synapse serverless SQL, database `retailflow_gold`, 7 views in schema `gold` over the Gold Delta tables

## Verification
- fct_order_items 112650 rows, dim_customer 96355, dim_product 32951, dim_seller 3095, dim_date 1461, dim_geography 19015, dim_order_status 8
- Revenue by state: SP first (47433 items, 5200992.91), then RJ, MG, RS, PR

## Gotchas hit
- MissingSubscriptionRegistration: resource providers must be registered
- AZURE_QUOTA_EXCEEDED: VM family quota was 0, switched node type
- SqlServerRegionDoesNotAllowProvisioning: Synapse workspace failed in East US, redeployed in another region
- Storage access needs RBAC (Storage Blob Data Reader), separate from subscription Owner
- Fact table key is customer_geography_key, dimension key is geography_key (role-playing key naming)
