# Azure phase

- `adf/`: ARM template export of the Azure Data Factory (pipeline `pl_retailflow_gold`, linked service, daily trigger).
- Databricks notebooks for Bronze, Silver and Gold are in the repo where I pushed them earlier.

ADF authenticates with a managed identity, and Databricks reads the storage key from a secret scope. No secrets are stored in this repo.
