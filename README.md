# Spark SQL Homework

## 0. Prerequisites
- Terraform
- Azure account
- Azure CLI
- Databricks CLI

## 1. Setup infrastructure via Terraform:
```
az login
cd ./terraform

terraform init
terraform plan -out ./state/terraform.plan
terraform apply ./state/terraform.plan

cd ../

# Destroy all necessary infrastructure after completing the homework:
terraform destroy
```
* **IMPORTANT:** Do not forget to add Role Assignment "Storage Blob Data Contributor" to the application registration
in your storage account. (**TODO:** Automate this step with Terraform)

## 2. Create databricks secrets:
* Provide all necessary credentials:
```
cp ./config/storage-creds.ini.sample ./config/storage-creds.ini  # Fill credentials inside the copied file
```
* Put databricks secret:
```
# Configure databricks access via token:
databricks configure --token

databricks secrets create-scope --initial-manage-principal users --scope abfs-access
databricks secrets put --scope abfs-access --key storage-creds --binary-file ./config/storage-creds.ini
```

## 3. Check you Databricks workspace
* Everything is ready for launching the notebook
* Example of the output datasets is inside media folder

## 4. TODO:
* Unit-tests
* Optimization based on the execution plans analysis
