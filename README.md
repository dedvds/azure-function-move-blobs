# azure-function-move-blobs

This repo contains the material to create an Azure Function that is triggered by an Event Grid. The function does the following:
- Checks if two files with a similar name are there (png + txt)
- if so:  copies the data to a processed folder and deletes the raw files

Authentication is done via Managed Identity: Azure Function MSI should be enabled and granted at least Storage Blob Data Contributor role on the storage account.

