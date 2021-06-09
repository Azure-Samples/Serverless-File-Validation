# Batch Validation

This folder contains sample Python Azure Functions used to validate a batch of .csv files stored in Azure Blob Storage. The path for each file in the batch looks like `{subpath}/{customer}_{date}_{time}_{type}.csv` (e.g. `input_data/cust1_20171010_1112_type1.csv`).

The `process_batch` function is time triggered and gets the list of batches that need to be validated and sends their information to an Azure Queue Storage.
The `validate_batch` function is queue triggered and validates each of the batches that was sent to the queue. The sample performs basic validation on each .csv file: appropriate file encoding, expected number of columns and a check on every field to ensure it's enclosed by double quotes. The batch files are moved into a `valid` or `invalid` folder, depending on the result of the validation process.

## Development details

Code in this folder has been developed with [Visual Studio Code](https://code.visualstudio.com/), using [Azure Functions extension](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions) and [Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) for Visual Studio Code (more details in [Useful links section](#useful-links) below). But you may still use any other editor or IDE of your liking.

Azure Functions and Python are multi-platform, so you should be able to use this code on either Windows, Linux or Mac.

## Configure your environment

See the official documentation for details: [Configure your environment](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-vs-code?pivots=programming-language-python#configure-your-environment).

### Visual Studio Code and Python virtual environment

To be able to run/debug the code, Visual Studio Code settings found in `.vscode` folder depend on the existence of a `.venv` folder containing a Python virtual environment.

While in this folder, you may create such an environment with `python -m venv ./.venv`.

### TLS/SSL error when using PIP on Python virtual environment (Windows)

If virtual environment's pip fails to install packages with an error like the following:

```text
pip is configured with locations that require TLS/SSL, however the ssl module in Python is not available.
```

and you have e.g. [Anaconda](https://www.anaconda.com/) installed, add the following paths to the Windows `Path` environment variable:

```text
<<path_to_your_Anaconda_installation>>\Anaconda3
<<path_to_your_Anaconda_installation>>\Anaconda3\scripts
<<path_to_your_Anaconda_installation>>\Anaconda3\library\bin
```

## Running the project locally

Make sure you have a `local.settings.json` file. See the `local.settings.sample.json` file for what this should look like.

If you are using Visual Studio Code, just run the code with `Run (Ctrl+Shift+D) > Attach to Python Functions`. It will activate the Python virtual environment and import required Python packages from `requirements.txt` file.

If you are in the console, don't forget to activate the Python virtual environment first (using `.venv\scripts\activate` on Windows or `.venv/bin/python` on Linux) and import required Python packages (`pip install -r requirements.txt`). Then run the functions with `func start`.

## Useful links

Azure Functions

- [Work with Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local)
- [Azure Functions Python developer guide](https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python)

Azure Functions with Visual Studio Code

- [Quickstart: Create a Python function in Azure using Visual Studio Code](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-vs-code?pivots=programming-language-python)

Azure Blob Storage

- [Quickstart: Manage blobs with Python v12 SDK](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python)
- [Azure Storage Blobs client library for Python - Version 12.3.0](https://docs.microsoft.com/en-us/azure/developer/python/sdk/storage/storage-blob-readme?view=storage-py-v12)
- [Azure Storage Blob client library for Python Samples](https://docs.microsoft.com/en-us/samples/azure/azure-sdk-for-python/storage-blob-samples/)
