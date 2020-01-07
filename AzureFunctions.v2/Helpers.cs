using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace FileValidation
{
    static class Helpers
    {
        public static async System.Threading.Tasks.Task<CloudTable> GetLockTableAsync(CloudStorageAccount storageAccount = null)
        {
            CloudTable customerFilesTable;
            if (storageAccount == null)
            {
                if (!CloudStorageAccount.TryParse(Environment.GetEnvironmentVariable(@"AzureWebJobsStorage"), out var sa))
                {
                    throw new Exception(@"Can't create a storage account accessor from app setting connection string, sorry!");
                }
                else
                {
                    storageAccount = sa;
                }
            }

            try
            {
                customerFilesTable = storageAccount.CreateCloudTableClient().GetTableReference(@"FileProcessingLocks");
            }
            catch (Exception ex)
            {
                throw new Exception($@"Error creating table client for locks: {ex}", ex);
            }

            while (true)
            {
                try
                {
                    await customerFilesTable.CreateIfNotExistsAsync();
                    break;
                }
                catch { }
            }

            return customerFilesTable;
        }

        public static CustomerBlobAttributes ParseEventGridPayload(dynamic eventGridItem, ILogger log)
        {
            if (eventGridItem.eventType == @"Microsoft.Storage.BlobCreated"
                && eventGridItem.data.api == @"PutBlob"
                && eventGridItem.data.contentType == @"text/csv")
            {
                try
                {
                    var retVal = CustomerBlobAttributes.Parse((string)eventGridItem.data.url);
                    if (retVal != null && !retVal.ContainerName.Equals(retVal.CustomerName))
                    {
                        throw new ArgumentException($@"File '{retVal.Filename}' uploaded to container '{retVal.ContainerName}' doesn't have the right prefix: the first token in the filename ({retVal.CustomerName}) must be the customer name, which should match the container name", nameof(eventGridItem));
                    }

                    return retVal;
                }
                catch (Exception ex)
                {
                    log.LogError(@"Error parsing Event Grid payload", ex);
                }
            }

            return null;
        }

        public static IEnumerable<string> GetExpectedFilesForCustomer() => new[] { @"type1", @"type2", @"type3", @"type4", @"type5", @"type7", @"type8", @"type9", @"type10" };

        public static async Task<bool> DoValidationAsync(string prefix, ILogger logger = null)
        {
            logger?.LogTrace(@"ValidateFileSet run.");
            if (!CloudStorageAccount.TryParse(Environment.GetEnvironmentVariable(@"CustomerBlobStorage"), out var storageAccount))
            {
                throw new Exception(@"Can't create a storage account accessor from app setting connection string, sorry!");
            }

            logger?.LogTrace($@"prefix: {prefix}");

            var filePrefix = prefix.Substring(prefix.LastIndexOf('/') + 1);
            logger?.LogTrace($@"filePrefix: {filePrefix}");

            var blobClient = storageAccount.CreateCloudBlobClient();
            var targetBlobs = await blobClient.ListBlobsAsync(WebUtility.UrlDecode(prefix));
            var customerName = filePrefix.Split('_').First().Split('-').Last();

            var errors = new List<string>();
            var expectedFiles = Helpers.GetExpectedFilesForCustomer();

            foreach (var blobDetails in targetBlobs)
            {
                var blob = await blobClient.GetBlobReferenceFromServerAsync(blobDetails.StorageUri.PrimaryUri);

                var fileParts = CustomerBlobAttributes.Parse(blob.Uri.AbsolutePath);
                if (!expectedFiles.Contains(fileParts.Filetype, StringComparer.OrdinalIgnoreCase))
                {
                    logger?.LogTrace($@"{blob.Name} skipped. Isn't in the list of file types to process ({string.Join(", ", expectedFiles)}) for customer '{customerName}'");
                    continue;
                }

                var lowerFileType = fileParts.Filetype.ToLowerInvariant();
                uint numColumns = 0;
                switch (lowerFileType)
                {
                    case @"type5":  // salestype
                        numColumns = 2;
                        break;
                    case @"type10": // mixed
                    case @"type4":  // shipfrom
                        numColumns = 3;
                        break;
                    case @"type1":  // channel
                    case @"type2":  // customer
                        numColumns = 4;
                        break;
                    case @"type9":  // itemdetail
                        numColumns = 5;
                        break;
                    case @"type3": // shipto
                        numColumns = 14;
                        break;
                    case @"type6": // salesdetail
                        numColumns = 15;
                        break;
                    case @"type8":  // product
                        numColumns = 21;
                        break;
                    case @"type7":  // sales
                        numColumns = 23;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(prefix), $@"Unhandled file type: {fileParts.Filetype}");
                }

                errors.AddRange(await ValidateCsvStructureAsync(blob, numColumns, lowerFileType));
            }

            if (errors.Any())
            {
                logger.LogError($@"Errors found in batch {filePrefix}: {string.Join(@", ", errors)}");

                // move files to 'invalid-set' folder
                await Helpers.MoveBlobsAsync(blobClient, targetBlobs, @"invalid-set", logger);
                return false;
            }
            else
            {
                // move these files to 'valid-set' folder
                await Helpers.MoveBlobsAsync(blobClient, targetBlobs, @"valid-set", logger);

                logger.LogInformation($@"Set {filePrefix} successfully validated and queued for further processing.");
                return true;
            }
        }

        private static async Task<IEnumerable<string>> ValidateCsvStructureAsync(ICloudBlob blob, uint requiredNumberOfColumnsPerLine, string filetypeDescription)
        {
            var errs = new List<string>();
            try
            {
                using (var blobReader = new StreamReader(await blob.OpenReadAsync(new AccessCondition(), new BlobRequestOptions(), new OperationContext())))
                {
                    var fileAttributes = CustomerBlobAttributes.Parse(blob.Uri.AbsolutePath);

                    for (var lineNumber = 0; !blobReader.EndOfStream; lineNumber++)
                    {
                        var errorPrefix = $@"{filetypeDescription} file '{fileAttributes.Filename}' Record {lineNumber}";
                        var line = blobReader.ReadLine();
                        var fields = line.Split(',');
                        if (fields.Length != requiredNumberOfColumnsPerLine)
                        {
                            errs.Add($@"{errorPrefix} is malformed. Should have {requiredNumberOfColumnsPerLine} values; has {fields.Length}");
                            continue;
                        }

                        for (var i = 0; i < fields.Length; i++)
                        {
                            errorPrefix = $@"{errorPrefix} Field {i}";
                            var field = fields[i];
                            // each field must be enclosed in double quotes
                            if (field[0] != '"' || field.Last() != '"')
                            {
                                errs.Add($@"{errorPrefix}: value ({field}) is not enclosed in double quotes ("")");
                                continue;
                            }
                        }
                    }

                    // Validate file is UTF-8 encoded
                    if (!blobReader.CurrentEncoding.BodyName.Equals("utf-8", StringComparison.OrdinalIgnoreCase))
                    {
                        errs.Add($@"{blob.Name} is not UTF-8 encoded");
                    }
                }
            }
            catch (StorageException storEx)
            {
                SwallowStorage404(storEx);
            }
            return errs;
        }

        private static void SwallowStorage404(StorageException storEx)
        {
            var webEx = storEx.InnerException as WebException;
            if ((webEx.Response as HttpWebResponse)?.StatusCode == HttpStatusCode.NotFound)
            {
                // Ignore
            }
            else
            {
                throw storEx;
            }
        }

        public static async Task MoveBlobsAsync(CloudBlobClient blobClient, IEnumerable<IListBlobItem> targetBlobs, string folderName, ILogger logger = null)
        {
            foreach (var b in targetBlobs)
            {
                var blobRef = await blobClient.GetBlobReferenceFromServerAsync(b.StorageUri.PrimaryUri);
                var sourceBlob = b.Container.GetBlockBlobReference(blobRef.Name);

                var targetBlob = blobRef.Container
                    .GetDirectoryReference($@"{folderName}")
                    .GetBlockBlobReference(Path.GetFileName(blobRef.Name));

                string sourceLeaseGuid = Guid.NewGuid().ToString(), targetLeaseGuid = Guid.NewGuid().ToString();
                var sourceLeaseId = await sourceBlob.AcquireLeaseAsync(TimeSpan.FromSeconds(60), sourceLeaseGuid);

                await targetBlob.StartCopyAsync(sourceBlob);

                while (targetBlob.CopyState.Status == CopyStatus.Pending)
                {
                    ;     // spinlock until the copy completes
                }

                var copySucceeded = targetBlob.CopyState.Status == CopyStatus.Success;
                if (!copySucceeded)
                {
                    logger?.LogError($@"Error copying {sourceBlob.Name} to {folderName} folder. Retrying once...");

                    await targetBlob.StartCopyAsync(sourceBlob);

                    while (targetBlob.CopyState.Status == CopyStatus.Pending)
                    {
                        ;     // spinlock until the copy completes
                    }

                    copySucceeded = targetBlob.CopyState.Status == CopyStatus.Success;
                    if (!copySucceeded)
                    {
                        logger?.LogError($@"Error retrying copy of {sourceBlob.Name} to {folderName} folder. File not moved.");
                    }
                }

                if (copySucceeded)
                {
#if DEBUG
                    try
                    {
#endif
                        await sourceBlob.ReleaseLeaseAsync(new AccessCondition { LeaseId = sourceLeaseId });
                        await sourceBlob.DeleteAsync();
#if DEBUG
                    }
                    catch (StorageException ex)
                    {
                        logger?.LogError($@"Error deleting blob {sourceBlob.Name}", ex);
                    }
#endif

                }
            }
        }
    }
}