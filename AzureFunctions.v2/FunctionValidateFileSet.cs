using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;

namespace FileValidation
{
    public static class FunctionValidateFileSet
    {
        [FunctionName(@"ValidateFileSet")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, @"post", Route = @"Validate")]HttpRequestMessage req, ILogger log)
        {
            log.LogTrace(@"ValidateFileSet run.");
            if (!CloudStorageAccount.TryParse(Environment.GetEnvironmentVariable(@"CustomerBlobStorage"), out var storageAccount))
            {
                throw new Exception(@"Can't create a storage account accessor from app setting connection string, sorry!");
            }

            var payload = JObject.Parse(await req.Content.ReadAsStringAsync());

            var prefix = payload["prefix"].ToString(); // This is the entire path w/ prefix for the file set
            log.LogTrace($@"prefix: {prefix}");

            var filePrefix = prefix.Substring(prefix.LastIndexOf('/') + 1);
            log.LogTrace($@"filePrefix: {filePrefix}");

            var lockTable = await Helpers.GetLockTableAsync();
            if (!await ShouldProceedAsync(lockTable, prefix, filePrefix, log))
            {
                return req.CreateResponse(HttpStatusCode.OK);
            }

            var blobClient = storageAccount.CreateCloudBlobClient();
            var targetBlobs = await blobClient.ListBlobsAsync(WebUtility.UrlDecode(prefix));

            var customerName = filePrefix.Split('_').First().Split('-').Last();

            var errors = new List<string>();
            var filesToProcess = payload["fileTypes"].Values<string>();

            foreach (var blobDetails in targetBlobs)
            {
                var blob = await blobClient.GetBlobReferenceFromServerAsync(blobDetails.StorageUri.PrimaryUri);

                var fileParts = CustomerBlobAttributes.Parse(blob.Uri.AbsolutePath);
                if (!filesToProcess.Contains(fileParts.Filetype, StringComparer.OrdinalIgnoreCase))
                {
                    log.LogTrace($@"{blob.Name} skipped. Isn't in the list of file types to process ({string.Join(", ", filesToProcess)}) for bottler '{customerName}'");
                    continue;
                }

                var lowerFileType = fileParts.Filetype.ToLowerInvariant();
                log.LogInformation($@"Validating {lowerFileType}...");

                uint numColumns = 0;
                switch (lowerFileType)
                {
                    case @"type5":  // salestype
                        numColumns = 2;
                        break;
                    case @"type10": // mixedpack
                    case @"type4":  // shipfrom
                        numColumns = 3;
                        break;
                    case @"type1":  // channel
                    case @"type2":  // customer
                        numColumns = 4;
                        break;
                    case @"type9":  // itemdetail
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
            try
            {
                await LockTableEntity.UpdateAsync(filePrefix, LockTableEntity.BatchState.Done, lockTable);
            }
            catch (StorageException)
            {
                log.LogWarning($@"That's weird. The lock for prefix {prefix} wasn't there. Shouldn't happen!");
                return req.CreateResponse(HttpStatusCode.OK);
            }

            if (errors.Any())
            {
                log.LogError($@"Errors found in batch {filePrefix}: {string.Join(@", ", errors)}");

                // move files to 'invalid-set' folder
                await MoveBlobsAsync(log, blobClient, targetBlobs, @"invalid-set");

                return req.CreateErrorResponse(HttpStatusCode.BadRequest, string.Join(@", ", errors));
            }
            else
            {
                // move these files to 'valid-set' folder
                await MoveBlobsAsync(log, blobClient, targetBlobs, @"valid-set");

                log.LogInformation($@"Set {filePrefix} successfully validated and queued for further processing.");

                return req.CreateResponse(HttpStatusCode.OK);
            }
        }

        private static async Task<bool> ShouldProceedAsync(CloudTable bottlerFilesTable, string prefix, string filePrefix, ILogger log)
        {
            try
            {
                var lockRecord = await LockTableEntity.GetLockRecordAsync(filePrefix, bottlerFilesTable);
                if (lockRecord?.State == LockTableEntity.BatchState.Waiting)
                {
                    // Update the lock record to mark it as in progress
                    lockRecord.State = LockTableEntity.BatchState.InProgress;
                    await bottlerFilesTable.ExecuteAsync(TableOperation.Replace(lockRecord));
                    return true;
                }
                else
                {
                    log.LogInformation($@"Validate for {prefix} skipped. State was {lockRecord?.State.ToString() ?? @"[null]"}.");
                }
            }
            catch (StorageException)
            {
                log.LogInformation($@"Validate for {prefix} skipped (StorageException. Somebody else picked it up already.");
            }

            return false;
        }

        private static async Task MoveBlobsAsync(ILogger log, CloudBlobClient blobClient, IEnumerable<IListBlobItem> targetBlobs, string folderName)
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
                var targetLeaseId = await targetBlob.AcquireLeaseAsync(TimeSpan.FromSeconds(60), targetLeaseGuid);

                await targetBlob.StartCopyAsync(sourceBlob);

                while (targetBlob.CopyState.Status == CopyStatus.Pending)
                {
                    ;     // spinlock until the copy completes
                }

                var copySucceeded = targetBlob.CopyState.Status == CopyStatus.Success;
                if (!copySucceeded)
                {
                    log.LogError($@"Error copying {sourceBlob.Name} to {folderName} folder. Retrying once...");

                    await targetBlob.StartCopyAsync(sourceBlob);

                    while (targetBlob.CopyState.Status == CopyStatus.Pending)
                    {
                        ;     // spinlock until the copy completes
                    }

                    copySucceeded = targetBlob.CopyState.Status == CopyStatus.Success;
                    if (!copySucceeded)
                    {
                        log.LogError($@"Error retrying copy of {sourceBlob.Name} to {folderName} folder. File not moved.");
                    }
                }

                if (copySucceeded)
                {
#if DEBUG
                    try
                    {
#endif
                        await sourceBlob.DeleteAsync();
#if DEBUG
                    }
                    catch (StorageException ex)
                    {
                        log.LogError($@"Error deleting blob {sourceBlob.Name}", ex);
                    }
#endif

                    await targetBlob.ReleaseLeaseAsync(new AccessCondition { LeaseId = targetLeaseId });
                    await sourceBlob.ReleaseLeaseAsync(new AccessCondition { LeaseId = sourceLeaseId });
                }
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
    }
}
