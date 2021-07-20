using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Messaging.EventGrid;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace FileValidation
{
    public static class FunctionEnsureAllFiles
    {
        [FunctionName("EnsureAllFiles")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, @"post")] HttpRequestMessage req, ILogger log)
        {
            var events = await req.Content.ReadAsAsync<EventGridEvent[]>();
            var eventGridSoleItem = events?.SingleOrDefault();
            if (eventGridSoleItem == null)
            {
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, $@"Expecting only one item in the Event Grid message");
            }

            if (eventGridSoleItem.EventType == @"Microsoft.EventGrid.SubscriptionValidationEvent")
            {
                log.LogTrace(@"Event Grid Validation event received.");
                return req.CreateCompatibleResponse(HttpStatusCode.OK, $"{{ \"validationResponse\" : \"{((dynamic)eventGridSoleItem.Data).validationCode}\" }}");
            }

            var newCustomerFile = Helpers.ParseEventGridPayload(eventGridSoleItem, log);
            if (newCustomerFile == null)
            {   // The request either wasn't valid (filename couldn't be parsed) or not applicable (put in to a folder other than /inbound)
                return req.CreateCompatibleResponse(HttpStatusCode.NoContent);
            }

            // get the prefix for the name so we can check for others in the same container with in the customer blob storage account
            var prefix = newCustomerFile.BatchPrefix;

            if (!CloudStorageAccount.TryParse(Environment.GetEnvironmentVariable(@"CustomerBlobStorage"), out var blobStorage))
            {
                throw new Exception(@"Can't create a storage account accessor from app setting connection string, sorry!");
            }

            var blobClient = blobStorage.CreateCloudBlobClient();
            var matches = await blobClient.ListBlobsAsync(prefix: $@"{newCustomerFile.ContainerName}/inbound/{prefix}");
            var matchNames = matches.Select(m => Path.GetFileNameWithoutExtension(blobClient.GetBlobReferenceFromServerAsync(m.StorageUri.PrimaryUri).GetAwaiter().GetResult().Name).Split('_').Last()).ToList();

            var expectedFiles = Helpers.GetExpectedFilesForCustomer();
            var filesStillWaitingFor = expectedFiles.Except(matchNames, new BlobFilenameVsDatabaseFileMaskComparer());

            if (!filesStillWaitingFor.Any())
            {
                // Verify that this prefix isn't already in the lock table for processings
                var lockTable = await Helpers.GetLockTableAsync();
                var entriesMatchingPrefix = await LockTableEntity.GetLockRecordAsync(prefix, lockTable);
                if (entriesMatchingPrefix != null)
                {
                    log.LogInformation($@"Skipping. We've already queued the batch with prefix '{prefix}' for processing");
                    return req.CreateCompatibleResponse(HttpStatusCode.NoContent);
                }

                log.LogInformation(@"Got all the files! Moving on...");
                try
                {
                    await lockTable.ExecuteAsync(TableOperation.Insert(new LockTableEntity(prefix)));
                }
                catch (StorageException)
                {
                    log.LogInformation($@"Skipping. We've already queued the batch with prefix '{prefix}' for processing");
                    return req.CreateCompatibleResponse(HttpStatusCode.NoContent);
                }

                using (var c = new HttpClient())
                {
                    var jsonObjectForValidator =
$@"{{
    ""prefix"" : ""{newCustomerFile.ContainerName}/inbound/{prefix}"",
    ""fileTypes"" : [
        {string.Join(", ", expectedFiles.Select(e => $@"""{e}"""))}
    ]
}}";
                    // call next step in functions with the prefix so it knows what to go grab
                    await c.PostAsync($@"{Environment.GetEnvironmentVariable(@"ValidateFunctionUrl")}", new StringContent(jsonObjectForValidator));

                    return req.CreateCompatibleResponse(HttpStatusCode.OK);
                }
            }
            else
            {
                log.LogInformation($@"Still waiting for more files... Have {matches.Count()} file(s) from this customer ({newCustomerFile.CustomerName}) for batch {newCustomerFile.BatchPrefix}. Still need {string.Join(", ", filesStillWaitingFor)}");

                return req.CreateCompatibleResponse(HttpStatusCode.Accepted);
            }
        }

        class BlobFilenameVsDatabaseFileMaskComparer : IEqualityComparer<string>
        {
            public bool Equals(string x, string y) => y.Contains(x);

            public int GetHashCode(string obj) => obj.GetHashCode();
        }
    }
}
