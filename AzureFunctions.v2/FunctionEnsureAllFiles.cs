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
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FileValidation
{
    public static class FunctionEnsureAllFiles
    {
        [FunctionName("EnsureAllFiles")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, @"post")]HttpRequestMessage req, ILogger log)
        {
            var payloadFromEventGrid = JToken.ReadFrom(new JsonTextReader(new StreamReader(await req.Content.ReadAsStreamAsync())));
            dynamic eventGridSoleItem = (payloadFromEventGrid as JArray)?.SingleOrDefault();
            if (eventGridSoleItem == null)
            {
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, $@"Expecting only one item in the Event Grid message");
            }

            if (eventGridSoleItem.eventType == @"Microsoft.EventGrid.SubscriptionValidationEvent")
            {
                log.LogTrace(@"Event Grid Validation event received.");
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent($"{{ \"validationResponse\" : \"{((dynamic)payloadFromEventGrid)[0].data.validationCode}\" }}")
                };
            }

            var newCustomerFile = Helpers.ParseEventGridPayload(eventGridSoleItem, log);
            if (newCustomerFile == null)
            {   // The request either wasn't valid (filename couldn't be parsed) or not applicable (put in to a folder other than /inbound)
                return req.CreateResponse(System.Net.HttpStatusCode.NoContent);
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
                    return req.CreateResponse(HttpStatusCode.NoContent);
                }

                log.LogInformation(@"Got all the files! Moving on...");
                try
                {
                    await lockTable.ExecuteAsync(TableOperation.Insert(new LockTableEntity(prefix)));
                }
                catch (StorageException)
                {
                    log.LogInformation($@"Skipping. We've already queued the batch with prefix '{prefix}' for processing");
                    return req.CreateResponse(HttpStatusCode.NoContent);
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

                    return req.CreateResponse(HttpStatusCode.OK);
                }
            }
            else
            {
                log.LogInformation($@"Still waiting for more files... Have {matches.Count()} file(s) from this customer ({newCustomerFile.CustomerName}) for batch {newCustomerFile.BatchPrefix}. Still need {string.Join(", ", filesStillWaitingFor)}");

                return req.CreateResponse(HttpStatusCode.NoContent);
            }
        }

        class BlobFilenameVsDatabaseFileMaskComparer : IEqualityComparer<string>
        {
            public bool Equals(string x, string y) => y.Contains(x);

            public int GetHashCode(string obj) => obj.GetHashCode();
        }
    }
}
