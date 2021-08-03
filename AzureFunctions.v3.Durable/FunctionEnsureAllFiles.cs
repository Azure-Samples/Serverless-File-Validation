using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Azure.Messaging.EventGrid;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace FileValidation
{
    public static class FunctionEnsureAllFiles
    {
        [FunctionName("EnsureAllFiles")]
        public static async Task Run([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            if (!context.IsReplaying)
            {
                log.LogTrace($@"EnsureAllFiles STARTING - InstanceId: {context.InstanceId}");
            }
            else
            {
                log.LogTrace($@"EnsureAllFiles REPLAYING");
            }

            var eventGridSoleItem = context.GetInput<EventGridEvent>();

            CustomerBlobAttributes newCustomerFile = Helpers.ParseEventGridPayload(eventGridSoleItem, log);
            if (newCustomerFile == null)
            {   // The request either wasn't valid (filename couldn't be parsed) or not applicable (put in to a folder other than /inbound)
                return;
            }

            var expectedFiles = Helpers.GetExpectedFilesForCustomer();
            var filesStillWaitingFor = new HashSet<string>(expectedFiles);
            var filename = newCustomerFile.Filename;

            while (filesStillWaitingFor.Any())
            {
                filesStillWaitingFor.Remove(Path.GetFileNameWithoutExtension(filename).Split('_').Last());
                if (filesStillWaitingFor.Count == 0)
                {
                    break;
                }

                log.LogTrace($@"Still waiting for more files... Still need {string.Join(", ", filesStillWaitingFor)} for customer {newCustomerFile.CustomerName}, batch {newCustomerFile.BatchPrefix}");

                filename = await context.WaitForExternalEvent<string>(@"newfile");
                log.LogTrace($@"Got new file via event: {filename}");
            }

            // Verify that this prefix isn't already in the lock table for processings
            log.LogInformation(@"Got all the files! Moving on...");

            // call next step in functions with the prefix so it knows what to go grab
            await context.CallActivityAsync(@"ValidateFileSet", new FilesetValidationRequest
            {
                Prefix = $@"{newCustomerFile.ContainerName}/inbound/{newCustomerFile.BatchPrefix}",
                ExpectedFiles = expectedFiles
            });
        }

        class BlobFilenameVsDatabaseFileMaskComparer : IEqualityComparer<string>
        {
            public bool Equals(string x, string y) => y.Contains(x);

            public int GetHashCode(string obj) => obj.GetHashCode();
        }
    }
}
