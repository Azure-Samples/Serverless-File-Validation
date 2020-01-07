using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace FileValidation
{
    public static class FunctionEnsureAllFiles
    {
        [FunctionName("EnsureAllFiles")]
#if FUNCTIONS_V1
        public static async Task Run([OrchestrationTrigger]DurableOrchestrationContext context, ILogger log)
#else
        public static async Task Run([OrchestrationTrigger]IDurableOrchestrationContext context, ILogger log)
#endif
        {
            if (!context.IsReplaying)
            {
                context.Log(log, $@"EnsureAllFiles STARTING - InstanceId: {context.InstanceId}");
            }
            else
            {
                context.Log(log, $@"EnsureAllFiles REPLAYING");
            }

            dynamic eventGridSoleItem = context.GetInputAsJson();

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

                context.Log(log, $@"Still waiting for more files... Still need {string.Join(", ", filesStillWaitingFor)} for customer {newCustomerFile.CustomerName}, batch {newCustomerFile.BatchPrefix}");

                filename = await context.WaitForExternalEvent<string>(@"newfile");
                context.Log(log, $@"Got new file via event: {filename}");
            }

            // Verify that this prefix isn't already in the lock table for processings
            context.Log(log, @"Got all the files! Moving on...");

            // call next step in functions with the prefix so it knows what to go grab
            await context.CallActivityAsync(@"ValidateFileSet", new { prefix = $@"{newCustomerFile.ContainerName}/inbound/{newCustomerFile.BatchPrefix}", fileTypes = expectedFiles });
        }

        class BlobFilenameVsDatabaseFileMaskComparer : IEqualityComparer<string>
        {
            public bool Equals(string x, string y) => y.Contains(x);

            public int GetHashCode(string obj) => obj.GetHashCode();
        }
    }
}
