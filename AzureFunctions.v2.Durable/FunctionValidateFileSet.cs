using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;

namespace FileValidation
{
    public static class FunctionValidateFileSet
    {
        [FunctionName(@"ValidateFileSet")]
#if FUNCTIONS_V1
        public static async Task<bool> Run([ActivityTrigger]DurableActivityContext context, ILogger log)
#else
        public static async Task<bool> Run([ActivityTrigger]IDurableActivityContext context, ILogger log)
#endif
        {
            log.LogTrace(@"ValidateFileSet run.");
            if (!CloudStorageAccount.TryParse(Environment.GetEnvironmentVariable(@"CustomerBlobStorage"), out _))
            {
                throw new Exception(@"Can't create a storage account accessor from app setting connection string, sorry!");
            }

            var payload = context.GetInputAsJson();
            var prefix = payload["prefix"].ToString(); // This is the entire path w/ prefix for the file set

            return await Helpers.DoValidationAsync(prefix, log);
        }
    }
}
