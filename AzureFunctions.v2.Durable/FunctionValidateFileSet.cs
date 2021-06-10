using System;
using System.Collections.Generic;
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
        public static async Task<bool> Run([ActivityTrigger] FilesetValidationRequest payload, ILogger log)
        {
            log.LogTrace(@"ValidateFileSet run.");
            if (!CloudStorageAccount.TryParse(Environment.GetEnvironmentVariable(@"CustomerBlobStorage"), out _))
            {
                throw new Exception(@"Can't create a storage account accessor from app setting connection string, sorry!");
            }

            var prefix = payload.Prefix; // This is the entire path w/ prefix for the file set

            return await Helpers.DoValidationAsync(prefix, log);
        }

    }

    public class FilesetValidationRequest
    {
        public string Prefix { get; set; }

        public IEnumerable<string> ExpectedFiles { get; set; }
    }

}
