using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace FileValidation
{
    [JsonObject(MemberSerialization.OptIn)]
    public class BatchEntity : IBatchEntity
    {
        private readonly string _id;
        private readonly ILogger _logger;

        public BatchEntity(string id, ILogger logger)
        {
            _id = id;
            _logger = logger;
        }

        [JsonProperty]
        public List<string> ReceivedFileTypes { get; set; } = new List<string>();

        [FunctionName(nameof(BatchEntity))]
        public static Task Run([EntityTrigger]IDurableEntityContext ctx, ILogger logger) => ctx.DispatchAsync<BatchEntity>(ctx.EntityKey, logger);

        public async Task NewFile(string fileUri)
        {
            var newCustomerFile = CustomerBlobAttributes.Parse(fileUri);
            _logger.LogInformation($@"Got new file via event: {newCustomerFile.Filename}");
            this.ReceivedFileTypes.Add(newCustomerFile.Filetype);

            _logger.LogTrace($@"Actor '{_id}' got file '{newCustomerFile.Filetype}'");

            var filesStillWaitingFor = Helpers.GetExpectedFilesForCustomer().Except(this.ReceivedFileTypes);
            if (filesStillWaitingFor.Any())
            {
                _logger.LogInformation($@"Still waiting for more files... Still need {string.Join(", ", filesStillWaitingFor)} for customer {newCustomerFile.CustomerName}, batch {newCustomerFile.BatchPrefix}");
            }
            else
            {
                _logger.LogInformation(@"Got all the files! Moving on...");

                // call next step in functions with the prefix so it knows what to go grab
                await Helpers.DoValidationAsync($@"{newCustomerFile.ContainerName}/inbound/{newCustomerFile.BatchPrefix}", _logger);
            }
        }
    }

    public interface IBatchEntity
    {
        Task NewFile(string fileUri);
    }

}
