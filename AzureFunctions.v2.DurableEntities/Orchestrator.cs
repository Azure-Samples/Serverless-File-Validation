using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;

namespace FileValidation
{
    public static class Orchestrator
    {
        [FunctionName("Orchestrator")]
        public static async System.Threading.Tasks.Task<HttpResponseMessage> RunAsync([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]HttpRequestMessage req, [DurableClient]IDurableClient starter, ILogger log)
        {
            var inputToFunction = JToken.ReadFrom(new JsonTextReader(new StreamReader(await req.Content.ReadAsStreamAsync())));
            dynamic eventGridSoleItem = (inputToFunction as JArray)?.SingleOrDefault();
            if (eventGridSoleItem == null)
            {
                return req.CreateResponse(HttpStatusCode.BadRequest, @"Expecting only one item in the Event Grid message");
            }

            if (eventGridSoleItem.eventType == @"Microsoft.EventGrid.SubscriptionValidationEvent")
            {
                log.LogTrace(@"Event Grid Validation event received.");
                return req.CreateCompatibleResponse(HttpStatusCode.OK, $"{{ \"validationResponse\" : \"{((dynamic)inputToFunction)[0].data.validationCode}\" }}");
            }

            CustomerBlobAttributes newCustomerFile = Helpers.ParseEventGridPayload(eventGridSoleItem, log);
            if (newCustomerFile == null)
            {   // The request either wasn't valid (filename couldn't be parsed) or not applicable (put in to a folder other than /inbound)
                return req.CreateResponse(HttpStatusCode.NoContent);
            }

            string customerName = newCustomerFile.CustomerName, name = newCustomerFile.Filename, containerName = newCustomerFile.ContainerName;
            log.LogInformation($@"Processing new file. customer: {customerName}, filename: {name}");

            // get the prefix for the name so we can check for others in the same container with in the customer blob storage account
            var prefix = newCustomerFile.BatchPrefix;
            await starter.SignalEntityAsync<IBatchEntity>(prefix, b => b.NewFile(newCustomerFile.FullUrl));

            return req.CreateResponse(HttpStatusCode.Accepted);

        }
    }
}