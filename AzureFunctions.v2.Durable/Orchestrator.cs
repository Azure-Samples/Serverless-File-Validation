using System.Linq;
using System.Net;
using System.Net.Http;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace FileValidation
{
    public static class Orchestrator
    {
        [FunctionName("Orchestrator")]
        public static async System.Threading.Tasks.Task<HttpResponseMessage> RunAsync([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequestMessage req, [DurableClient] IDurableClient starter, ILogger log)
        {
            var events = await req.Content.ReadAsAsync<EventGridEvent[]>();
            var eventGridSoleItem = events?.SingleOrDefault();
            if (eventGridSoleItem == null)
            {
                return req.CreateCompatibleResponse(HttpStatusCode.BadRequest, @"Expecting only one item in the Event Grid message");
            }

            if (eventGridSoleItem.EventType == @"Microsoft.EventGrid.SubscriptionValidationEvent")
            {
                log.LogTrace(@"Event Grid Validation event received.");
                return req.CreateCompatibleResponse(HttpStatusCode.OK, $"{{ \"validationResponse\" : \"{((dynamic)eventGridSoleItem.Data).validationCode}\" }}");
            }

            CustomerBlobAttributes newCustomerFile = Helpers.ParseEventGridPayload(eventGridSoleItem, log);
            if (newCustomerFile == null)
            {   // The request either wasn't valid (filename couldn't be parsed) or not applicable (put in to a folder other than /inbound)
                return req.CreateCompatibleResponse(HttpStatusCode.NoContent);
            }

            string customerName = newCustomerFile.CustomerName, name = newCustomerFile.Filename;
            log.LogInformation($@"Processing new file. customer: {customerName}, filename: {name}");

            // get the prefix for the name so we can check for others in the same container with in the customer blob storage account
            var prefix = newCustomerFile.BatchPrefix;

            var instanceForPrefix = await starter.GetStatusAsync(prefix);
            if (instanceForPrefix == null)
            {
                log.LogTrace($@"New instance needed for prefix '{prefix}'. Starting...");
                var retval = await starter.StartNewAsync(@"EnsureAllFiles", prefix, eventGridSoleItem);
                log.LogTrace($@"Started. {retval}");
            }
            else
            {
                log.LogTrace($@"Instance already waiting. Current status: {instanceForPrefix.RuntimeStatus}. Firing 'newfile' event...");

                if (instanceForPrefix.RuntimeStatus != OrchestrationRuntimeStatus.Running
                    && instanceForPrefix.RuntimeStatus != OrchestrationRuntimeStatus.Pending)
                {
                    if (instanceForPrefix.RuntimeStatus != OrchestrationRuntimeStatus.Terminated)
                    {
                        await starter.TerminateAsync(prefix, @"bounce");
                    }

                    var retval = await starter.StartNewAsync(@"EnsureAllFiles", prefix, eventGridSoleItem);
                    log.LogTrace($@"Restarted listener for {prefix}. {retval}");
                }
                else
                {
                    await starter.RaiseEventAsync(prefix, @"newfile", newCustomerFile.Filename);
                }
            }

            return starter.CreateCheckStatusResponse(req, prefix);
        }
    }
}
