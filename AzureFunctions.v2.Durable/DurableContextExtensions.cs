using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace FileValidation
{
    static class DurableContextExtensions
    {
#if FUNCTIONS_V1
        public static void Log(this DurableOrchestrationContext context, ILogger log, string messsage, bool onlyIfNotReplaying = true)
        {
            if (!onlyIfNotReplaying || !context.IsReplaying)
            {
                log.LogWarning(messsage);
            }
        }

        public static void Log(this DurableOrchestrationClient _, ILogger log, string messsage) => log.LogWarning(messsage);

        public static JToken GetInputAsJson(this DurableActivityContextBase ctx) => ctx.GetInput<JToken>();

        public static JToken GetInputAsJson(this DurableOrchestrationContextBase ctx) => ctx.GetInput<JToken>();
#else
        public static void Log(this IDurableOrchestrationContext context, ILogger log, string messsage, bool onlyIfNotReplaying = true)
        {
            if (!onlyIfNotReplaying || !context.IsReplaying)
            {
                log.LogWarning(messsage);
            }
        }

        public static void Log(this IDurableClient _, ILogger log, string messsage) => log.LogWarning(messsage);

        public static JToken GetInputAsJson(this IDurableActivityContext ctx) => ctx.GetInput<JToken>();

        public static JToken GetInputAsJson(this IDurableOrchestrationContext ctx) => ctx.GetInput<JToken>();
#endif

    }
}
