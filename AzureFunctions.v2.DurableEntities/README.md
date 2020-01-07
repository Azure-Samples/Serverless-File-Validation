# File validation using Durable Entities
To learn more about Durable Entities, check out the documentation [here](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-entities).

In this sample, you'll see how we can treat the "batch" which is being validated as a virtual actor using Durable Entities. It's then up to the entity itself to determine when all files are present, tracking state along the way.

As you'll see, it greatly simplifies the amount of Orchestration code we had to write in the other examples.