using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace FileValidation
{
    static class StorageExtensions
    {
        public static async System.Threading.Tasks.Task<IEnumerable<T>> ExecuteQueryAsync<T>(this CloudTable table, TableQuery<T> query) where T : ITableEntity, new()
        {
            TableContinuationToken token = null;
            var retVal = new List<T>();
            do
            {
                var results = await table.ExecuteQuerySegmentedAsync(query, token);
                retVal.AddRange(results.Results);
                token = results.ContinuationToken;
            } while (token != null);

            return retVal;
        }


        public static async System.Threading.Tasks.Task<IEnumerable<IListBlobItem>> ListBlobsAsync(this CloudBlobClient blobClient, string prefix)
        {
            BlobContinuationToken token = null;
            var retVal = new List<IListBlobItem>();
            do
            {
                var results = await blobClient.ListBlobsSegmentedAsync(prefix, token);
                retVal.AddRange(results.Results);
                token = results.ContinuationToken;
            } while (token != null);

            return retVal;
        }

    }

    static class HttpExtensions
    {
        public static HttpResponseMessage CreateCompatibleResponse(this HttpRequestMessage _, HttpStatusCode code) => new HttpResponseMessage(code);

        public static HttpResponseMessage CreateCompatibleResponse(this HttpRequestMessage _, HttpStatusCode code, string stringContent) => new HttpResponseMessage(code) { Content = new StringContent(stringContent) };
    }
}
