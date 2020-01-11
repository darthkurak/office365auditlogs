using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.Http.Internal;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AzFunction
{
    public static class WriteLogsToBlob
    {
        private static Lazy<HttpClient> HttpClient = new Lazy<HttpClient>(() =>
        {
            var httpClient = new HttpClient();

            var office365serviceCredentials = System.Environment.GetEnvironmentVariable("Office365Credentials", EnvironmentVariableTarget.Process);
            var byteArray = Encoding.ASCII.GetBytes(office365serviceCredentials);
            httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            return httpClient;
        });

        private static Lazy<BlobManager> BlobManager = new Lazy<BlobManager>(() =>
        {
            var connectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process);
            return new BlobManager(new BlobProvider(connectionString));
        });

        [FunctionName("WriteLogsToBlob")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            var queryItems = req.Query.SelectMany(x => x.Value, (col, value) => new KeyValuePair<string, string>(col.Key.ToLowerInvariant(), value)).ToList();

            var container = GetContainerName(queryItems);

            RemoveSelfHandledParameters(queryItems);

            log.LogInformation($"C# Http trigger function executed at: {DateTime.Now}");

            PrintQueryParameters(log, queryItems);

            AddPaginationParameters(queryItems);

            var uri = BuildUri(queryItems);

            return await GetLogsAndWriteToBlobs(log, container, uri);
        }

        private static string GetContainerName(List<KeyValuePair<string, string>> queryItems)
        {
            var container = "auditlogs";

            if (queryItems.Any(p => p.Key == "container"))
            {
                var val = queryItems.First(p => p.Key == "container").Value;

                if (!string.IsNullOrWhiteSpace(val))
                {
                    container = val;
                }
            }

            return container;
        }

        private static void RemoveSelfHandledParameters(List<KeyValuePair<string, string>> queryItems)
        {
            queryItems.RemoveAll(p => p.Key == "container" || p.Key == "sessioncommand" || p.Key == "sessionid" || p.Key == "resultsize");
        }

        private static void PrintQueryParameters(ILogger log, List<KeyValuePair<string, string>> queryItems)
        {
            foreach (var queryItem in queryItems)
            {
                log.LogInformation($"{queryItem.Key}: {queryItem.Value}");
            }
        }

        private static void AddPaginationParameters(List<KeyValuePair<string, string>> queryItems)
        {
            //this is unique session used for paging mechanism in Audit Log Api
            var sessionId = Guid.NewGuid();

            queryItems.Add(new KeyValuePair<string, string>("ResultSize", "300"));
            queryItems.Add(new KeyValuePair<string, string>("SessionId", sessionId.ToString()));
            queryItems.Add(new KeyValuePair<string, string>("SessionCommand", "ReturnNextPreviewPage"));
        }

        private static Uri BuildUri(List<KeyValuePair<string, string>> queryItems)
        {
            var queryBuilder = new QueryBuilder(queryItems);

            string url = $"https://outlook.office365.com/psws/service.svc/UnifiedAuditLog";

            var uriBuilder = new UriBuilder(url);

            uriBuilder.Query = queryBuilder.ToQueryString().Value;

            var uri = uriBuilder.Uri;

            return uri;
        }

        private static async Task<ActionResult> GetLogsAndWriteToBlobs(ILogger log, string container, Uri uri)
        {

            bool noMoreResults = false;

            //get auditLogs from Audit Log Api, loop until there is no more result, same sessionId is used to page API
            int sum = 0;
            do
            {
                var result = await HttpClient.Value.GetAsync(uri);

                if (!result.IsSuccessStatusCode)
                {
                    var response = await result.Content.ReadAsStringAsync();
                    var objectResult = new ObjectResult("Calling UnifiedAuditLog Api failed: " + response) { StatusCode = (int)result.StatusCode };
                    return objectResult;
                }

                var auditLog = await result.Content.ReadAsStringAsync();

                JObject auditLogObject = JObject.Parse(auditLog);
                var list = (auditLogObject["value"] as JArray);
                if (list?.Count > 0)
                {
                    log.LogInformation($"Writing {list.Count} dataLogs points");
                    sum += list.Count;
                    //Group logs by Date
                    var groups = list.GroupBy(p => GetKey(p));
                    foreach (var group in groups)
                    {
                        //Append them to blob, we selecting AuditData (actual logs)
                        await BlobManager.Value.AppendAsync(container, group.Key, Encoding.UTF8.GetBytes(string.Join("\n", group.Select(p => p["AuditData"])) + "\n"), log);
                    }
                }
                else
                {
                    noMoreResults = true;
                }
            }
            while (!noMoreResults);

            log.LogInformation($"Writing finished! {sum} dataLogs points was written.");

            return (ActionResult)new OkObjectResult($"Logs written {sum}");
        }

        private static string GetKey(JToken jToken)
        {
            var dateTime = jToken["CreationDate"].ToObject<DateTime>();
            return "auditlogs-" + dateTime.Year + "-" + dateTime.Month + "-" + dateTime.Day + ".json";
        }
    }
}
