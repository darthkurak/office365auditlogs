using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace AzFunction
{
    public class BlobManager
    {
        private readonly BlobProvider _blobProvider;

        public BlobManager(BlobProvider blobProvider)
        {
            _blobProvider = blobProvider;
        }

        public async Task AppendAsync(string containerName, string path, byte[] data, ILogger logger)
        {
            var blobClient = _blobProvider.CloudBlobClient.Value;
            var container = blobClient.GetContainerReference(containerName);
            await CreateContainerIfNotExistsAsync(container, logger);

            var appendBlob = container.GetAppendBlobReference(path);

            await CreateBlobIfNotExists(appendBlob, logger);

            using (var ms = new MemoryStream(data))
            {
                await AppendBlockAsync(appendBlob, ms, logger);
            }
        }

        private async Task<T> DoAndLog<T>(string actionName, Func<Task<T>> action, ILogger logger)
        {
            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var result = await action();
                stopwatch.Stop();
                logger.LogDebug(
                    $"{nameof(BlobManager)}.{actionName} called. Took: {stopwatch.Elapsed.TotalMilliseconds} ms");
                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"{nameof(BlobManager)}.{actionName} failed. Exception={ex.Message}");

                throw;
            }
        }

        private async Task AppendBlockAsync(CloudAppendBlob appendBlob, MemoryStream ms, ILogger logger) =>
            await DoAndLog(nameof(AppendBlockAsync), async () =>
            {
                await appendBlob.AppendBlockAsync(ms);
                return true;
            }, logger);

        private async Task CreateContainerIfNotExistsAsync(CloudBlobContainer container, ILogger logger) =>
            await DoAndLog(nameof(CreateContainerIfNotExistsAsync), async () =>
            {
                await container.CreateIfNotExistsAsync();
                return true;
            }, logger);

        private async Task CreateBlobIfNotExists(CloudAppendBlob blob, ILogger logger) =>
            await DoAndLog(nameof(CreateBlobIfNotExists), async () =>
            {
                try
                {
                    await blob.CreateOrReplaceAsync(
                        AccessCondition.GenerateIfNotExistsCondition(),
                        new BlobRequestOptions() { RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(1), 10) },
                        null);
                }
                catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                }
                return true;
            }, logger);
    }
}