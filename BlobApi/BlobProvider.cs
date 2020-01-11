using System;
using System.Collections.Concurrent;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzFunction
{
    public class BlobProvider
    {
        public readonly Lazy<CloudBlobClient> CloudBlobClient;
        public readonly Lazy<CloudStorageAccount> StorageAccount;

        public BlobProvider(string connectionString)
        {
            StorageAccount = new Lazy<CloudStorageAccount>(() =>
            {
                if (!CloudStorageAccount.TryParse(connectionString, out var storageAccount))
                {
                    throw new Exception("Cannot connect to account storage!");
                }

                return storageAccount;
            });
            CloudBlobClient = new Lazy<CloudBlobClient>(() => { return StorageAccount.Value.CreateCloudBlobClient(); });
        }
    }
}