
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using TableEntity = Microsoft.WindowsAzure.Storage.Table.TableEntity;
using System.Numerics;

namespace Api
{
    class Program
    {
        static async Task Main()
        {

            string connectionString = "<YOUR-CONNECTIONSTRING>";

            BlobContainerClient containerClient = new BlobContainerClient(connectionString, "<YOUR-CONTAINER>");

            //TODO uppdatera
            string wantedBlob = "YYYYMMDDHHMM";
        
            await foreach (BlobItem blobItem in containerClient.GetBlobsAsync()) {
                    if(blobItem.Name == wantedBlob) {                        
                        BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);
                        BlobDownloadResult downloadResult = await blobClient.DownloadContentAsync();

                        Console.WriteLine($"Downloaded blob: {downloadResult.Content.ToString()}");
                    }
            }

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("<YOUR-TABLE>");
            
            
            Int64 from = Int64.Parse("YYYYMMDDHHMM");
            Int64 to = Int64.Parse("YYYYMMDDHHMM");

            List<string> queries = new List<string>();
            for (Int64 i = from; i < to; i++)
            {
                queries.Add(i.ToString());
            }

            List<LogEntity> logEntities = new List<LogEntity>();
            foreach (var q in queries) {
                            TableQuery<LogEntity> queryReq = new TableQuery<LogEntity>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, q));
           
                try {
                    foreach (LogEntity entity in await table.ExecuteQuerySegmentedAsync<LogEntity>(queryReq, null))
                    {
                        logEntities.Add(entity);
                    }
                } catch (Exception e) {
                    Console.WriteLine(e.Message + "\tQuery could not be executed");
                }
            }
            
            foreach (var item in logEntities)
            {
                Console.WriteLine($"Log: {item.PartitionKey}, {item.Statuscode}");
            }
        }

        public class LogEntity : TableEntity
        {
            public LogEntity(string fileName, string id)
            {
                this.PartitionKey = fileName;
                this.RowKey = id;
            }
            public LogEntity() { }
            public string? Statuscode { get; set; }
        }   
    }
}