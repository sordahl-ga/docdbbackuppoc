using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Azure.Documents.Client;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.Azure.Documents.Linq;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Blob;

namespace docdbbackuppoc
{
    public static class BackUpFunction
    {
        private static CloudTableClient tableClient;

        private static string DatabaseName = System.Environment.GetEnvironmentVariable("database");
        private static string CollectionName = System.Environment.GetEnvironmentVariable("collection");
        private static string endpointUrl = System.Environment.GetEnvironmentVariable("endpointurl");
        private static string authorizationKey = System.Environment.GetEnvironmentVariable("authKey");
        private static string storageAcct = System.Environment.GetEnvironmentVariable("AZURE_STORAGE");
        [FunctionName("BacupDocDB")]
        public static void Run([TimerTrigger("0 */1 * * * *")]TimerInfo myTimer, TraceWriter log)
        {

            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");
            if (new List<string> { DatabaseName, CollectionName, endpointUrl, authorizationKey, storageAcct }.Any(i => String.IsNullOrEmpty(i)))
            {
                log.Error("Please provide environment variables for database, collection, endpointurl,authKey and AZURE_STORAGE.");
            }
            else
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageAcct);
                tableClient = storageAccount.CreateCloudTableClient();
                //When did we last check for changes?
                StateInfo lastrunstate = GetOrUpdateLastRunStateInfo().Result;
                StateInfo currentstate = new StateInfo(DateTime.Now);
                log.Info("BacupDocDB: Last Ran @" + lastrunstate.LastRunTime.ToString("s"));
                log.Info("BacupDocDB: Current Time @" + currentstate.LastRunTime.ToString("s"));
               
                int numdocs = BackUpCollection(storageAccount,lastrunstate.LastRunTime).Result;
                var x = GetOrUpdateLastRunStateInfo(currentstate).Result;
                log.Info("CosmosDBChangeFeed for db: " + DatabaseName + " colletion: " + CollectionName + " " + numdocs + " changed documents saved to file.");
                
            }

        }
        private static async Task<StateInfo> GetOrUpdateLastRunStateInfo(StateInfo state = null)
        {
            CloudTable stateTable = tableClient.GetTableReference("docdbbackupstate");
            await stateTable.CreateIfNotExistsAsync();

            if (state == null)
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<StateInfo>("StateInfo", "LastRun");
                // Execute the retrieve operation.
                var retrievedResult = await stateTable.ExecuteAsync(retrieveOperation);
                if (retrievedResult.Result != null)
                {
                    return (StateInfo)retrievedResult.Result;
                }
                return new StateInfo(DateTime.Now);
            }
            else
            {
                // Create the TableOperation that inserts the state entity.
                TableOperation insertOperation = TableOperation.InsertOrReplace(state);
                // Execute the insert operation.
                await stateTable.ExecuteAsync(insertOperation);
                return state;
            }
        }
        private static async Task<int> BackUpCollection(CloudStorageAccount storageAccount,DateTime starttime)
        {
            Uri collectionURI = UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName);
            //Our changes file daily roll example
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            //Setup our container we are going to use and create it.
            CloudBlobContainer container = blobClient.GetContainerReference("dbchanges");
            await container.CreateIfNotExistsAsync();
            DateTime changefiledate = DateTime.Today;
            CloudAppendBlob appBlob = container.GetAppendBlobReference(
            string.Format("{0}_{1}_{2}{3}", DatabaseName, CollectionName,changefiledate.ToString("yyyyMMdd"), ".json"));
            var exists = await appBlob.ExistsAsync();
            if (!exists)
            {
                await appBlob.CreateOrReplaceAsync();
                await appBlob.AppendTextAsync("[");
                //Store file as JSON Array for easy import

            }
            int x = 0;
            string pkRangesResponseContinuation = null;
            using (var client = new DocumentClient(new Uri(endpointUrl), authorizationKey,
                new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
            {
                var pkRangesResponse = await client.ReadPartitionKeyRangeFeedAsync(collectionURI, new FeedOptions { RequestContinuation = pkRangesResponseContinuation });
                List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();
                partitionKeyRanges.AddRange(pkRangesResponse);
                pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
                Dictionary<string, string> checkpoints = new Dictionary<string, string>();
                bool comma = exists;
                foreach (PartitionKeyRange pkRange in partitionKeyRanges)
                {
                    string continuation = null;
                    checkpoints.TryGetValue(pkRange.Id, out continuation);
                    IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                        collectionURI,
                        new ChangeFeedOptions
                        {
                            PartitionKeyRangeId = pkRange.Id,
                            StartFromBeginning = true,
                            RequestContinuation = continuation,
                            MaxItemCount = -1,
                            // Set reading time: only show change feed results modified since StartTime
                            StartTime = starttime
                        });
                    
                    while (query.HasMoreResults)
                    {
                        FeedResponse<dynamic> readChangesResponse = query.ExecuteNextAsync<dynamic>().Result;

                        foreach (dynamic changedDocument in readChangesResponse)
                        {
                            Console.WriteLine("document: {0}", changedDocument);
                            await appBlob.AppendTextAsync((comma ? "," : "")  + changedDocument);
                            x++;
                            comma = true;
                        }
                        checkpoints[pkRange.Id] = readChangesResponse.ResponseContinuation;
                    }
                }
            }
            return x;
        }
    }
}
