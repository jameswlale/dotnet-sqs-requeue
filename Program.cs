using System;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;

namespace requeue
{
    class Program
    {
        private static string dlQueueUrl = "[The DLQ Url]";
        private static string targetQueueUrl = "[The target Queue Url]";

        static void Main(string[] args)
        {
            Requeue();
            Console.ReadKey();
        }

        async static void Requeue()
        {
            using (IAmazonSQS sqs = new AmazonSQSClient(RegionEndpoint.EUWest2))
            {
                var response = await sqs.ReceiveMessageAsync(dlQueueUrl);
                var messages = response.Messages;

                await GetMessages(sqs);

                Console.WriteLine("Finished requeuing");
            }
        }

        async static Task GetMessages(IAmazonSQS sqs)
        {
            Console.WriteLine("Retrieving messages...");
            
            var response = await sqs.ReceiveMessageAsync(dlQueueUrl);
            var messages = response.Messages;

            Console.WriteLine($"{messages.Count} Messages found");

            if (messages.Count == 0)
            {
                return;
            }

            messages.ForEach(async (message) =>
            {
                var resp = await sqs.SendMessageAsync(targetQueueUrl, message.Body);                 
                Console.Write(resp);
                Console.WriteLine("Requeued message");
                await sqs.DeleteMessageAsync(dlQueueUrl, message.ReceiptHandle);
            });

            await GetMessages(sqs);
        }
    }
}
