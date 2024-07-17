using Kafka.Consumer;

class Program
{
    static async Task Main(string[] args)
    {
        // Kafka configuration
        string kafkaBootstrapServers = "localhost:9092";
        string kafkaTopic = "my-topic";
        string kafkaGroupId = "my-group";

        // Create a Kafka consumer
        var kafkaConsumer = new KafkaConsumer(kafkaBootstrapServers, kafkaTopic, kafkaGroupId);

        // Cancellation token to stop consuming messages
        var cts = new CancellationTokenSource();

        // Consume messages
        var consumeTask = kafkaConsumer.ConsumeMessagesAsync(cts.Token);

        // Wait for user to stop consuming
        Console.WriteLine("Press any key to stop consuming...");
        Console.ReadKey();

        // Stop consuming messages
        cts.Cancel();
        await consumeTask;

        Console.WriteLine("Consumer stopped.");
    }
}