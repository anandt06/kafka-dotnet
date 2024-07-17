using Kafka.Producer;

class Program
{
    static async Task Main(string[] args)
    {
        // Kafka configuration
        string kafkaBootstrapServers = "localhost:9092";
        string kafkaTopic = "my-topic";

        // Create a Kafka producer
        var kafkaProducer = new KafkaProducer(kafkaBootstrapServers, kafkaTopic);

        //var topics = await kafkaProducer.ListTopicsAsync();

        //await kafkaProducer.DeleteTopicAsync("");

        // Message to send
        string message = "2 Hello, Kafka from C#!";

        // Produce message to Kafka
        await kafkaProducer.ProduceMessageAsync(message);

        Console.WriteLine("Message produced to Kafka.");
    }
}