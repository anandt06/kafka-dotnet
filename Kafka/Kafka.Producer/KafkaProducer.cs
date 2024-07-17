using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Producer
{
    public class KafkaProducer
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;

        public KafkaProducer(string bootstrapServers, string topic)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;
        }

        public async Task ProduceMessageAsync(string message)
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var deliveryResult = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });
                    Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
                }
            }
        }

        public async Task<List<string>> ListTopicsAsync()
        {
            var config = new AdminClientConfig { BootstrapServers = _bootstrapServers };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                try
                {
                    var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                    var topics = metadata.Topics.Select(t => t.Topic).ToList();
                    return topics;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error retrieving topics: {ex.Message}");
                    return new List<string>();
                }
            }
        }

        public async Task DeleteTopicAsync(string topic)
        {
            var config = new AdminClientConfig { BootstrapServers = _bootstrapServers };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                try
                {
                    await adminClient.DeleteTopicsAsync(new List<string> { topic });
                    Console.WriteLine($"Topic '{topic}' deleted successfully.");
                }
                catch (DeleteTopicsException e)
                {
                    foreach (var result in e.Results)
                    {
                        Console.WriteLine($"An error occurred deleting topic {result.Topic}: {result.Error.Reason}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An error occurred: {ex.Message}");
                }
            }
        }
    }
}