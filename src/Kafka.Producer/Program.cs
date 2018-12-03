using Confluent.Kafka;
using System;

namespace Kafka.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: Kafka.Producer <topic> <total-messages>");
                return;
            }

            var topic = args[0];
            var total = Convert.ToInt32(args[1]);

            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            using (var producer = new Producer<Null, string>(config))
            {
                Console.WriteLine($"{producer.Name} producing on topic {topic}...");

                try
                {
                    for (int i = 0; i < total; i++)
                    {
                        var result = producer.ProduceAsync(topic, new Message<Null, string>
                        {
                            Value = Guid.NewGuid().ToString()
                        }).Result;

                        Console.WriteLine($"Message {result.Value} delivered to {result.TopicPartitionOffset}"); 
                    }
                }
                catch (KafkaException ex)
                {
                    Console.WriteLine($"Error to delivery message: {ex.Error.Reason}");
                }
            }
        }
    }
}
