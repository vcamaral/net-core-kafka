using Confluent.Kafka;
using System;

namespace Kafka.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine($"Usage: Kafka.Consumer <topic>");
                return;
            }

            var topic = args[0];

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-consumer",
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var consumer = new Consumer<Null, string>(config))
            {
                consumer.Subscribe(topic);

                Console.WriteLine($"Consuming topic {topic}...");

                while (true)
                {
                    try
                    {
                        var result = consumer.Consume();
                        Console.WriteLine($"Message {result.Value} consumed");
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Error to consume message: {ex.Error.Reason}");
                    }
                }
            }
        }
    }
}
