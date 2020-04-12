using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using System;
using GeoJSON.Net;
using System.Threading;
using GeoJSON.Net.Geometry;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace VATMap_Kafka
{
    /// <summary>
    /// Kafka object. Handles consumption
    /// </summary>
    internal class Kafka
    {
        /// <summary>
        /// VATSIM Kafka URL
        /// </summary>
        private const string KAFKA_URL = "kafka-datafeed.vatsim.net:9092";

        /// <summary>
        /// Kafka Username
        /// </summary>
        private const string USERNAME = "datafeed-reader";

        /// <summary>
        /// Kafka password
        /// </summary>
        private const string PASSWORD = "datafeed-reader";

        /// <summary>
        /// Creates a new consumer object
        /// </summary>
        internal IConsumer<Ignore, string> Consumer
        {
            get
            {
                // Configure consumer
                ConsumerConfig config = new ConsumerConfig
                {
                    BootstrapServers = KAFKA_URL,
                    GroupId = "vatmap_consumer",
                    SecurityProtocol = SecurityProtocol.SaslPlaintext,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = USERNAME,
                    SaslPassword = PASSWORD,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

                // Aaaand then return
                return new ConsumerBuilder<Ignore, string>(config).Build();
            }
        }

        /// <summary>
        /// Begins consumption from Kafka
        /// </summary>
        internal void Start ()
        {
            // Consumer object
            using var consumer = this.Consumer;

            // Subscribe consumer to VATSIM kafka feed
            consumer.Subscribe("datafeed");

            // Ensure safe termination
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => // Wait for Ctrl+C
            {
                // Cancel kafka consumption safely
                e.Cancel = true;
                tokenSource.Cancel();
            };

            // Read from kafka
            try
            {
                // Consume until cancel triggered
                while (true)
                {
                    try
                    {   
                        // Consume Kafka
                        ConsumeResult<Ignore, string> result = consumer.Consume(tokenSource.Token);

                        // Get the raw message
                        JObject messageRaw = JObject.Parse(result.Message.Value);

                        // Check if the message type is correct
                        if ((string) messageRaw["message_type"] == "update_position")
                        {
                            // Cast coordinates to doubles
                            double lat = (double)messageRaw["data"]["latitude"];
                            double lon = (double)messageRaw["data"]["longitude"];

                            // Define new GeoJson point
                            Point geoPoint = new Point(new Position(lat, lon));

                            // Built PilotPosition object
                            PilotPosition position = new PilotPosition(
                                (string)messageRaw["data"]["callsign"],
                                (int)messageRaw["data"]["altitude"],
                                geoPoint);

                            // *Sigh*
                            DefaultContractResolver thisWillMakeAidanHappy = new DefaultContractResolver
                            {
                                NamingStrategy = new SnakeCaseNamingStrategy()
                            };

                            // Serialise the pilot position
                            string jsonPosition = JsonConvert.SerializeObject(position, new JsonSerializerSettings
                            {
                                ContractResolver = thisWillMakeAidanHappy
                            });

                            // Write to console
                            Console.WriteLine(jsonPosition + "\n");                           
                        }
                        else // If not a pilot position update
                        {
                            // We don't want it
                            continue;
                        }
                    }
                    catch (ConsumeException ex) // If something happens
                    {
                        // Catch the oops
                        Console.WriteLine($"Exception: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException) // Catch the cancel event
            {
                // Leave group and close cleanly
                consumer.Close();
            }
        }
    }
}
