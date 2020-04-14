using System;
using System.Threading;
using Confluent.Kafka;

namespace VATMap_Kafka
{
    public class Program
    {
        static void Main(string[] args)
        {
            Server server = new Server();

            server.Start();
        }
    }
}
