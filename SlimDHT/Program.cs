using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using CoCoL;
using log4net;
using log4net.Config;

namespace SlimDHT
{
    class Program
    {
        static void Main(string[] args)
        {
            // Allow decomposing the peer-info classes
            SerializationSetup.Setup();

            // Configure log.net
            XmlConfigurator.Configure(LogManager.GetRepository(Assembly.GetEntryAssembly()), new FileInfo("log4net.config"));

            // Make some core channels profiled
            using (new ProfilerChannelScope(
                Channels.ConnectionBrokerRequests,
                Channels.MRURequests,
                Channels.RoutingTableRequests))
            {
                try
                {
                    Task.WaitAll(ConsoleProcess.RunAsync());
                }
                catch (Exception ex)
                {
                    Console.Write(ex);
                }
            }
        }
    }
}
