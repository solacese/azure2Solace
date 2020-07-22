using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using SolaceSystems.Solclient.Messaging;
using System.Threading;
using System.Text;

namespace SB2SolaceCSharp
{
    public static class Function1
    {
        private static IContext context = null;
        private static ISession session = null;
        private static ContextFactoryProperties cfp = new ContextFactoryProperties();
        private static string sUserName = Environment.GetEnvironmentVariable("solace-username");
        private static string sPassword = Environment.GetEnvironmentVariable("solace-password");
        private static string sVPNName = Environment.GetEnvironmentVariable("solace-vpnname");
        private static string sHost = Environment.GetEnvironmentVariable("solace-host");

        [FunctionName("Function1")]
        public static void Run([ServiceBusTrigger("testq", Connection = "SBConnection")]string myQueueItem, ILogger log)
        {
            connect();
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");
            sendMessage2Solace(myQueueItem);
        }

        public static void connect()
        {
            if (session == null)
                connect2Solace();
        }

        public static void sendMessage2Solace(String msg)
        {
            IMessage message = ContextFactory.Instance.CreateMessage();
            message.Destination = ContextFactory.Instance.CreateTopic("azure");
            message.DeliveryMode = MessageDeliveryMode.Direct;
            message.BinaryAttachment = Encoding.ASCII.GetBytes(msg);

            Console.WriteLine("About to send message '{0}' to topic '{1}'", msg, "azure");
            session.Send(message);
            message.Dispose();
            Console.WriteLine("Message sent. Exiting.");

        }

        public static void connect2Solace()
        {
            // Set log level.
            cfp.SolClientLogLevel = SolLogLevel.Warning;
            // Log errors to console.
            cfp.LogToConsoleError();
            // Must init the API before using any of its artifacts.
            ContextFactory.Instance.Init(cfp);

            ContextProperties contextProps = new ContextProperties();
            SessionProperties sessionProps = new SessionProperties();

            sessionProps.Host = sHost;
            sessionProps.UserName = sUserName;
            sessionProps.Password = sPassword;
            sessionProps.SSLValidateCertificate = false;
            sessionProps.VPNName = sVPNName;

            //Connection retry logic
            sessionProps.ConnectRetries = -1; //-1 means try to connect forever.
            sessionProps.ConnectTimeoutInMsecs = 10000; //10 seconds
            sessionProps.ReconnectRetries = -1; //-1 means try to reconnect forever.
            sessionProps.ReconnectRetriesWaitInMsecs = 5000; //wait for 5 seconds before retry

            // Compression is set as a number from 0-9, where 0 means "disable
            // compression", and 9 means max compression. The default is no
            // compression.
            // Selecting a non-zero compression level auto-selects the
            // compressed SMF port on the appliance, as long as no SMF port is
            // explicitly specified.
            sessionProps.CompressionLevel = 9;

            #region Create the Context

            context = ContextFactory.Instance.CreateContext(contextProps, null);

            #endregion

            #region Create and connect the Session

            session = context.CreateSession(sessionProps, null, null);

            session.Connect();
            Console.WriteLine("Connected to Solace >>>>>>>>>.");

            #endregion
        }

    }
}
