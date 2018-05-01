using System;
using System.Collections.Generic;
using System.Threading;
using StackExchange.Redis;

namespace ConsoleApplication115
{
    class Program
    {
        private static long _requests = long.MaxValue;
        static byte[] value;
        static int pendingRequests;
        static bool get;
        static long startfrom;
        static string host;
        static void Main(string[] args)
        {
            if (!ExecutionContext.IsFlowSuppressed())
            {
                ExecutionContext.SuppressFlow();
            }
            //new ThreadPoolMonitor.ThreadPoolLogger(new TimeSpan(0, 0, 2));
            var numberofconnections = Int32.Parse(args[0]);
            host = args[1];
            var password = args[2];
            pendingRequests = Int32.Parse(args[3]);
            var size = Int32.Parse(args[4]);
            var ssl = bool.Parse(args[5]);
            get = bool.Parse(args[6]);
            startfrom = long.Parse(args[7]);
            Console.WriteLine("Host:\t\t\t{0}", host);
            Console.WriteLine("PendingRequests:\t{0}", pendingRequests);
            Console.WriteLine("Cache Item Size:\t{0} bytes", size);
            Console.WriteLine();

            ConnectionMultiplexer []cm = new ConnectionMultiplexer[numberofconnections];
            ConfigurationOptions config = new ConfigurationOptions();
            config.CommandMap = CommandMap.Create(new HashSet<string>(new string[] { "SUBSCRIBE" }), false);
            config.Ssl = ssl;
            config.EndPoints.Add(host);
            config.ResponseTimeout = Int32.MaxValue;
            config.Password = password;
            config.AllowAdmin = true;
            config.AbortOnConnectFail = true;
            for (int i = 0; i < numberofconnections; i++)
            {
                cm[i] = ConnectionMultiplexer.Connect(config);
            }
            // Set test key
            value = new byte[size];
            (new Random()).NextBytes(value);
            //cm[0].GetDatabase().StringSet("test", value);
            for (int i = 0; i < numberofconnections; i++)
            {
                DoRequest(cm[i]);
            }

            while (true)
            {
                //Thread.Sleep(1000);

                //var now = DateTime.Now;
                //var elapsed = (now - last).TotalSeconds;
                //last = now;

                //var rps = (long)(Interlocked.Exchange(ref _requests, 0) / elapsed);
                //var throughput = Math.Round((double)rps * size * 8 / (1000000 * elapsed), 1);

                ////  var totalLatencyInTicks = Interlocked.Exchange(ref _totalLatencyInTicks, 0);
                ////   var totalLatencyInMs = ((double)totalLatencyInTicks) / TimeSpan.TicksPerMillisecond;
                //// var avgLatencyInMs = Math.Round((rps > 0 ? totalLatencyInMs / rps : 0), 1);
                //avgthroughput += throughput;
                //Console.WriteLine("{0}\t{1}\t{2}\t{3}", now.ToString("hh:mm:ss.fff"), rps, throughput, Math.Round(avgthroughput/ ++counter,1));
            }
        }
        
        static Random r = new Random();

        static void DoRequest(ConnectionMultiplexer cm)
        {
            keyname = startfrom == 0 ? "test" : null;
            for (int i = 0; i < pendingRequests; i++)
            {
                DoCalls(cm);
            }
        }

        static string keyname = null;
        static void DoCalls(ConnectionMultiplexer cm)
        {
            var key = keyname ?? "test" + Interlocked.Increment(ref startfrom);
            // var sw = Stopwatch.StartNew();
            //cm.GetDatabase().StringSetAsync("test" + (i++), value).ContinueWith((v) =>
            if (get)
            {
                cm.GetDatabase().StringGetAsync(key).ContinueWith((v) =>
                {
                    // await Task.Delay(sleep);
                    //Interlocked.Add(ref _totalLatencyInTicks, sw.Elapsed.Ticks);
                    //Interlocked.Increment(ref _requests);
                    DoCalls(cm);
                });
            } else
            {
                cm.GetDatabase().StringSetAsync(key, value).ContinueWith((v) =>
                {
                    if(v.IsFaulted)
                    {
                        Console.WriteLine($"{host} => {v.Exception}");
                    }
                    // await Task.Delay(sleep);
                    //Interlocked.Add(ref _totalLatencyInTicks, sw.Elapsed.Ticks);
                    //Interlocked.Increment(ref _requests);
                    DoCalls(cm);
                });
            }
        }
    }
}
