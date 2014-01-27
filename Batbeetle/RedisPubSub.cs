using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class RedisPubSub
    {
        public delegate void EventHandler(object sender, PubSubEventArgs e);
        public event EventHandler OnSubscribed, OnUnsubscribed;
        public event EventHandler OnMessageReceived;
        private RedisClient client;
        private static int subscriberCount;
        private static object locker = new object();

        public RedisPubSub(RedisClient client)
        {
            this.client = client;
        }

        public void SubscribeToChannel(params string[] channels)
        {
            lock (locker)
            {
                subscriberCount += channels.Length;
            }
            var cmd = new RedisCommand(Commands.Subscribe);
            foreach(var channel in channels)
                cmd.ArgList.Add(channel.ToByte());
            this.client.SendCommand(cmd);
            if (OnSubscribed != null)
                OnSubscribed(this, new PubSubEventArgs());
            var subdata = this.client.ReadMultibulkResponse();
            var arg = new PubSubEventArgs(subdata);
            //OnMessageReceived(this, arg);

            while (subscriberCount > 0 && this.client.IsConnected())
            {
                try
                {
                    Console.WriteLine("subscribe count > 0 and is connected");
                    var recdata = this.client.ReadMultibulkResponse();
                    arg = new PubSubEventArgs(recdata);
                    if (OnMessageReceived != null)
                        OnMessageReceived(this, arg);
                }
                catch { }
            }
        }

        public void Unsubscribe(params string[] channels)
        {
            lock (locker)
            {
                subscriberCount--;
                Console.WriteLine("subscribe count = 0");
                var cmd = new RedisCommand(Commands.Unsubscribe);
                foreach (var channel in channels)
                    cmd.ArgList.Add(channel.ToByte());
                this.client.SendCommand(cmd);
                if (OnUnsubscribed != null)
                    OnUnsubscribed(this, new PubSubEventArgs());
                var recdata = this.client.ReadMultibulkResponse();
                var arg = new PubSubEventArgs(recdata);
                if (OnMessageReceived != null)
                    OnMessageReceived(this, arg);
            }
        }
    }

    public class PubSubEventArgs : EventArgs
    {
        public byte[][] Message { get; set; }
        public PubSubEventArgs() { }
        public PubSubEventArgs(byte[][] msg)
        {
            this.Message = msg;
        }
    }
}
