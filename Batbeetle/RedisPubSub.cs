using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class RedisPubSub
    {
        public delegate void EventHandler(object sender, PubSubEventArgs e);
        public event EventHandler OnSubscribed;
        public event EventHandler OnMessageReceived;
        private RedisClient client;
        private int subscriberCount;

        public RedisPubSub(RedisClient client)
        {
            this.client = client;
        }

        public void SubscribeToChannel(string channel)
        {
            subscriberCount++;
            var cmd = new RedisCommand(Commands.Subscribe);
            cmd.ArgList.Add(channel.ToByte());
            this.client.SendCommand(cmd);
            OnSubscribed(this, new PubSubEventArgs());
            var subdata = this.client.ReadMultibulkResponse();
            var arg = new PubSubEventArgs(subdata);
            //OnMessageReceived(this, arg);

            while (this.subscriberCount > 0)
            {
                try
                {
                    var recdata = this.client.ReadMultibulkResponse();
                    arg = new PubSubEventArgs(recdata);
                    OnMessageReceived(this, arg);
                }
                catch { }
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
