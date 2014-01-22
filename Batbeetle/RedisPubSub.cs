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

        public RedisPubSub(RedisClient client)
        {
            this.client = client;
        }

        public void SubscribeToChannel(string channel)
        {
            var cmd = new RedisCommand(Commands.Subscribe);
            cmd.ArgList.Add(channel.ToByte());
            this.client.SendCommand(cmd);
            OnSubscribed(this, new PubSubEventArgs());
            //var subdata = this.client.ReadMultibulkResponse();
            while (this.client.Available() > 0)
            {
                try
                {
                    var data = this.client.ReadMultibulkResponse();
                    var arg = new PubSubEventArgs(data);
                    OnMessageReceived(this, arg);
                }catch{}
            }
        }
    }

    public class PubSubEventArgs : EventArgs
    {
        public byte[] Message { get; set; }
        public PubSubEventArgs() { }
        public PubSubEventArgs(byte[] msg)
        {
            this.Message = msg;
        }
    }
}
