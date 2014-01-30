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

            foreach (var ch in channels)
            {
                var subdata = this.client.ReadMultibulkResponse();
                if (OnSubscribed != null)
                    OnSubscribed(this, new PubSubEventArgs(subdata));
            }

            while (subscriberCount > 0 && this.client.IsConnected())
            {
                var recdata = this.client.ReadMultibulkResponse();
                var arg = new PubSubEventArgs(recdata);
                if (OnMessageReceived != null)
                    OnMessageReceived(this, arg);
            }
        }

        public void Unsubscribe(params string[] channels)
        {
            lock (locker)
            {
                subscriberCount--;
            }
            var cmd = new RedisCommand(Commands.Unsubscribe);
            foreach (var channel in channels)
                cmd.ArgList.Add(channel.ToByte());
            this.client.SendCommand(cmd);
            foreach (var ch in channels)
            {
                var unsubdata = this.client.ReadMultibulkResponse();
                if (OnUnsubscribed != null)
                    OnUnsubscribed(this, new PubSubEventArgs(unsubdata));
            }
        }
    }

    public class PubSubEventArgs : EventArgs
    {
        public Message Message { get; set; }
        public PubSubEventArgs(byte[][] data)
        {
            if (data != null)
            {
                this.Message = this.ParseMessage(data);
            }
        }

        private Message ParseMessage(byte[][] data)
        {
            var result = data.MultiBytesToList();
            var msg = new Message();
            switch (result[0])
            {
                case "subscribe":
                    msg.MessageType = Batbeetle.MessageType.Subscribe;
                    break;
                case "unsubscribe":
                    msg.MessageType = Batbeetle.MessageType.Unsubscribe;
                    break;
                case "message":
                    msg.MessageType = Batbeetle.MessageType.Message;
                    break;
            }

            msg.Channel = result[1];
            msg.MessageData = result[2];
            return msg;
        }
    }

    public enum MessageType
    {
        Subscribe,
        Unsubscribe,
        Message
    }

    public class Message
    {
        public string MessageData { get; set; }
        public MessageType MessageType { get; set; }
        public string Channel { get; set; }
    }
}
