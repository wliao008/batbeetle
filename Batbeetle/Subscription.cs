﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class Subscription
    {
        public delegate void EventHandler(object sender, SubscriptionEventArgs e);
        //public event EventHandler OnSubscribed, OnUnsubscribed;
        public event EventHandler OnMessageReceived;
        private RedisClient client;
        private static int subscriberCount;
        private static object locker = new object();

        public Subscription(RedisClient client)
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

            //foreach (var ch in channels)
            //{
            //    var subdata = this.client.ReadMultibulkResponse();
            //    if (OnSubscribed != null)
            //        OnSubscribed(this, new SubscriptionEventArgs(subdata));
            //}

            while (subscriberCount > 0 && this.client.IsConnected())
            {
                var recdata = this.client.ReadMultibulkResponse();
                var arg = new SubscriptionEventArgs(recdata);
                if (OnMessageReceived != null)
                    OnMessageReceived(this, arg);
            }
        }

        /// <summary>
        /// Unsubscribes the client from the given channels, or from all of them if none is given.
        /// </summary>
        /// <param name="channels"></param>
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
            
            //if no channels given, all are unsubscribed
            for (int i = 0; i <= subscriberCount; i++)
            {
                var unsubdata = this.client.ReadMultibulkResponse();
                if (OnMessageReceived != null)
                    OnMessageReceived(this, new SubscriptionEventArgs(unsubdata));
            }
        }
    }

    public class SubscriptionEventArgs : EventArgs
    {
        public Message Message { get; set; }
        public SubscriptionEventArgs(byte[][] data)
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
