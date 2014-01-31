using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.ComponentModel;
using System.Threading;

namespace UnitTest
{
    [TestClass]
    public class SubscriptionTests : TestBase
    {
        [TestMethod]
        public void Subscribe_ValidParams_ShouldSubscribeToChannels()
        {
            BackgroundWorker subscriber = new BackgroundWorker();
            BackgroundWorker publisher = new BackgroundWorker();
            int count = 0;
            subscriber.DoWork += (s, e) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    var pubsub = new Subscription(client);
                    pubsub.OnMessageReceived += (s2, e2) =>
                    {
                        if (e2.Message.MessageType == MessageType.Subscribe)
                        {
                            count++;
                            if (count == 2)
                                publisher.RunWorkerAsync();
                        }

                        if (e2.Message.MessageType == MessageType.Message)
                        {
                            Assert.AreEqual("hello", e2.Message.MessageData);
                            client.Quit();
                        }
                    };

                    pubsub.SubscribeToChannel("foo", "bar");
                }
            };

            publisher.DoWork += (t, f) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    var result = client.Publish("foo".ToByte(), "hello".ToByte());
                    Assert.IsNotNull(result);
                }
            };

            subscriber.RunWorkerAsync();
        }

        [TestMethod]
        public void Unsubscribe_NoChannelsGiven_ShouldUnsubscribeAll()
        {
            BackgroundWorker subscriber = new BackgroundWorker();
            BackgroundWorker unsubscriber = new BackgroundWorker();
            int count = 0;
            RedisClient client = null;
            subscriber.DoWork += (s, e) =>
            {
                using (client = new RedisClient(this.Host))
                {
                    var pubsub = new Subscription(client);
                    pubsub.OnMessageReceived += (s1, e1) =>
                    {
                        if (e1.Message.MessageType == MessageType.Subscribe)
                        {
                            count++;
                            Assert.AreEqual(MessageType.Subscribe, e1.Message.MessageType);
                            if (count == 2)
                                pubsub.Unsubscribe();
                        }

                        if (e1.Message.MessageType == MessageType.Unsubscribe)
                        {
                            count--;
                            Assert.AreEqual(MessageType.Unsubscribe, e1.Message.MessageType);
                            if (count == 0)
                                client.Quit();
                        }
                    };
                    pubsub.SubscribeToChannel("foo", "bar");
                }
            };
            subscriber.RunWorkerAsync();

            unsubscriber.DoWork += (s, e) =>
            {
                var pubsub2 = new Subscription(client);
                pubsub2.Unsubscribe();
            };
        }

        [TestMethod]
        public void Unsubscribe_GivenChannel_ShouldOnlyUnsubscribeThoseChannels()
        {
            BackgroundWorker subscriber = new BackgroundWorker();
            BackgroundWorker publisher = new BackgroundWorker();
            int count = 0;
            RedisClient client = null;
            subscriber.DoWork += (s, e) =>
            {
                using (client = new RedisClient(this.Host))
                {
                    var pubsub = new Subscription(client);
                    pubsub.OnMessageReceived += (s1, e1) =>
                    {
                        if (e1.Message.MessageType == MessageType.Subscribe)
                        {
                            count++;
                            Assert.AreEqual(MessageType.Subscribe, e1.Message.MessageType);
                            if (count == 2)
                                pubsub.Unsubscribe("foo");
                        }

                        if (e1.Message.MessageType == MessageType.Message)
                        {
                            Assert.AreEqual("hi", e1.Message.MessageData);
                            client.Quit();
                        }

                        if (e1.Message.MessageType == MessageType.Unsubscribe)
                        {
                            count--;
                            Assert.AreEqual(MessageType.Unsubscribe, e1.Message.MessageType);
                            if (count == 1)
                                publisher.RunWorkerAsync();
                        }
                    };
                    pubsub.SubscribeToChannel("foo", "bar");
                }
            };
            subscriber.RunWorkerAsync();

            publisher.DoWork += (s, e) =>
            {
                using (client = new RedisClient(this.Host))
                {
                    client.Publish("bar".ToByte(), "hi".ToByte());
                }
            };
        }
    }
}
