using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.ComponentModel;
using System.Threading;

namespace UnitTest
{
    [TestClass]
    public class PubsubTests : TestBase
    {
        //[TestMethod]
        //public void Publish_ValidParms_ShouldPublishMsg()
        //{
        //    using (var client = new RedisClient(this.Host))
        //    {
        //    }
        //}

        [TestMethod]
        public void Subscribe_ValidParams_ShouldSubscribeToChannels()
        {
            BackgroundWorker subscriber = new BackgroundWorker();
            BackgroundWorker publisher = new BackgroundWorker();
            subscriber.DoWork += (s, e) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    var pubsub = new RedisPubSub(client);
                    pubsub.OnSubscribed += (s1, e1) =>
                    {
                        System.Diagnostics.Debug.WriteLine("Subscribed!");
                        publisher.RunWorkerAsync();
                    };
                    pubsub.OnMessageReceived += (s2, e2) =>
                    {
                        System.Diagnostics.Debug.WriteLine("Msg rec'd");
                        System.Diagnostics.Debug.WriteLine(e2.Message.MultiBytesToString());
                        Assert.IsNotNull(e2.Message);
                        Assert.AreEqual("message\r\nfoo\r\nhello\r\n", e2.Message.MultiBytesToString());
                        client.Quit();
                    };

                    pubsub.SubscribeToChannel("foo");
                }
            };

            publisher.DoWork += (t, f) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    var result = client.Publish("foo".ToByte(), "hello".ToByte());
                    System.Diagnostics.Debug.WriteLine("Published msg");
                    Assert.IsNotNull(result);
                }
            };

            subscriber.RunWorkerAsync();
        }
    }
}
