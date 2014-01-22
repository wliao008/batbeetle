using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.ComponentModel;

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
            subscriber.DoWork += (s, e) =>
            {
                using (var client = new RedisClient(this.Host))
                {
                    var result = client.Subscribe("foo".ToByte());
                    Assert.IsNotNull(result);
                    Assert.AreEqual("message\nfoo\nhello\n", result.BytesToString());
                }
            };
            subscriber.RunWorkerAsync();

            using (var client = new RedisClient(this.Host))
            {
                var result = client.Publish("foo".ToByte(), "hello".ToByte());
                Assert.IsNotNull(result);
            }
        }
    }
}
