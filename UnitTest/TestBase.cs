using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class TestBase
    {
        public readonly string Host = "127.0.0.1";

        [TestInitialize]
        public void BaseInit()
        {
            using (var client = new RedisClient(this.Host))
            {
                client.FlushAll();
            }
        }
    }
}
