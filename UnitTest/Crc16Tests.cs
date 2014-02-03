using System;
using Batbeetle;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class Crc16Tests
    {
        [TestMethod]
        public void GetCrc16_ShouldReturnUshort()
        {
            var result = Crc16.GetCrc16("foo1".ToByte());
            Assert.AreEqual(62583, result);
        }
    }
}
