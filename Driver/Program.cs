using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Batbeetle;
using ProtoBuf;

namespace Driver
{
    class Program
    {
        static void Main(string[] args)
        {
            var before = DateTime.Now;
            using (var client = new RedisClient("127.0.0.1", 6379))
            {
                Hashtable tbl = new Hashtable();
                tbl["name"] = "Wei Liao";
                tbl["age"] = 32;
                tbl["url"] = "http://www.goo.com";
                tbl["email"] = "Wei.Liao@liazon.com";
                client.HMSet("mynewhash", tbl);
                var now = DateTime.Now - before;
                Console.WriteLine("took {0} sec {1} ms, total {2} ms",
                    now.Seconds, now.Milliseconds, now.TotalMilliseconds);
            }

            Console.Read();
        }
    }
}
