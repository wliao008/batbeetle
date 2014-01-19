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
using System.Diagnostics;

namespace Driver
{
    class Program
    {
        static void Main(string[] args)
        {
            var before = DateTime.Now;
            using (var client = new RedisClient("127.0.0.1", 6379))
            {
                client.Set("key1", "10");
                client.Set("key2", "val2");
                var resp = client.Dump("key1".ToByte());
                var str = resp.BytesToString();
                Console.WriteLine("Response: " + resp.BytesToString());

                //Hashtable guids = new Hashtable();
                //for (int i = 0; i < 1000; i++)
                //{
                //    guids["name" + i] = Guid.NewGuid().ToString();
                //}
                //client.HMSet("guidtbl", guids);

                //Hashtable tbl = client.HMGetAll("guidtbl");
                //foreach (DictionaryEntry de in tbl)
                //    Console.WriteLine("{0}: {1}", de.Key, de.Value);

                var now = DateTime.Now - before;
                Console.WriteLine("took {0} sec {1} ms, total {2} ms",
                    now.Seconds, now.Milliseconds, now.TotalMilliseconds);
            }

            Console.Read();
        }
    }
}
