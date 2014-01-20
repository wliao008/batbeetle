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
                //Hashtable guids = new Hashtable();
                //for (int i = 0; i < 1000; i++)
                //{
                //    guids["name" + i] = Guid.NewGuid().ToString();
                //}
                //client.HMSet("myguids", guids);

                //Hashtable tbl = client.HMGetAll("myguids");
                //foreach (DictionaryEntry de in tbl)
                //    Console.WriteLine("{0}: {1}", de.Key, de.Value);

                client.Set("mykey".ToByte(), "foobar".ToByte());
                var resp = client.Bitcount("mykey".ToByte(), "1".ToByte(), "1".ToByte());
                Console.WriteLine(resp);

                var now = DateTime.Now - before;
                Console.WriteLine("took {0} sec {1} ms, total {2} ms",
                    now.Seconds, now.Milliseconds, now.TotalMilliseconds);
            }

            Console.Read();
        }
    }
}
