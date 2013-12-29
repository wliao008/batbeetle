using System;
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
            //DbEntities db = new DbEntities();
            //var consumers = db.Consumers.ToList();
            //var products = db.Products.ToList();
            //var count = consumers.Count + products.Count;
            //using (var r = new RedisClient("192.168.10.137"))
            //{
            //    //for (int i = 0; i < count; ++i)
            //    //{
            //    //    //Console.WriteLine("Item " + i);
            //    //    //r.Set("key" + i.ToString(), Guid.NewGuid());
            //    //}
            //    r.Set("name", "weiliao3");
            //    r.Set("num", 10);
            //    Console.WriteLine("setting name");
            //}

            //using (var ms = new MemoryStream())
            //{
            //    var formatter = new BinaryFormatter();
            //    foreach (var c in consumers)
            //    {
            //        //formatter.Serialize(ms, c);
            //        Serializer.Serialize(ms, c);
            //    }

            //    foreach (var p in products)
            //        Serializer.Serialize(ms, p);
            //} 
            using (var client = new RedisClient("192.168.10.139", 6380))
            {
                client.Connect()
                    .ContinueWith(t =>
                {
                    Console.WriteLine("Main: Connected");
                    var task = client.Set("username", "weiliao");
                    var awaiter = task.GetAwaiter();
                    awaiter.GetResult();
                    var now = DateTime.Now - before;
                    Console.WriteLine("took {0} sec {1} ms, total {2} ms",
                        now.Seconds, now.Milliseconds, now.TotalMilliseconds);

                });
            }

            Console.Read();
        }
    }
}
