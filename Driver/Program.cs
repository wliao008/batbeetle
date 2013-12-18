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
            DbEntities db = new DbEntities();
            var consumers = db.Consumers.ToList();
            var products = db.Products.ToList();
            var before = DateTime.Now;
            var count = consumers.Count + products.Count;
            //using (var r = new RedisClient("192.168.1.115"))
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

            using (var ms = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                foreach (var c in consumers)
                {
                    //formatter.Serialize(ms, c);
                    Serializer.Serialize(ms, c);
                }

                foreach (var p in products)
                    Serializer.Serialize(ms, p);
            } 
            var now = DateTime.Now - before;
            Console.WriteLine("Redis (write): took {0} sec {1} ms to store {2} records, total {3} ms",
                now.Seconds, now.Milliseconds, count, now.TotalMilliseconds);
            Console.Read();
        }
    }
}
