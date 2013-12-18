using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Batbeetle;

namespace Driver
{
    class Program
    {
        static void Main(string[] args)
        {
            var before = DateTime.Now;
            var count = 50000;
            using (var r = new RedisClient("192.168.1.115"))
            {
                //for (int i = 0; i < count; ++i)
                //{
                //    //Console.WriteLine("Item " + i);
                //    //r.Set("key" + i.ToString(), Guid.NewGuid());
                //}
                r.Set("name", "weiliao3");
                r.Set("num", 10);
                Console.WriteLine("setting name");
            }
            var now = DateTime.Now - before;
            Console.WriteLine("Redis (write): took {0} sec {1} ms to store {2} records, total {3} ms",
                now.Seconds, now.Milliseconds, count, now.TotalMilliseconds);
            Console.Read();
        }
    }
}
