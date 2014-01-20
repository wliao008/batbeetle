using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class RedisTransaction
    {
        private RedisClient client;
        public RedisTransaction(RedisClient client)
        {
            this.client = client;
            this.Init();
        }

        private void Init()
        {
            this.client.Multi();
        }

        public void Commit()
        {
            var resp = this.client.Exec();
            var str = resp.BytesToString();
            var sc = str.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var s in sc)
            {
                Console.WriteLine(s);
            }
        }
    }
}
