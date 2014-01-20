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
        private Queue<Action<RedisClient>> commands;
        public RedisTransaction(RedisClient client)
        {
            this.client = client;
            this.commands = new Queue<Action<RedisClient>>();
            this.Init();
        }

        private void Init()
        {
            Console.WriteLine("Calling Multi()");
            this.client.Multi();
            Console.WriteLine("Transaction.Init, Multi() called");
        }

        public void QueueCommand(Action<RedisClient> cmd)
        {
            this.commands.Enqueue(cmd);
        }

        public void Commit()
        {
            foreach (var cmd in this.commands)
            {
                cmd(this.client);
            }

            var resp = this.client.Exec();
            Console.WriteLine("Exec called: ");
            var str = resp.BytesToString();
            var sc = str.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var s in sc)
            {
                Console.WriteLine(s);
            }
        }
    }
}
