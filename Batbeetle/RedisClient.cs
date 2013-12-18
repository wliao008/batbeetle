using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class RedisClient : NativeBase
    {
        public RedisClient(string host, int port = 6379)
            : base(host, port)
        {
        }
    }
}
