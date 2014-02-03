using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class Cluster
    {
        private readonly ushort RedisClusterHashSlot = 16384;
        private readonly ushort RedisClusterRequestTtl = 16;
        private readonly ushort RedisClusterDefaultTimeout = 1;

        public ushort KeySlot(byte[] key)
        {
            return (ushort)(Crc16.GetCrc16(key) % RedisClusterHashSlot);
        }
    }
}
