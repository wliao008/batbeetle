using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public class RedisClient : BaseClient
    {
        public RedisClient(string host, int port = 6379)
            : base(host, port)
        {
        }

        public void Set(string key, string value)
        {
            base.Set(Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(value));
        }

        public void Set(string key, string value, int expireInSec)
        {
            base.Set(Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(value),
                Encoding.UTF8.GetBytes(expireInSec.ToString()),
                null,
                false,
                false);
        }

        public void Set(
            string key, 
            string value, 
            int? expireInSec, 
            int? expireInMillisec, 
            bool setIfNotExist,
            bool setIfExist)
        {
            base.Set(
                Encoding.UTF8.GetBytes(key),
                Encoding.UTF8.GetBytes(value),
                expireInSec.HasValue ? Encoding.UTF8.GetBytes(expireInSec.Value.ToString()) : null,
                expireInMillisec.HasValue ? Encoding.UTF8.GetBytes(expireInMillisec.ToString()) : null,
                setIfNotExist,
                setIfExist);
        }

        public string Get(string key)
        {
            var bytes = base.Get(Encoding.UTF8.GetBytes(key));
            return Encoding.UTF8.GetString(bytes);
        }

        public void HMSet(string key, Hashtable hash)
        {
            var len = hash.Count;
            var keys = new byte[len][];
            var vals = new byte[len][];
            int i = 0;
            foreach(DictionaryEntry de in hash)
            {
                keys[i] = de.Key.ToByte();
                vals[i] = de.Value.ToByte();
                i++;
            }

            base.HMSet(key.ToByte(), keys, vals);
        }

        public Hashtable HMGetAll(string key)
        {
            var data = base.HMGetAll(key.ToByte());
            var str = data.BytesToString();
            var sc = str.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            Hashtable tbl = new Hashtable();
            for (int i = 0; i < sc.Length; i += 2)
                tbl[sc[i]] = sc[i + 1];
            return tbl;
        }
    }
}
