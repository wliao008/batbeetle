using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public static class Extension
    {
        public static byte[] ToByte(this object obj)
        {
            return Encoding.UTF8.GetBytes(obj.ToString());
        }

        public static byte[] ToByte(this string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }
    }
}
