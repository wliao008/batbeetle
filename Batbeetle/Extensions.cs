using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Batbeetle
{
    public static class Extensions
    {
        public static byte[] ToByte(this object obj)
        {
            return Encoding.UTF8.GetBytes(obj.ToString());
        }

        public static byte[] ToByte(this string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }

        public static string BytesToString(this byte[] bytes)
        {
            return Encoding.UTF8.GetString(bytes);
        }

        public static string MultiBytesToString(this byte[][] multibytes)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var bytes in multibytes)
                sb.AppendLine(bytes.BytesToString());
            return sb.ToString();
        }

        public static List<string> MultiBytesToList(this byte[][] multibytes)
        {
            if (multibytes == null) return new List<string>();
            var split = multibytes.MultiBytesToString().Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            var list = new List<string>();
            list.AddRange(split);
            return list;
        }
    }
}
