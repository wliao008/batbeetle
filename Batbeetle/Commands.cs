namespace Batbeetle
{
    public static partial class Commands
    {
        //args
        public static readonly byte[] Ex = new byte[] { 0x45, 0x58 };
        public static readonly byte[] Px = new byte[] { 0x50, 0x58 };
        public static readonly byte[] Nx = new byte[] { 0x4E, 0x58 };
        public static readonly byte[] Xx = new byte[] { 0x58, 0x58 };
        public static readonly byte[] Copy = new byte[] { 0x43, 0x4F, 0x50, 0x59 };
        public static readonly byte[] Replace = new byte[] { 0x52, 0x45, 0x50, 0x4C, 0x41, 0x43, 0x45 };
        public static readonly byte[] Encoding = new byte[] { 0x45, 0x4E, 0x43, 0x4F, 0x44, 0x49, 0x4E, 0x47 };
        public static readonly byte[] Refcount = new byte[] { 0x52, 0x45, 0x46, 0x43, 0x4F, 0x55, 0x4E, 0x54 };
        public static readonly byte[] Idletime = new byte[] { 0x49, 0x44, 0x4C, 0x45, 0x54, 0x49, 0x4D, 0x45 };

        //cluster
        public static readonly byte[] Cluster = new byte[] { 0x43, 0x4C, 0x55, 0x53, 0x54, 0x45, 0x52 };
        public static readonly byte[] Nodes = new byte[] { 0x4E, 0x4F, 0x44, 0x45, 0x53 };
    }
}
