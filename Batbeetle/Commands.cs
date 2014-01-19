namespace Batbeetle
{
    public static class Commands
    {
        public static readonly byte[] Ping = new byte[] { 0x50, 0x49, 0x4E, 0x47 };
        public static readonly byte[] Info = new byte[] { 0x49, 0x4E, 0x46, 0x4F };
        public static readonly byte[] Set = new byte[] { 0x53, 0x45, 0x54 };
        public static readonly byte[] Get = new byte[] { 0x47, 0x45, 0x54 };
        public static readonly byte[] HMSet = new byte[] { 0x48, 0x4D, 0x53, 0x45, 0x54 };
        public static readonly byte[] HGetAll = new byte[] { 0x48, 0x47, 0x45, 0x54, 0x41, 0x4C, 0x4C };

        //keys
        public static readonly byte[] Del = new byte[] { 0x44, 0x45, 0x4C };
        public static readonly byte[] Expire = new byte[] { 0x45, 0x58, 0x50, 0x49, 0x52, 0x45 };
        public static readonly byte[] Dump = new byte[] { 0x44, 0x55, 0x4d, 0x50 };

        //args for the Set command
        public static readonly byte[] Ex = new byte[] { 0x45, 0x58 };
        public static readonly byte[] Px = new byte[] { 0x50, 0x58 };
        public static readonly byte[] Nx = new byte[] { 0x4E, 0x58 };
        public static readonly byte[] Xx = new byte[] { 0x58, 0x58 };
    }
}
