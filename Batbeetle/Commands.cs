namespace Batbeetle
{
    public static class Commands
    {
        public static readonly byte[] Set = new byte[] { 0x53, 0x45, 0x54 };
        public static readonly byte[] Get = new byte[] { 0x47, 0x45, 0x54 };

        //args for the Set command
        public static readonly byte[] Ex = new byte[] { 0x45, 0x58 };
    }
}
