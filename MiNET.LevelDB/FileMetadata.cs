namespace MiNET.LevelDB
{
	public class FileMetadata
	{
		public ulong FileNumber { get; set; }
		public ulong FileSize { get; set; }
		public byte[] SmallestKey { get; set; }
		public byte[] LargestKey { get; set; }
	}
}