using System.Collections.Generic;

namespace MiNET.LevelDB
{
	public class VersionEdit
	{
		public string Comparator { get; set; }
		public ulong? LogNumber { get; set; }
		public ulong? PreviousLogNumber { get; set; }
		public ulong? NextFileNumber { get; set; }
		public ulong? LastSequenceNumber { get; set; }
		public Dictionary<int, byte[]> CompactPointers { get; set; } = new Dictionary<int, byte[]>();
		public Dictionary<int, List<ulong>> DeletedFiles { get; set; } = new Dictionary<int, List<ulong>>();
		public Dictionary<int, List<FileMetadata>> NewFiles { get; set; } = new Dictionary<int, List<FileMetadata>>();
	}
}