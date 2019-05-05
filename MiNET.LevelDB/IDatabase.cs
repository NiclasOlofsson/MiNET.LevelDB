using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using log4net;
using Newtonsoft.Json;

namespace MiNET.LevelDB
{
	public interface IDatabase
	{
		DirectoryInfo Directory { get; }

		void Delete(Span<byte> key);

		void Put(Span<byte> key, Span<byte> value);

		byte[] Get(Span<byte> key);

		List<string> GetDbKeysStartingWith(string startWith);

		void Open();

		void Close();

		void Destroy();

		bool IsClosed();
	}

	public class Database : IDatabase
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Database));
		private ManifestReader _manifestReader;
		private LogReader _memCache;
		private Statistics _statistics = new Statistics();

		public DirectoryInfo Directory { get; private set; }

		public Database(DirectoryInfo dbDirectory)
		{
			Directory = dbDirectory;
		}

		public void Delete(Span<byte> key)
		{
			throw new NotImplementedException();
		}

		public void Put(Span<byte> key, Span<byte> value)
		{
			throw new NotImplementedException();
		}

		public byte[] Get(Span<byte> key)
		{
			if (_manifestReader == null) throw new Exception("No manifest for database. Did you open it?");
			if (_memCache == null) throw new Exception("No current memory cache for database. Did you open it?");

			ResultStatus result;
			result = _memCache.Get(key);
			if (result.State == ResultState.Deleted || result.State == ResultState.Exist)
			{
				if (result.Data == ReadOnlySpan<byte>.Empty) return null;
				return result.Data.ToArray();
			}

			result = _manifestReader.Get(key);
			if (result.Data == ReadOnlySpan<byte>.Empty) return null;
			return result.Data.ToArray();
		}

		public List<string> GetDbKeysStartingWith(string startWith)
		{
			throw new NotImplementedException();
		}

		public void Open()
		{
			if (Directory.Name.EndsWith(".mcworld"))
			{
				// Exported from MCPE. Unpack to temp

				Log.Debug($"Opening directory: {Directory.Name}");

				var originalFile = Directory;

				string newDirPath = Path.Combine(Path.GetTempPath(), Directory.Name);
				Directory = new DirectoryInfo(Path.Combine(newDirPath, "db"));
				if (!Directory.Exists || originalFile.LastWriteTimeUtc > Directory.LastWriteTimeUtc)
				{
					ZipFile.ExtractToDirectory(originalFile.FullName, newDirPath, true);
					Log.Warn($"Created new temp directory: {Directory.FullName}");
				}
			}

			// Verify that directory exists
			if (!Directory.Exists) throw new DirectoryNotFoundException(Directory.Name);

			// Read Manifest into memory

			string manifestFilename;
			using (var manifestStream = File.OpenText($@"{Path.Combine(Directory.FullName, "CURRENT")}"))
			{
				manifestFilename = manifestStream.ReadLine();
				manifestStream.Close();
			}

			Log.Debug($"Reading manifest from {Path.Combine(Directory.FullName, manifestFilename)}");
			_manifestReader = new ManifestReader(new FileInfo($@"{Path.Combine(Directory.FullName, manifestFilename)}"));

			// Read current log
			FileInfo f = new FileInfo(Path.Combine(Directory.FullName, $"{_manifestReader.ReadVersionEdit().LogNumber:000000}.log"));
			_memCache = new LogReader(f);
		}

		public void Close()
		{
			throw new NotImplementedException();
		}

		public void Destroy()
		{
			throw new NotImplementedException();
		}

		public bool IsClosed()
		{
			throw new NotImplementedException();
		}
	}

	public class Statistics
	{
		public int QuerySuccesses { get; set; }
		public int QueryFailes { get; set; }
		public int TableCacheHits { get; set; }
	}

	public enum LogTagType
	{
		Comparator = 1,
		LogNumber = 2,
		NextFileNumber = 3,
		LastSequence = 4,
		CompactPointer = 5,
		DeletedFile = 6,
		NewFile = 7,

		// 8 was used for large value refs
		PrevLogNumber = 9
	}

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

	public class FileMetadata
	{
		public ulong FileNumber { get; set; }
		public ulong FileSize { get; set; }
		public byte[] SmallestKey { get; set; }
		public byte[] LargestKey { get; set; }
	}

	public class ByteArrayConverter : JsonConverter
	{
		public override object ReadJson(
			JsonReader reader,
			Type objectType,
			object existingValue,
			JsonSerializer serializer)
		{
			throw new NotImplementedException();
		}

		public override void WriteJson(
			JsonWriter writer,
			object value,
			JsonSerializer serializer)
		{
			byte[] bytes = (byte[]) value;
			string base64String = bytes.HexDump(bytes.Length);

			serializer.Serialize(writer, base64String);
		}

		public override bool CanRead
		{
			get { return false; }
		}

		public override bool CanConvert(Type t)
		{
			return typeof(byte[]).IsAssignableFrom(t);
		}
	}

	public enum BlockTag
	{
		Dimension0 = 0,
		Dimension1 = 1,
		Dimension2 = 2,

		Data2D = 0x2D,
		Data2DLegacy = 0x2E,
		SubChunkPrefix = 0x2F,
		LegacyTerrain = 0x30,
		BlockEntity = 0x31,
		Entity = 0x32,
		PendingTicks = 0x33,
		BlockExtraData = 0x34,
		BiomeState = 0x35,

		Version = 0x76,

		Undefined = 0xFF
	};
}