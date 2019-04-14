using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;

namespace MiNET.LevelDB
{
	public interface ILevelDb
	{
		DirectoryInfo Directory { get; }

		void Delete(byte[] key);

		void Put(byte[] key, byte[] value);

		byte[] Get(byte[] key);

		List<String> GetDbKeysStartingWith(String startWith);

		void Open();

		void Close();

		void Destroy();

		bool IsClosed();
	}

	class LevelDb : ILevelDb
	{
		public DirectoryInfo Directory { get; }

		public LevelDb(DirectoryInfo dbDirectory)
		{
			Directory = dbDirectory;
		}

		public void Delete(byte[] key)
		{
			throw new NotImplementedException();
		}

		public void Put(byte[] key, byte[] value)
		{
			throw new NotImplementedException();
		}

		public byte[] Get(byte[] key)
		{
			throw new NotImplementedException();
		}

		public List<string> GetDbKeysStartingWith(string startWith)
		{
			throw new NotImplementedException();
		}

		public void Open()
		{
			throw new NotImplementedException();
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

	public class Footer
	{
		public static readonly byte[] Magic = {0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb};
	}

	public class InternalKey
	{
		public byte[] Key { get; }

		public InternalKey(byte[] key)
		{
			Key = key;
		}
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
		public Dictionary<int, InternalKey> CompactPointers { get; set; } = new Dictionary<int, InternalKey>();
		public Dictionary<int, ulong> DeletedFiles { get; set; } = new Dictionary<int, ulong>();
		public Dictionary<int, List<FileMetadata>> NewFiles { get; set; } = new Dictionary<int, List<FileMetadata>>();
	}

	public class FileMetadata
	{
		public ulong FileNumber { get; set; }
		public ulong FileSize { get; set; }
		public InternalKey SmallestKey { get; set; }
		public InternalKey LargestKey { get; set; }
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