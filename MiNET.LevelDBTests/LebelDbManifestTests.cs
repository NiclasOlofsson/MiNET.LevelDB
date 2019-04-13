using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using NUnit.Framework;

namespace MiNET.LevelDBTests
{
	// https://github.com/basho/leveldb/wiki/mv-overview

	[TestFixture]
	public class LebelDbManifestTests
	{
		[SetUp]
		public void Init()
		{
			/* ... */
		}

		[TearDown]
		public void Cleanup()
		{
			file.Close();
			file = null;
		}

		[Test]
		public void LevelDbReadManifestTest()
		{
			var directory = @"D:\Temp\World Saves PE\WoUIAK-EAQA=\db\";
			//var directory = @"D:\Temp\World Saves PE\ExoGAHavAAA=\db\";

			var currentStream = File.OpenText($@"{directory}CURRENT");
			string manifestFilename = currentStream.ReadLine();
			currentStream.Close();

			var stream = File.OpenRead($@"{directory}{manifestFilename}");

			byte[] recordBytes = new byte[4 + 2 + 1];

			string comparator = null;
			ulong? logNumber = null;
			ulong? previousLogNumber = null;
			ulong? nextFileNumber = null;
			ulong? lastSequenceNumber = null;

			while (stream.Read(recordBytes, 0, recordBytes.Length) != 0)
			{
				var record = ReadRecord(recordBytes, stream);
				if (record.RecordType == RecordType.Full)
				{
					Log($"{record}");

					VersionEdit versionEdit = new VersionEdit();

					var seek = new BinaryReader(new MemoryStream(record.Data));
					while (seek.BaseStream.Position < seek.BaseStream.Length)
					{
						var persistentId = LevelDbTests.ReadVarInt32(seek.BaseStream);
						if (persistentId == 1)
						{
							// COMPARATOR

							versionEdit.Comparator = ReadLenghtPrefixedString(seek);
						}
						else if (persistentId == 2)
						{
							// LOG_NUMBER

							versionEdit.LogNumber = LevelDbTests.ReadVarInt32(seek.BaseStream);
						}
						else if (persistentId == 3)
						{
							// NEXT_FILE_NUMBER

							versionEdit.NextFileNumber = LevelDbTests.ReadVarInt32(seek.BaseStream);
						}
						else if (persistentId == 4)
						{
							// LAST_SEQUENCE

							versionEdit.LastSequenceNumber = LevelDbTests.ReadVarInt32(seek.BaseStream);
						}
						else if (persistentId == 5)
						{
							// COMPACT_POINTER

							int level = (int) LevelDbTests.ReadVarInt32(seek.BaseStream);
							InternalKey key = new InternalKey(ReadLenghtPrefixedBytes(seek));
							versionEdit.CompactPointers[level] = key;
						}
						else if (persistentId == 6)
						{
							// DELETED_FILE

							int level = (int) LevelDbTests.ReadVarInt32(seek.BaseStream);
							ulong fileNumber = LevelDbTests.ReadVarInt32(seek.BaseStream);
							versionEdit.DeletedFiles[level] = fileNumber;
						}
						else if (persistentId == 7)
						{
							// NEW_FILE

							int level = (int) LevelDbTests.ReadVarInt32(seek.BaseStream);
							ulong fileNumber = LevelDbTests.ReadVarInt32(seek.BaseStream);
							ulong fileSize = LevelDbTests.ReadVarInt32(seek.BaseStream);
							var smallest = new InternalKey(ReadLenghtPrefixedBytes(seek));
							var largest = new InternalKey(ReadLenghtPrefixedBytes(seek));

							FileMetadata fileMetadata = new FileMetadata();
							fileMetadata.FileNumber = fileNumber;
							fileMetadata.FileSize = fileSize;
							fileMetadata.SmallestKey = smallest;
							fileMetadata.LargestKey = largest;
							versionEdit.NewFiles[level] = fileMetadata;
						}
						else if (persistentId == 9)
						{
							// PREVIOUS_LOG_NUMBER

							versionEdit.PreviousLogNumber = LevelDbTests.ReadVarInt32(seek.BaseStream);
						}
						else
						{
							throw new Exception($"Unknown persistent ID={persistentId}");
						}
					}
					Print(versionEdit);

					comparator = versionEdit.Comparator ?? comparator;
					logNumber = versionEdit.LogNumber ?? logNumber;
					previousLogNumber = versionEdit.PreviousLogNumber ?? previousLogNumber;
					nextFileNumber = versionEdit.NextFileNumber ?? nextFileNumber;
					lastSequenceNumber = versionEdit.LastSequenceNumber ?? lastSequenceNumber;
				}
				else
				{
					Log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				}

				Log("------------------------------------------------------------");
			}

			VersionEdit finalVersion = new VersionEdit();
			finalVersion.Comparator = comparator;
			finalVersion.LogNumber = logNumber;
			finalVersion.PreviousLogNumber = previousLogNumber ?? 0;
			finalVersion.NextFileNumber = nextFileNumber;
			finalVersion.LastSequenceNumber = lastSequenceNumber;

			Log("============================================================");
			Print(finalVersion);
			Log("============================================================");
		}

		public static string ReadLenghtPrefixedString(BinaryReader seek)
		{
			ulong length = LevelDbTests.ReadVarInt32(seek.BaseStream);
			string s = Encoding.UTF8.GetString(seek.ReadBytes((int) length));
			return s;
		}

		public static byte[] ReadLenghtPrefixedBytes(BinaryReader seek)
		{
			ulong size = LevelDbTests.ReadVarInt32(seek.BaseStream);
			return seek.ReadBytes((int) size);
		}

		public static void Print(object obj)
		{
			var jsonSerializerSettings = new JsonSerializerSettings
			{
				PreserveReferencesHandling = PreserveReferencesHandling.Arrays,
				NullValueHandling = NullValueHandling.Ignore,
				Formatting = Formatting.Indented,
			};

			string result = JsonConvert.SerializeObject(obj, jsonSerializerSettings);
			Log($"{result}");
		}

		[Test]
		public void LevelDbReadLogTest()
		{
			var stream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000341.log");

			byte[] record = new byte[4 + 2 + 1];

			while (stream.Read(record, 0, record.Length) == record.Length)
			{
				var rec = ReadRecord(record, stream);
				//if (rec.RecordType != RecordType.Empty)
				if (rec.RecordType == RecordType.First || rec.RecordType == RecordType.Full)
				{
					Log($"{rec}");
					var seek = new BinaryReader(new MemoryStream(rec.Data));

					long sequenceNumber = seek.ReadInt64();
					long size = seek.ReadInt32();

					while (seek.BaseStream.Position < seek.BaseStream.Length)
					{
						byte recType = seek.ReadByte();

						var v1 = LevelDbTests.ReadVarInt32(seek.BaseStream);
						byte[] currentKey = new byte[v1];
						seek.Read(currentKey, 0, (int) v1);

						ulong v2 = 0;
						byte[] currentVal = new byte[0];
						if (recType == 1)
						{
							v2 = LevelDbTests.ReadVarInt32(seek.BaseStream);
							currentVal = new byte[v2];
							seek.Read(currentVal, 0, (int) v2);
						}

						Log($"RecType={recType}, Sequence={sequenceNumber}, Size={size}, v1={v1}, v2={v2}\nCurrentKey={LevelDbTests.HexDump(currentKey, currentKey.Length, false, false)}\nCurrentVal=\n{LevelDbTests.HexDump(currentVal, cutAfterFive: true)} ");
					}
					Log("------------------------------------------------------------");
				}
			}
		}

		static StreamWriter file;

		private static void Log(string s)
		{
			if (file == null)
			{
				file = new StreamWriter(@"D:\Temp\test_log.txt", true);
			}

			file.WriteLine(s.TrimEnd());
		}

		private Record ReadRecord(byte[] record, Stream stream)
		{
			//Console.WriteLine($"{LebelDbTests.HexDump(record)}");

			uint checksum = BitConverter.ToUInt32(record, 0);
			ushort length = BitConverter.ToUInt16(record, 4);
			byte type = record[6];
			byte[] data = new byte[length];
			stream.Read(data, 0, data.Length);

			Record rec = new Record()
			{
				Checksum = checksum,
				Length = length,
				RecordType = (RecordType) type,
				Data = data
			};

			return rec;
		}
	}

	public class InternalKey
	{
		public byte[] Key { get; }

		public InternalKey(byte[] key)
		{
			Key = key;
		}
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
		public Dictionary<int, FileMetadata> NewFiles { get; set; } = new Dictionary<int, FileMetadata>();
	}

	public class FileMetadata
	{
		public ulong FileNumber { get; set; }
		public ulong FileSize { get; set; }
		public InternalKey SmallestKey { get; set; }
		public InternalKey LargestKey { get; set; }
	}

	public enum RecordType
	{
		Zero = 0,
		Full = 1,
		First = 2,
		Middle = 3,
		Last = 4,
		Eof = Last + 1,
		BadRecord = Last + 2,
	}

	public struct Record
	{
		public uint Checksum { get; set; }
		public ushort Length { get; set; }
		public RecordType RecordType { get; set; }
		public byte[] Data { get; set; }

		public override string ToString()
		{
			return $"{nameof(Length)}: {Length}, {nameof(RecordType)}: {RecordType}"
					+ $", {nameof(Data)}: \n{LevelDbTests.HexDump(Data, cutAfterFive: Data.Length > 16*10)}"
				;
		}
	}
}