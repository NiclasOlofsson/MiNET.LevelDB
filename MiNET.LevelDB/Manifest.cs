#region LICENSE

// The contents of this file are subject to the Common Public Attribution
// License Version 1.0. (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
// https://github.com/NiclasOlofsson/MiNET/blob/master/LICENSE.
// The License is based on the Mozilla Public License Version 1.1, but Sections 14
// and 15 have been added to cover use of software over a computer network and
// provide for limited attribution for the Original Developer. In addition, Exhibit A has
// been modified to be consistent with Exhibit B.
// 
// Software distributed under the License is distributed on an "AS IS" basis,
// WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
// the specific language governing rights and limitations under the License.
// 
// The Original Code is MiNET.
// 
// The Original Developer is the Initial Developer.  The Initial Developer of
// the Original Code is Niclas Olofsson.
// 
// All portions of the code written by Niclas Olofsson are Copyright (c) 2014-2020 Niclas Olofsson.
// All Rights Reserved.

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using MiNET.LevelDB.Utils;
using Newtonsoft.Json;

namespace MiNET.LevelDB
{
	/// <summary>
	///     The first layer is the "manifest". Every table file has an entry in the manifest. The manifest entry tracks the
	///     first and last key contained in each table file. The manifest keeps the table file entries in one of seven sorted
	///     arrays. Each of the seven arrays represents one "level" of table files. A user request for a key causes leveldb to
	///     check each table file that overlaps the target key. leveldb searches each potential table file, level by level,
	///     until finding the first that yields an exact match for requested key.
	/// </summary>
	public class Manifest : IDisposable
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Manifest));

		private readonly DirectoryInfo _baseDirectory;
		private BytewiseComparator _comparator = new BytewiseComparator();

		public Version CurrentVersion { get; internal set; }

		public Manifest(DirectoryInfo baseDirectory)
		{
			_baseDirectory = baseDirectory;
		}

		public void Load(LogReader reader)
		{
			if (CurrentVersion != null) return;

			CurrentVersion = ReadVersionEdit(reader);
			Log.Debug($"Loading manifest");
			Print(Log, CurrentVersion);

			foreach (var level in CurrentVersion.Levels) // Search all levels for file with matching index
			{
				foreach (FileMetadata tbl in level.Value)
				{
					Table tableReader = tbl.Table;
					if (tableReader == null)
					{
						tableReader = GetTable(tbl.FileNumber);
						tableReader.Initialize();
						tbl.Table = tableReader;
					}
				}
			}

			if (!"leveldb.BytewiseComparator".Equals(CurrentVersion.Comparator, StringComparison.InvariantCultureIgnoreCase))
				throw new Exception($"Found record, but contains invalid or unsupported comparator: {CurrentVersion.Comparator}");
		}

		public void Save(LogWriter writer)
		{
			if (CurrentVersion == null) return;

			var bytes = EncodeVersion(CurrentVersion);
			writer.WriteData(bytes);
		}

		public ResultStatus Get(Span<byte> key)
		{
			foreach (var level in CurrentVersion.Levels) // Search all levels for file with matching index
			{
				foreach (FileMetadata tbl in level.Value)
				{
					if (Log.IsDebugEnabled) Log.Debug($"Checking table {tbl.FileNumber} for key: {key.ToHexString()}");
					Span<byte> smallestKey = tbl.SmallestKey.AsSpan().UserKey();
					Span<byte> largestKey = tbl.LargestKey.AsSpan().UserKey();

					if (_comparator.Compare(key, smallestKey) >= 0 && _comparator.Compare(key, largestKey) <= 0)
					{
						if (Log.IsDebugEnabled) Log.Debug($"Found table file for key in level {level.Key} in file={tbl.FileNumber}, Smallest:{tbl.SmallestKey.ToHexString()}, Largest:{tbl.LargestKey.ToHexString()}");

						Table tableReader = tbl.Table;
						if (tableReader == null)
						{
							tableReader = GetTable(tbl.FileNumber);
							tableReader.Initialize();
							tbl.Table = tableReader;
						}

						ResultStatus result = tableReader.Get(key);
						if (result.State == ResultState.Exist || result.State == ResultState.Deleted) return result;
					}
				}
			}

			if (Log.IsDebugEnabled) Log.Debug($"Found no table for key: {key.ToHexString()}");

			return ResultStatus.NotFound;
		}

		internal Table GetTable(ulong fileNumber)
		{
			var file = new FileInfo(Path.Combine(_baseDirectory.FullName, $"{fileNumber:000000}.ldb"));
			if (!file.Exists) throw new Exception($"Could not find table {file.FullName}");
			var table = new Table(file);
			return table;
		}

		public static Version ReadVersionEdit(LogReader logReader)
		{
			var version = new Version();

			while (true)
			{
				ReadOnlySpan<byte> data = logReader.ReadData();

				if (logReader.Eof) break;

				var reader = new SpanReader(data);

				//var versionEdit = new VersionEdit();

				while (!reader.Eof)
				{
					var logTag = (LogTagType) reader.ReadVarLong();
					switch (logTag)
					{
						case LogTagType.Comparator:
						{
							version.Comparator = reader.ReadLengthPrefixedString();
							break;
						}
						case LogTagType.LogNumber:
						{
							version.LogNumber = reader.ReadVarLong();
							break;
						}
						case LogTagType.NextFileNumber:
						{
							version.NextFileNumber = reader.ReadVarLong();
							break;
						}
						case LogTagType.LastSequence:
						{
							version.LastSequenceNumber = reader.ReadVarLong();
							break;
						}
						case LogTagType.CompactPointer:
						{
							int level = (int) reader.ReadVarLong();
							var internalKey = reader.ReadLengthPrefixedBytes();
							version.CompactPointers[level] = internalKey.ToArray();
							break;
						}
						case LogTagType.DeletedFile:
						{
							int level = (int) reader.ReadVarLong();
							ulong fileNumber = reader.ReadVarLong();

							if (!version.DeletedFiles.ContainsKey(level)) version.DeletedFiles[level] = new List<ulong>();
							version.DeletedFiles[level].Add(fileNumber);
							break;
						}
						case LogTagType.NewFile:
						{
							int level = (int) reader.ReadVarLong();
							ulong fileNumber = reader.ReadVarLong();
							ulong fileSize = reader.ReadVarLong();
							ReadOnlySpan<byte> smallest = reader.ReadLengthPrefixedBytes();
							ReadOnlySpan<byte> largest = reader.ReadLengthPrefixedBytes();

							var fileMetadata = new FileMetadata();
							fileMetadata.FileNumber = fileNumber;
							fileMetadata.FileSize = fileSize;
							fileMetadata.SmallestKey = smallest.ToArray();
							fileMetadata.LargestKey = largest.ToArray();
							if (!version.Levels.ContainsKey(level)) version.Levels[level] = new List<FileMetadata>();
							version.Levels[level].Add(fileMetadata);
							break;
						}
						case LogTagType.PrevLogNumber:
						{
							version.PreviousLogNumber = reader.ReadVarLong();
							break;
						}
						default:
						{
							throw new ArgumentOutOfRangeException($"Unknown tag={logTag}");
						}
					}
				}
			}

			// Clean files
			var deletedFiles = new List<ulong>();
			foreach (List<ulong> versionDeletedFile in version.DeletedFiles.Values)
			{
				deletedFiles.AddRange(versionDeletedFile);
			}

			foreach (KeyValuePair<int, List<FileMetadata>> levelKvp in version.Levels)
			{
				foreach (FileMetadata newFile in levelKvp.Value.ToArray())
				{
					if (deletedFiles.Contains(newFile.FileNumber)) levelKvp.Value.Remove(newFile);
				}
			}

			version.Levels = version.Levels.OrderBy(kvp => kvp.Key).ToDictionary(pair => pair.Key, pair => pair.Value);
			version.Comparator ??= "leveldb.BytewiseComparator";

			return version;
		}

		public static Span<byte> EncodeVersion(Version version)
		{
			var array = new byte[4096];
			var buffer = new Span<byte>(array);
			var writer = new SpanWriter(buffer);

			//	case Manifest.LogTagType.Comparator:
			if (!string.IsNullOrEmpty(version.Comparator))
			{
				writer.WriteVarLong((ulong) LogTagType.Comparator);
				writer.Write(version.Comparator);
			}
			//	case Manifest.LogTagType.LogNumber:
			{
				writer.WriteVarLong((ulong) LogTagType.LogNumber);
				writer.WriteVarLong((ulong) version.LogNumber);
			}
			//	case Manifest.LogTagType.PrevLogNumber:
			{
				writer.WriteVarLong((ulong) LogTagType.PrevLogNumber);
				writer.WriteVarLong((ulong) version.PreviousLogNumber);
			}
			//	case Manifest.LogTagType.NextFileNumber:
			{
				writer.WriteVarLong((ulong) LogTagType.NextFileNumber);
				writer.WriteVarLong((ulong) version.NextFileNumber);
			}
			//	case Manifest.LogTagType.LastSequence:
			{
				writer.WriteVarLong((ulong) LogTagType.LastSequence);
				writer.WriteVarLong((ulong) version.LastSequenceNumber);
			}
			//	case Manifest.LogTagType.CompactPointer:
			if (version.CompactPointers.Count > 0)
			{
				foreach (KeyValuePair<int, byte[]> pointer in version.CompactPointers)
				{
					writer.WriteVarLong((ulong) LogTagType.CompactPointer);
					writer.WriteVarLong((ulong) pointer.Key);
					writer.WriteLengthPrefixed(pointer.Value);
				}
			}
			//	case Manifest.LogTagType.DeletedFile:
			//if (version.DeletedFiles.Count > 0)
			//{
			//	foreach (KeyValuePair<int, List<ulong>> files in version.DeletedFiles)
			//	{
			//		foreach (ulong fileNumber in files.Value)
			//		{
			//			writer.WriteVarLong((ulong) LogTagType.DeletedFile);
			//			writer.WriteVarLong((ulong) files.Key);
			//			writer.WriteVarLong(fileNumber);
			//		}
			//	}
			//}
			//	case Manifest.LogTagType.NewFile:
			if (version.Levels.Count > 0)
			{
				foreach (KeyValuePair<int, List<FileMetadata>> files in version.Levels)
				{
					int level = files.Key;
					foreach (FileMetadata fileMeta in files.Value)
					{
						writer.WriteVarLong((ulong) LogTagType.NewFile);
						//int level = (int) reader.ReadVarLong();
						writer.WriteVarLong((ulong) level);
						//ulong fileNumber = reader.ReadVarLong();
						writer.WriteVarLong((ulong) fileMeta.FileNumber);
						//ulong fileSize = reader.ReadVarLong();
						writer.WriteVarLong((ulong) fileMeta.FileSize);
						//var smallest = reader.ReadLengthPrefixedBytes();
						writer.WriteLengthPrefixed(fileMeta.SmallestKey);
						//var largest = reader.ReadLengthPrefixedBytes();
						writer.WriteLengthPrefixed(fileMeta.LargestKey);
					}
				}
			}

			int length = writer.Position;
			return buffer.Slice(0, length).ToArray();
		}


		public static void Print(ILog log, object obj)
		{
			if (!log.IsDebugEnabled) return;

			var jsonSerializerSettings = new JsonSerializerSettings
			{
				PreserveReferencesHandling = PreserveReferencesHandling.Arrays,
				NullValueHandling = NullValueHandling.Ignore,
				Formatting = Formatting.Indented,
				Converters = {new ByteArrayConverter()}
			};

			string result = JsonConvert.SerializeObject(obj, jsonSerializerSettings);
			log.Debug($"\n{result}");
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

		public void FindFileWithKey(Span<byte> key)
		{
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

		private bool _disposed = false;

		public void Close()
		{
			if (_disposed) return;
			_disposed = true;

			var tableFiles = CurrentVersion.Levels;
			CurrentVersion = null;
			foreach (KeyValuePair<int, List<FileMetadata>> level in tableFiles) // Search all levels for file with matching index
			{
				foreach (FileMetadata tbl in level.Value)
				{
					tbl.Table?.Dispose();
					tbl.Table = null;
				}
			}
		}

		public void Dispose()
		{
			Close();
		}
	}
}