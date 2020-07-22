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
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using log4net;
using MiNET.LevelDB.Utils;

[assembly: InternalsVisibleTo("MiNET.LevelDB.Tests")]

namespace MiNET.LevelDB
{
	public interface IDatabase : IDisposable
	{
		DirectoryInfo Directory { get; }

		void Delete(Span<byte> key);

		void Put(Span<byte> key, Span<byte> value);

		byte[] Get(Span<byte> key);

		List<string> GetDbKeysStartingWith(string startWith);

		void Open();

		void Close();

		bool IsClosed();
	}

	public class Database : IDatabase
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Database));
		private Manifest _manifest;
		private MemCache _newMemCache;
		private LogWriter _log;
		private Statistics _statistics = new Statistics();

		public DirectoryInfo Directory { get; private set; }
		public bool CreateIfMissing { get; set; } = false;

		public static bool ParanoidMode { get; set; }

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
			if (_manifest == null) throw new InvalidOperationException("No manifest for database. Did you open it?");
			if (_newMemCache == null) throw new InvalidOperationException("No current memory cache for database. Did you open it?");


			ulong sequenceNumber = _manifest.CurrentVersion.LastSequenceNumber++ ?? 0; //TODO: Make threadsafe!

			var batch = new WriteBatch
			{
				Sequence = sequenceNumber,
			};
			batch.Operations.Add(new BatchOperation()
			{
				Key = key.ToArray(),
				Data = value.ToArray(),
				ResultState = ResultState.Exist
			});

			// Write to LOG here.
			_log?.WriteData(batch.EncodeBatch());
			_newMemCache.Put(key, value);

			//CompactMemCache();
		}

		public byte[] Get(Span<byte> key)
		{
			if (_manifest == null) throw new InvalidOperationException("No manifest for database. Did you open it?");
			if (_newMemCache == null) throw new InvalidOperationException("No current memory cache for database. Did you open it?");

			ResultStatus result = _newMemCache.Get(key);
			if (result.State == ResultState.Deleted || result.State == ResultState.Exist)
			{
				if (result.Data == ReadOnlySpan<byte>.Empty) return null;
				return result.Data.ToArray();
			}

			result = _manifest.Get(key);
			if (result.Data == ReadOnlySpan<byte>.Empty) return null;
			return result.Data.ToArray();
		}

		public List<string> GetDbKeysStartingWith(string startWith)
		{
			throw new NotImplementedException();
		}

		public void Open()
		{
			if (_manifest != null) throw new InvalidOperationException("Already had manifest for database. Did you already open it?");
			if (_newMemCache != null) throw new InvalidOperationException("Already had memory cache for database. Did you already open it?");

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

				Log.Warn($"Extracted bedrock world and set new DB directory to: {Directory.FullName}");
			}

			// Verify that directory exists
			if (!Directory.Exists)
			{
				if (!CreateIfMissing)
				{
					var notFoundException = new DirectoryNotFoundException(Directory.FullName);
					Log.Error(notFoundException);
					throw notFoundException;
				}

				Directory.Create();
				Directory.Refresh();

				// Create new MANIFEST

				var manifest = new Manifest(Directory);
				manifest.CurrentVersion = new VersionEdit()
				{
					Comparator = "leveldb.BytewiseComparator",
					LogNumber = 1,
					PreviousLogNumber = 0,
					NextFileNumber = 2,
					LastSequenceNumber = 1
				};
				string filename = $"MANIFEST-000001";
				using var writer = new LogWriter(new FileInfo($@"{Path.Combine(Directory.FullName, filename)}"));
				manifest.Save(writer);
				manifest.Close();

				// Create new CURRENT text file and store manifest filename in it
				using StreamWriter current = File.CreateText($@"{Path.Combine(Directory.FullName, "CURRENT")}");
				current.WriteLine(filename);
				current.Close();

				// Done and created
			}

			Directory.Refresh(); // If this has been manipulated on the way, this is really needed.

			// Read Manifest into memory

			string manifestFilename = GetCurrentManifestFile();
			Log.Debug($"Reading manifest from {Path.Combine(Directory.FullName, manifestFilename)}");
			using (var reader = new LogReader(new FileInfo($@"{Path.Combine(Directory.FullName, manifestFilename)}")))
			{
				_manifest = new Manifest(Directory);
				_manifest.Load(reader);
			}

			// Read current log
			var logFile = new FileInfo(Path.Combine(Directory.FullName, _manifest.CurrentVersion.GetLogFileName()));
			Log.Debug($"Reading log from {logFile.FullName}");
			using (var reader = new LogReader(logFile))
			{
				_newMemCache = new MemCache();
				_newMemCache.Load(reader);
			}

			_log = new LogWriter(logFile);

			// We do this on startup. It will rotate the log files and create
			// level 0 tables. However, we want to use into reusing the logs.
			CompactMemCache(true);
		}

		public void Close()
		{
			if (_newMemCache != null)
			{
				var memCache = _newMemCache;
				_newMemCache = null;

				//if (_manifest != null)
				//{
				//	//TODO: Save of log should happen continuous (async) when doing Put() operations.
				//	using var logWriter = new LogWriter(new FileInfo(Path.Combine(Directory.FullName, $"{(_manifest.CurrentVersion.LogNumber ?? 0):000000}.log")));
				//	memCache.Write(logWriter);
				//}
			}
			_log?.Close();
			_log = null;
			_manifest?.Close();
			_manifest = null;
		}

		public bool IsClosed()
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			Close();
		}

		private void CompactMemCache(bool force = false)
		{
			if (!force && _newMemCache.GetEstimatedSize() < 4000000L) return; // 4Mb

			if (_newMemCache._resultCache.Count == 0) return;

			Log.Warn($"Compact kicking in");

			// Lock memcache for write

			// Write prev memcache to a level 0 table

			VersionEdit currentVersion = _manifest.CurrentVersion;
			ulong newFileNumber = currentVersion.GetNewFileNumber();

			var tableFileInfo = new FileInfo(Path.Combine(Directory.FullName, $"{newFileNumber:000000}.ldb"));
			FileMetadata meta = WriteLevel0Table(_newMemCache, tableFileInfo);
			var newTable = new Table(tableFileInfo);
			meta.FileNumber = newFileNumber;
			meta.Table = newTable;

			// Update version data and commit new manifest (save)

			var newVersion = new VersionEdit(currentVersion);
			newVersion.LogNumber++;
			newVersion.PreviousLogNumber = 0; // Not used anymore
			newVersion.AddNewFile(0, meta);

			Manifest.Print(newVersion);

			// replace current memcache with new version. Should probably do this after manifest is confirmed written ok.
			var newCache = new MemCache();
			_newMemCache = newCache;

			// Replace log file with new one
			string logFileToDelete = currentVersion.GetLogFileName();
			var logFile = new FileInfo(Path.Combine(Directory.FullName, newVersion.GetLogFileName()));
			LogWriter oldLog = _log;
			_log = new LogWriter(logFile);
			oldLog?.Close();
			// Remove old log file
			File.Delete(Path.Combine(Directory.FullName, logFileToDelete));

			// Update manifest with new version
			string manifestFileToDelete = GetCurrentManifestFile();
			_manifest.CurrentVersion = newVersion;
			string manifestFilename = $"MANIFEST-{newVersion.GetNewFileNumber():000000}";
			using (var writer = new LogWriter(new FileInfo($@"{Path.Combine(Directory.FullName, manifestFilename)}")))
			{
				_manifest.Save(writer);
			}

			using (StreamWriter currentStream = File.CreateText($@"{Path.Combine(Directory.FullName, "CURRENT")}"))
			{
				currentStream.WriteLine(manifestFilename);
				currentStream.Close();
			}

			// Remove old manifest file
			File.Delete($@"{Path.Combine(Directory.FullName, manifestFileToDelete)}");

			// unlock
		}

		private string GetCurrentManifestFile()
		{
			using StreamReader currentStream = File.OpenText($@"{Path.Combine(Directory.FullName, "CURRENT")}");
			return currentStream.ReadLine();
		}

		internal FileMetadata WriteLevel0Table(MemCache memCache, FileInfo tableFileInfo)
		{
			using FileStream fileStream = File.Create(tableFileInfo.FullName);
			var creator = new TableCreator(fileStream);

			byte[] smallestKey = null;
			byte[] largestKey = null;

			foreach (KeyValuePair<byte[], MemCache.ResultCacheEntry> entry in memCache._resultCache.OrderBy(kvp => kvp.Key, new BytewiseComparator()).ThenBy(kvp => kvp.Value.Sequence))
			{
				if (entry.Value.ResultState != ResultState.Exist && entry.Value.ResultState != ResultState.Deleted) continue;

				byte[] opAndSeq = BitConverter.GetBytes((ulong) entry.Value.Sequence);
				opAndSeq[0] = (byte) (entry.Value.ResultState == ResultState.Exist ? 1 : 0);

				byte[] key = entry.Key.Concat(opAndSeq).ToArray();
				byte[] data = entry.Value.Data;

				smallestKey ??= key;
				largestKey = key;

				if (Log.IsDebugEnabled)
				{
					if (entry.Value.ResultState == ResultState.Deleted)
						Log.Warn($"Key:{key.ToHexString()} {entry.Value.Sequence}, {entry.Value.ResultState == ResultState.Exist}, size:{entry.Value.Data?.Length ?? 0}");
					else
						Log.Debug($"Key:{key.ToHexString()} {entry.Value.Sequence}, {entry.Value.ResultState == ResultState.Exist}");
				}

				creator.Add(key, data);
			}

			creator.Finish();
			long fileSize = fileStream.Length;
			fileStream.Close();

			Log.Debug($"Size distinct:{memCache._resultCache.Distinct().Count()}");
			Log.Debug($"Wrote {memCache._resultCache.Count} values");

			return new FileMetadata
			{
				FileNumber = 0, // Set in calling method
				FileSize = (ulong) fileSize,
				SmallestKey = smallestKey,
				LargestKey = largestKey,
				Table = null
			};
		}
	}

	internal class BatchOperation
	{
		public byte[] Key { get; set; }
		public byte[] Data { get; set; }
		public ResultState ResultState { get; set; } = ResultState.Undefined;
	}

	internal class WriteBatch
	{
		public ulong Sequence { get; set; }
		public List<BatchOperation> Operations = new List<BatchOperation>();

		internal ReadOnlySpan<byte> EncodeBatch()
		{
			if (Operations.Count == 0) throw new ArgumentException("Zero size batch", nameof(Operations));

			long maxSize = 0;
			maxSize += 8; // sequence
			maxSize += 4 * Operations.Count; // count
			foreach (BatchOperation entry in Operations)
			{
				maxSize += 1; // op code
				maxSize += 10; // varint max
				maxSize += entry.Key.Length;
				if (entry.ResultState == ResultState.Exist)
				{
					maxSize += 10; // varint max
					maxSize += entry.Data?.Length ?? 0;
				}
			}

			Span<byte> data = new byte[maxSize]; // big enough to contain all data regardless of size

			var writer = new SpanWriter(data);

			// write sequence
			writer.Write((ulong) Sequence);
			// write operations count
			writer.Write((uint) Operations.Count);

			foreach (var operation in Operations)
			{
				byte[] key = operation.Key;
				// write op type (byte)
				writer.Write(operation.ResultState == ResultState.Exist ? (byte) OperationType.Value : (byte) OperationType.Delete);
				// write key
				writer.WriteLengthPrefixed(key);

				if (operation.ResultState == ResultState.Exist)
				{
					// write data
					writer.WriteLengthPrefixed(operation.Data);
				}
			}

			return data.Slice(0, writer.Position);
		}
	}


	public class Statistics
	{
		public int QuerySuccesses { get; set; }
		public int QueryFailes { get; set; }
		public int TableCacheHits { get; set; }
	}
}