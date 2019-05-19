using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using log4net;

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

			_newMemCache.Put(key, value);
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
				if (!CreateIfMissing) throw new DirectoryNotFoundException(Directory.Name);

				Directory.Create();

				// Create new MANIFEST

				VersionEdit newVersion = new VersionEdit();
				newVersion.NextFileNumber = 1;
				newVersion.Comparator = "leveldb.BytewiseComparator";

				// Create new CURRENT text file and store manifest filename in it
				using (var manifestStream = File.CreateText($@"{Path.Combine(Directory.FullName, "CURRENT")}"))
				{
					manifestStream.WriteLine($"MANIFEST-{newVersion.NextFileNumber++:000000}");
					manifestStream.Close();
				}


				// Done
			}

			// Read Manifest into memory

			string manifestFilename;
			using (var currentStream = File.OpenText($@"{Path.Combine(Directory.FullName, "CURRENT")}"))
			{
				manifestFilename = currentStream.ReadLine();
				currentStream.Close();
			}

			Log.Debug($"Reading manifest from {Path.Combine(Directory.FullName, manifestFilename)}");
			using (var reader = new LogReader(new FileInfo($@"{Path.Combine(Directory.FullName, manifestFilename)}")))
			{
				_manifest = new Manifest(Directory);
				_manifest.Load(reader);
			}

			// Read current log
			//TODO: remove unit-test-stuff
			var logFileName = Path.Combine(Directory.FullName, $"{_manifest.CurrentVersion.LogNumber + 1:000000}.log");
			FileInfo f = new FileInfo(logFileName);
			if (!f.Exists)
			{
				f = new FileInfo(Path.Combine(Directory.FullName, $"{_manifest.CurrentVersion.LogNumber:000000}.log"));
			}

			using (var reader = new LogReader(f))
			{
				_newMemCache = new MemCache();
				_newMemCache.Load(reader);
			}
		}

		public void Close()
		{
			if (_newMemCache != null)
			{
				var memCache = _newMemCache;
				_newMemCache = null;

				if (_manifest != null)
				{
					var nextLogNumber = _manifest.CurrentVersion.LogNumber + 1;
					using (var logWriter = new LogWriter(new FileInfo(Path.Combine(Directory.FullName, $"{nextLogNumber:000000}.log"))))
					{
						memCache.Write(logWriter);
					}
				}
			}

			if (_manifest != null)
			{
				var temp = _manifest;
				_manifest = null;
				temp.Close();
			}
		}

		public bool IsClosed()
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			Close();
		}
	}

	public class Statistics
	{
		public int QuerySuccesses { get; set; }
		public int QueryFailes { get; set; }
		public int TableCacheHits { get; set; }
	}
}