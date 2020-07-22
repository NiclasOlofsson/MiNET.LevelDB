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
using log4net;
using MiNET.LevelDB.Utils;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
{
	[TestFixture]
	public class LevelDbTableTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LevelDbTableTests));

		byte[] _indicatorChars = {0x64, 0x69, 0x6d, 0x65, 0x6e, 0x73, 0x6f, 0x6e, 0x30};

		[Test]
		public void LevelDbReadFindInTableTest()
		{
			FileInfo fileInfo = new FileInfo(@"TestWorld\000050.ldb");
			ResultStatus result;
			using (Table table = new Table(fileInfo))
			{
				result = table.Get(new byte[] {0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2f, 0x00,});
			}

			if (result.Data != null)
			{
				if (Log.IsDebugEnabled) Log.Debug("Result:\n" + result.Data.HexDump(cutAfterFive: true));
				return;
			}

			Assert.Fail("Found no entry");
		}

		[Test]
		public void WriteLevel0TableTest()
		{
			using var logReader = new LogReader(new FileInfo(@"TestWorld\000047.log"));
			var memCache = new MemCache();
			memCache.Load(logReader);

			var newFileInfo = new FileInfo(Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".ldb"));
			var db = new Database(null);
			db.WriteLevel0Table(memCache, newFileInfo);

			var table = new Table(newFileInfo);

			//Key:fe ff ff ff f1 ff ff ff 76 
			ResultStatus result = table.Get(new byte[] {0xfe, 0xff, 0xff, 0xff, 0xf1, 0xff, 0xff, 0xff, 0x76});
			Assert.AreEqual(ResultState.Exist, result.State);

			//Key:fa 40 ab 14 4d 96 ec 7b 62 38 f7 63
			result = table.Get(new byte[] {0xfa, 0x40, 0xab, 0x14, 0x4d, 0x96, 0xec, 0x7b, 0x62, 0x38, 0xf7, 0x63});
			Assert.AreEqual(ResultState.NotFound, result.State);

			//Key:fd ff ff ff f1 ff ff ff 39  28036, False, size:0
			result = table.Get(new byte[] {0xfd, 0xff, 0xff, 0xff, 0xf1, 0xff, 0xff, 0xff, 0x39});
			Assert.AreEqual(ResultState.Deleted, result.State);

			foreach (KeyValuePair<byte[], MemCache.ResultCacheEntry> entry in memCache._resultCache.OrderBy(kvp => kvp.Key, new BytewiseComparator()))
			{
				if (entry.Value.ResultState != ResultState.Exist)
					continue;

				byte[] key = entry.Key;
				byte[] data = entry.Value.Data;

				result = table.Get(key);
				Assert.AreEqual(ResultState.Exist, result.State);
				Assert.AreEqual(data, result.Data.ToArray());
			}
		}


		[Test]
		public void DupCompressionTest()
		{
			// This is just an off-topic experiment. Allocation free compression, reusing the same buffer for input
			// and output. Also works for decompression if uncompressed size is know beforehand.

			var buffer = FillArrayWithRandomBytes(1234, 10000, 10).AsMemory();
			var originalBuffer = buffer.ToArray();
			var firstBytes = buffer.Slice(0, 10).ToArray();

			var inStream = new BufferStream(buffer);

			var compressStream = new DeflateStream(inStream, CompressionLevel.Optimal, true);
			compressStream.Write(buffer.Span);
			Log.Debug("Flushing");
			compressStream.Flush();

			var output = inStream.GetBuffer();
			var inFirstBytes = output.Slice(0, 10);
			Assert.AreNotEqual(firstBytes.ToHexString(), inFirstBytes.ToHexString());

			long len = inStream.Position;
			Assert.AreEqual(5127, len);

			var compressedMem = new BufferStream(buffer.Slice(0, (int) len));
			var decompressStream = new DeflateStream(compressedMem, CompressionMode.Decompress);

			var outStream = new BufferStream(buffer);
			decompressStream.CopyTo(outStream);

			var inFinalBytes = outStream.GetBuffer().Slice(0, 10);
			Assert.AreEqual(firstBytes.ToHexString(), inFinalBytes.ToHexString());
			Assert.AreEqual(originalBuffer.ToHexString(), outStream.GetBuffer().ToHexString());
		}

		public static byte[] FillArrayWithRandomBytes(int seed, int size, int max)
		{
			var bytes = new byte[size];
			var random = new Random(seed);
			for (int i = 0; i < bytes.Length; i++)
			{
				bytes[i] = (byte) random.Next(max);
			}

			return bytes;
		}


		public class BufferStream : Stream
		{
			private static readonly ILog Log = LogManager.GetLogger(typeof(BufferStream));

			private readonly Memory<byte> _buffer;
			private long _position = 0;
			private long _length = 0;

			public BufferStream(Memory<byte> buffer)
			{
				_buffer = buffer;
				_length = buffer.Length;
			}

			public override void Flush()
			{
				throw new NotImplementedException();
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				Log.Debug($"Read: {_position} {offset}, {count}");
				int readLen = (int) Math.Min(count, _length - Position);
				_buffer.Slice((int) _position, readLen).Span.CopyTo(buffer);
				//Buffer.BlockCopy(_buffer, (int) _position, buffer, offset, readLen);
				_position += readLen;
				return readLen;
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				throw new NotImplementedException();
				return 0;
			}

			public override void SetLength(long value)
			{
				throw new NotImplementedException();
			}

			public override void Write(byte[] buffer, int offset, int count)
			{
				Log.Debug($"Write: {_position} {offset}, {count}, {_buffer.Slice((int) _position, count).Span.Length}");
				buffer.AsSpan(0, count).CopyTo(_buffer.Slice((int) _position, count).Span);
				_length += count;
				_position += count;
			}

			public override bool CanRead { get; } = true;
			public override bool CanSeek { get; } = false;
			public override bool CanWrite { get; } = true;

			public override long Length
			{
				get
				{
					return _length;
				}
			}

			public override long Position
			{
				get => _position;
				set => _position = value;
			}

			public Memory<byte> GetBuffer()
			{
				return _buffer;
			}
		}
	}
}