using System;
using System.Collections.Generic;
using System.Text;
using log4net;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
{
	[TestFixture]
	public class BloomFilterPolicyTest
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(BloomFilterPolicyTest));

		public static readonly int BLOOM_BITS = 10;
		private byte[] _filter = new byte[0];
		private List<byte[]> _keys = new List<byte[]>();
		private BloomFilterPolicy _policy = new BloomFilterPolicy(BLOOM_BITS);

		[Test]
		public void EmptyBloom()
		{
			Assert.IsTrue(!Matches(""));
			Assert.IsTrue(!Matches("hello"));
			Assert.IsTrue(!Matches("world"));
		}

		[Test]
		public void SmallBloom()
		{
			Add("hello");
			Add("world");
			Assert.IsTrue(Matches("hello"), "Key should be found");
			Assert.IsTrue(Matches("world"), "Key should be sound");
			Assert.IsTrue(!Matches("x"));
			Assert.IsTrue(!Matches("foo"));
		}

		[Test]
		public void TestVariableLength()
		{
			// Count number of filters that significantly exceed the false positive rate
			int mediocreFilters = 0;
			int goodFilters = 0;

			for (int length = 1; length <= 10000; length = NextLength(length))
			{
				Reset();

				for (uint i = 0; i < length; i++)
				{
					_keys.Add(BitConverter.GetBytes(i));
				}

				Build();

				Assert.LessOrEqual(_filter.Length, (length * BLOOM_BITS / 8) + 40);

				// All added keys must match
				for (uint i = 0; i < length; i++)
				{
					Assert.IsTrue(Matches(BitConverter.GetBytes(i)));
				}

				// Check false positive rate
				double rate = FalsePositiveRate();
				Log.Debug($"False positives: {rate * 100.0}%, length={length}, bytesLength={_filter.Length}");

				Assert.LessOrEqual(rate, 0.02);
				if (rate > 0.0125)
				{
					mediocreFilters++; // Allowed, but not too often
				}
				else
				{
					goodFilters++;
				}
			}

			Log.Debug($"Filters: {goodFilters} good, {mediocreFilters} mediocre\n");
			Assert.LessOrEqual(mediocreFilters, goodFilters);
		}

		[Test]
		public void TestBits()
		{
			int h = 31;
			int bits = 64;
			int bitpos1 = (int) (((long) h) % bits);
			int bitpos2 = (int) (((long) h)) % bits;
			int bitpos3 = h % bits;

			int bitpos4 = (int) ((ToLong(h)) % bits);

			Assert.AreEqual(bitpos1, 31);
			Assert.AreEqual(bitpos1, bitpos2);
			Assert.AreEqual(bitpos1, bitpos3);
			Assert.AreEqual(bitpos1, bitpos4);
		}

		private long ToLong(int h)
		{
			return h & 0xffffffffL;
		}

		private double FalsePositiveRate()
		{
			int result = 0;
			for (uint i = 0; i < 10000; i++)
			{
				if (Matches(BitConverter.GetBytes(i + 1000000000)))
				{
					result++;
				}
			}
			return result / 10000.0;
		}

		private static int NextLength(int length)
		{
			if (length < 10)
			{
				length += 1;
			}
			else if (length < 100)
			{
				length += 10;
			}
			else if (length < 1000)
			{
				length += 100;
			}
			else
			{
				length += 1000;
			}
			return length;
		}

		private void Add(string value)
		{
			_keys.Add(GetBytes(value));
		}

		private byte[] GetBytes(string s)
		{
			return Encoding.UTF32.GetBytes(s);
		}

		private bool Matches(string s)
		{
			return Matches(GetBytes(s));
		}


		private bool Matches(byte[] s)
		{
			if (_keys.Count != 0)
			{
				Build();
			}
			return _policy.KeyMayMatch(s, _filter);
		}


		private void Reset()
		{
			_keys.Clear();
			_filter = new byte[0];
		}


		private void Build()
		{
			_filter = _policy.CreateFilter(_keys);
			_keys.Clear();
		}
	}
}