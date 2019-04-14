using System;
using MiNET.LevelDB;
using NUnit.Framework;

namespace MiNET.LevelDBTests
{
	public class ComparatorTests
	{
		const int Less = -1;
		const int Equal = 0;
		const int Greater = 1;

		[Test]
		public void BytewiseComparatorTest()
		{
			var comparator = new BytewiseComparator();

			// Basic
			Assert.AreEqual(Equal, comparator.Compare(new Span<byte>(), new Span<byte>()));
			Assert.AreEqual(Greater, comparator.Compare(new Span<byte>(new byte[] {0}), new Span<byte>()));
			Assert.AreEqual(Less, comparator.Compare(new Span<byte>(), new Span<byte>(new byte[] {0})));

			Assert.AreEqual(Equal, comparator.Compare(new Span<byte>(new byte[] {0}), new Span<byte>(new byte[] {0})));
			Assert.AreEqual(Equal, comparator.Compare(new Span<byte>(new byte[] {0, 1, 2}), new Span<byte>(new byte[] {0, 1, 2})));

			Assert.AreEqual(Greater, comparator.Compare(new Span<byte>(new byte[] {1}), new Span<byte>(new byte[] {0})));
			Assert.AreEqual(Greater, comparator.Compare(new Span<byte>(new byte[] {1, 1}), new Span<byte>(new byte[] {1, 0})));
			Assert.AreEqual(Greater, comparator.Compare(new Span<byte>(new byte[] {1, 1}), new Span<byte>(new byte[] {1, 0, 1})));

			Assert.AreEqual(Less, comparator.Compare(new Span<byte>(new byte[] {0}), new Span<byte>(new byte[] {1})));
			Assert.AreEqual(Less, comparator.Compare(new Span<byte>(new byte[] {1, 0}), new Span<byte>(new byte[] {1, 1})));
			Assert.AreEqual(Less, comparator.Compare(new Span<byte>(new byte[] {1, 0, 1}), new Span<byte>(new byte[] {1, 1})));

			Assert.AreEqual(Greater, comparator.Compare(new Span<byte>(new byte[] {1, 0, 2}), new Span<byte>(new byte[] {1, 0, 1, 2, 3})));

			//"SmallestKey": {
			//	"Key": "00 00 00 00 10 00 00 00 31 01 1f 34 00 00 00 00 00  ........1..4....."
			//},
			//"LargestKey": {
			//	"Key": "ff ff ff ff fc ff ff ff 76 01 a7 42 00 00 00 00 00  ÿÿÿÿüÿÿÿv.§B....."
			//}

			Assert.AreEqual(Less, comparator.Compare(
				new byte[] {0x00, 0x00, 0x00, 0x00, 0x10, 0x00},
				new byte[] {0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x31, 0x01, 0x1f, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00,}));

			Assert.AreEqual(Greater, comparator.Compare(
				new byte[] { 0x00, 0x00, 0x00, 0x00, 0x10, 0x01 },
				new byte[] { 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x31, 0x01, 0x1f, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, }));

		}
	}
}