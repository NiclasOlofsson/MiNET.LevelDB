using System;

namespace MiNET.LevelDB
{
	public class Hashing
	{
		const uint M = 0xc6a4a793;
		const int R = 24;

		public static uint Hash(Span<byte> data, uint seed)
		{
			uint h = seed ^ ((uint) data.Length * M);
			int idx = 0;

			// Do four bytes at a time
			for (; idx + 4 <= data.Length; idx += 4)
			{
				var w = BitConverter.ToUInt32(data.Slice(idx, 4));
				h += w;
				h *= M;
				h ^= h >> 16;
			}

			// Pick up remaining bytes
			switch (data.Length - idx)
			{
				case 3:
					h += (uint) (data[idx + 2] & 0xff) << 16;
					goto case 2;
				case 2:
					h += (uint) (data[idx + 1] & 0xff) << 8;
					goto case 1;
				case 1:
					h += (uint) data[idx] & 0xff;
					h *= M;
					h ^= h >> R;
					break;
			}

			return h;
		}

		private static int ByteToInt(Span<byte> data, int index)
		{
			return (data[index] & 0xff) |
					(data[index + 1] & 0xff) << 8 |
					(data[index + 2] & 0xff) << 16 |
					(data[index + 3] & 0xff) << 24;
		}
	}
}