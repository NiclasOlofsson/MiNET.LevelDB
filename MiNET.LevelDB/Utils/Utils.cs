using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace MiNET.LevelDB.Utils
{
	public class LevelDbUtils
	{
		public static string HexDump(byte[] bytes, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			StringBuilder sb = new StringBuilder();
			for (int line = 0; line < bytes.Length; line += bytesPerLine)
			{
				if (cutAfterFive && line >= bytesPerLine*5)
				{
					sb.AppendLine(".. output cut after 5 lines");
					break;
				}

				byte[] lineBytes = bytes.Skip(line).Take(bytesPerLine).ToArray();
				if (printLineCount) sb.AppendFormat("{0:x8} ", line);
				sb.Append(string.Join(" ", lineBytes.Select(b => b.ToString("x2"))
						.ToArray())
					.PadRight(bytesPerLine*3));
				if (printText)
				{
					sb.Append(" ");
					sb.Append(new string(lineBytes.Select(b => b < 32 ? '.' : (char) b)
						.ToArray()));
				}
				if (bytesPerLine < bytes.Length)
					sb.AppendLine();
			}

			return sb.ToString();
		}
	}

	public static class LevelDbHelpers
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static Span<byte> UserKey(this Span<byte> fullKey)
		{
			if(fullKey == null || fullKey.IsEmpty) return Span<byte>.Empty;
			return fullKey.Slice(0, fullKey.Length - 8);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static ulong SequenceNumber(this Span<byte> fullKey)
		{
			var number = BitConverter.ToUInt64(fullKey.Slice(fullKey.Length - 8, 8));
			var sequence = number >> 8;
			return sequence;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static byte OperationType(this Span<byte> fullKey)
		{
			return fullKey.Slice(fullKey.Length - 8, 1)[0];
		}

		public static string ToHexString(this Span<byte> bytes)
		{
			return bytes.ToArray().HexDump(bytes.Length, cutAfterFive: true, printText: false);
		}

		public static string ToHexString(this ReadOnlySpan<byte> bytes)
		{
			return bytes.HexDump(bytes.Length, cutAfterFive: true, printText: false);
		}

		public static string HexDump(this byte[] value, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			return LevelDbUtils.HexDump(value, bytesPerLine, printLineCount, printText, cutAfterFive);
		}

		public static string HexDump(this ReadOnlySpan<byte> value, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			return LevelDbUtils.HexDump(value.ToArray(), bytesPerLine, printLineCount, printText, cutAfterFive);
		}
	}

	internal class Crc32C
	{
		private static readonly uint[] Table = new uint[4096];
		private const uint Poly = 2197175160;
		private const uint MaskDelta = 0xa282ead8;

		static Crc32C()
		{
			for (uint index1 = 0; index1 < 256U; ++index1)
			{
				uint num = index1;
				for (int index2 = 0; index2 < 16; ++index2)
				{
					for (int index3 = 0; index3 < 8; ++index3)
						num = ((int) num & 1) == 1 ? Poly ^ num >> 1 : num >> 1;
					Table[index2*256 + index1] = num;
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static uint Compute(ReadOnlySpan<byte> input)
		{
			return Append(0U, input);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static uint Compute(byte b)
		{
			return Append(0U, b);
		}


		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static uint Append(uint crc, ReadOnlySpan<byte> input)
		{
			if (input.Length <= 0) return crc;

			int offset = 0;
			int length = input.Length;
			uint num1 = uint.MaxValue ^ crc;
			uint[] table = Table;
			for (; length >= 16; length -= 16)
			{
				uint num2 = table[768 + input[offset + 12]] ^ table[512 + input[offset + 13]] ^ table[256 + input[offset + 14]] ^ table[input[offset + 15]];
				uint num3 = table[1792 + input[offset + 8]] ^ table[1536 + input[offset + 9]] ^ table[1280 + input[offset + 10]] ^ table[1024 + input[offset + 11]];
				uint num4 = table[2816 + input[offset + 4]] ^ table[2560 + input[offset + 5]] ^ table[2304 + input[offset + 6]] ^ table[2048 + input[offset + 7]];
				num1 = table[3840 + (((int) num1 ^ input[offset]) & byte.MaxValue)] ^ table[3584 + (((int) (num1 >> 8) ^ input[offset + 1]) & byte.MaxValue)] ^ table[3328 + (((int) (num1 >> 16) ^ input[offset + 2]) & byte.MaxValue)] ^ table[3072 + (((int) (num1 >> 24) ^ input[offset + 3]) & byte.MaxValue)] ^ num4 ^ num3 ^ num2;
				offset += 16;
			}
			while (--length >= 0)
				num1 = table[((int) num1 ^ input[offset++]) & byte.MaxValue] ^ num1 >> 8;
			return num1 ^ uint.MaxValue;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static uint Append(uint crc, byte b)
		{
			uint num1 = uint.MaxValue ^ crc;
			num1 = Table[((int) num1 ^ b) & byte.MaxValue] ^ num1 >> 8;
			return num1 ^ uint.MaxValue;
		}

		/// <summary>
		///     Return a masked representation of crc.
		///     <p />
		///     Motivation: it is problematic to compute the CRC of a string that
		///     contains embedded CRCs.Therefore we recommend that CRCs stored
		///     somewhere (e.g., in files) should be masked before being stored.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static uint Mask(uint crc)
		{
			// Rotate right by 15 bits and add a constant.
			return ((crc >> 15) | (crc << 17)) + MaskDelta;
		}


		/// <summary>
		///     Return the crc whose masked representation is masked_crc.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static uint Unmask(uint maskedCrc)
		{
			uint rot = maskedCrc - MaskDelta;
			return ((rot >> 17) | (rot << 15));
		}
	}
}