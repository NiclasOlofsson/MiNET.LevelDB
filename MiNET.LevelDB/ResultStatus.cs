using System;

namespace MiNET.LevelDB
{
	public enum ResultState
	{
		Undefined,
		Exist,
		Deleted,
		NotFound
	}

	public ref struct ResultStatus
	{
		public ResultState State { get; }
		public ReadOnlySpan<byte> Data { get; }

		public ResultStatus(ResultState state, ReadOnlySpan<byte> data)
		{
			State = state;
			Data = data;
		}

		public static ResultStatus NotFound => new ResultStatus(ResultState.NotFound, Span<byte>.Empty);
		public static ResultStatus Deleted => new ResultStatus(ResultState.Deleted, Span<byte>.Empty);
	}
}