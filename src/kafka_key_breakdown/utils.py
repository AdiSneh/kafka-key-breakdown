import asyncio
from datetime import datetime, timedelta
from typing import Any, Coroutine, TypeVar

from more_itertools import chunked

T = TypeVar("T")


def range_datetime(start_time: datetime, end_time: datetime, step: timedelta) -> list[datetime]:
    return [
        datetime.fromtimestamp(t)
        for t in range(
            int(start_time.timestamp()), int(end_time.timestamp()), int(step.total_seconds())
        )
    ]


def consecutive_differences(numbers: list[int]) -> list[int]:
    return [b - a for a, b in zip(numbers, numbers[1:])]


async def gather_with_limit(*coroutines: Coroutine[Any, Any, T], limit: int) -> list[T]:
    return sum(
        [await asyncio.gather(*chunk) for chunk in chunked(coroutines, limit)],
        start=[],
    )
