import asyncio
import logging
import sys
from collections import Counter
from datetime import datetime, timedelta

from aiokafka import AIOKafkaConsumer, TopicPartition

from .graph import graph
from .utils import gather_with_limit, range_datetime

log = logging.Logger("Kafka Key Breakdown")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
)
log.addHandler(handler)


async def get_key_distribution_for_timespan(
    topic: str,
    partition: int,
    bootstrap_servers: str,
    consumer_group: str,
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    max_num_messages: int,
    concurrency_limit: int,
) -> dict[datetime, Counter[str]]:
    topic_partition = TopicPartition(topic, partition)
    async with AIOKafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=consumer_group) as consumer:
        first_offset = (await consumer.beginning_offsets([topic_partition]))[topic_partition]
        last_offset = (await consumer.end_offsets([topic_partition]))[topic_partition]
        intervals_to_offsets = await _get_intervals_to_offsets(
            consumer=consumer, topic_partition=topic_partition, start_time=start_time, end_time=end_time,
            interval=interval, first_offset=first_offset
            )
    if not intervals_to_offsets:
        return dict()

    consume_coroutines = []
    for i, offset in enumerate(offsets := list(intervals_to_offsets.values())):
        num_messages = min(
            (
                offsets[i + 1] - offset
                if i < len(offsets) - 1
                else last_offset - offsets[-1]
            ),
            max_num_messages,
        )
        consume_coroutines.append(
            _get_key_distribution_by_offset(
                topic_partition=topic_partition, bootstrap_servers=bootstrap_servers,
                consumer_group=f"{consumer_group}-{i % concurrency_limit}",
                offset=max(offset - num_messages, first_offset), num_messages=num_messages
                )
        )
    results = await gather_with_limit(*consume_coroutines, limit=concurrency_limit)
    return dict(zip(intervals_to_offsets.keys(), results))


async def _get_intervals_to_offsets(
    consumer: AIOKafkaConsumer,
    topic_partition: TopicPartition,
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    first_offset: int,
) -> dict[datetime, int]:
    intervals_to_offsets = dict()
    for t in range_datetime(start_time, end_time, interval):
        offset_and_timestamp = (await consumer.offsets_for_times(
            {topic_partition: int(t.timestamp() * 1000)}
        ))[topic_partition]
        if offset_and_timestamp is None:
            log.warning(
                f"Timestamp {t} is later than the latest record time in {topic_partition}. Skipping."
            )
        elif offset_and_timestamp.offset == first_offset:
            log.warning(
                f"Timestamp {t} yielded the same offset as the first record in topic partition {topic_partition}, "
                f"meaning that the timestamp is probably earlier than the earliest record time. Skipping."
            )
        else:
            intervals_to_offsets[t] = offset_and_timestamp.offset
    return intervals_to_offsets


async def _get_key_distribution_by_offset(
    topic_partition: TopicPartition,
    bootstrap_servers: str,
    consumer_group: str,
    offset: int,
    num_messages: int,
) -> Counter[str]:
    if num_messages == 0:
        return Counter()
    async with AIOKafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=consumer_group) as consumer:
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, offset)
        messages = (
            await consumer.getmany(
                topic_partition,  # type: ignore
                timeout_ms=1000,
                max_records=num_messages,
            )
        )[topic_partition]
    return Counter(m.key.decode() for m in messages)


if __name__ == '__main__':
    graph(
        asyncio.run(get_key_distribution_for_timespan(
            topic="adi",
            partition=0,
            bootstrap_servers="localhost:9092",
            consumer_group="adi",
            start_time=datetime(2025, 6, 17, 22, 45, 0),
            end_time=datetime(2025, 6, 17, 23, 40, 0),
            interval=timedelta(seconds=30),
            max_num_messages=10000,
            concurrency_limit=10,
        ))
    )
