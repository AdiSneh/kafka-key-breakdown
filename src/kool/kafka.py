from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Coroutine

from aiokafka import AIOKafkaConsumer, TopicPartition

from .logger import log
from .utils import gather_with_limit, range_datetime


async def get_topic_key_distribution_for_timespan(
    topic: str,
    bootstrap_servers: str,
    consumer_group: str,
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    max_num_messages: int,
    concurrency_limit: int,
) -> dict[datetime, Counter[str]]:
    async with AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers, group_id=consumer_group
    ) as consumer:
        partitions = consumer.partitions_for_topic(topic)

    partition_to_time_to_consume_coroutine = dict()
    for partition in partitions:
        partition_to_time_to_consume_coroutine[
            partition
        ] = await get_key_distribution_for_timespan_coroutines(
            topic_partition=TopicPartition(topic, partition),
            bootstrap_servers=bootstrap_servers,
            consumer_group=consumer_group,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            max_num_messages=max_num_messages,
            concurrency_limit=concurrency_limit,
        )

    all_coroutines = sum(
        [list(d.values()) for d in partition_to_time_to_consume_coroutine.values()], start=[]
    )
    all_respective_times = sum(
        [list(d.keys()) for d in partition_to_time_to_consume_coroutine.values()], start=[]
    )
    results = await gather_with_limit(*all_coroutines, limit=concurrency_limit)
    time_to_key_counter = defaultdict(Counter)
    for time, result in zip(all_respective_times, results):
        time_to_key_counter[time] += result
    return time_to_key_counter


async def get_partition_key_distribution_for_timespan(
    topic_partition: TopicPartition,
    bootstrap_servers: str,
    consumer_group: str,
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    max_num_messages: int,
    concurrency_limit: int,
) -> dict[datetime, Counter[str]]:
    time_to_consume_coroutine = await get_key_distribution_for_timespan_coroutines(
        topic_partition=topic_partition,
        bootstrap_servers=bootstrap_servers,
        consumer_group=consumer_group,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        max_num_messages=max_num_messages,
        concurrency_limit=concurrency_limit,
    )
    results = await gather_with_limit(
        *time_to_consume_coroutine.values(),
        limit=concurrency_limit,
    )
    return dict(zip(time_to_consume_coroutine.keys(), results))


async def get_key_distribution_for_timespan_coroutines(
    topic_partition: TopicPartition,
    bootstrap_servers: str,
    consumer_group: str,
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    max_num_messages: int,
    concurrency_limit: int,
) -> dict[datetime, Coroutine[Any, Any, Counter[str]]]:
    async with AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers, group_id=consumer_group
    ) as consumer:
        first_offset = (await consumer.beginning_offsets([topic_partition]))[topic_partition]
        last_offset = (await consumer.end_offsets([topic_partition]))[topic_partition]
        time_to_offset = await _get_time_to_offset(
            consumer=consumer,
            topic_partition=topic_partition,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            first_offset=first_offset,
        )
    if not time_to_offset:
        return dict()

    time_to_consume_coroutine: dict[datetime, Coroutine[Any, Any, Counter[str]]] = dict()
    offsets = list(time_to_offset.values())
    for i, (time, offset) in enumerate(time_to_offset.items()):
        i: int  # PyCharm doesn't recognize that the index is an int when enumerating on dict items.
        num_messages = min(
            (offsets[i + 1] - offset if i < len(offsets) - 1 else last_offset - offsets[-1]),
            max_num_messages,
        )
        time_to_consume_coroutine[time] = _get_key_distribution_by_offset(
            topic_partition=topic_partition,
            bootstrap_servers=bootstrap_servers,
            consumer_group=f"{consumer_group}-{i % concurrency_limit}",
            offset=max(offset - num_messages, first_offset),
            num_messages=num_messages,
        )
    return time_to_consume_coroutine


async def _get_time_to_offset(
    consumer: AIOKafkaConsumer,
    topic_partition: TopicPartition,
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    first_offset: int,
) -> dict[datetime, int]:
    time_to_offset = dict()
    for t in range_datetime(start_time, end_time, interval):
        offset_and_timestamp = (
            await consumer.offsets_for_times({topic_partition: int(t.timestamp() * 1000)})
        )[topic_partition]
        if offset_and_timestamp is None:
            log.debug(
                f"Timestamp {t} is later than the latest record time in {topic_partition}. "
                f"Skipping."
            )
        elif offset_and_timestamp.offset == first_offset:
            log.debug(
                f"Timestamp {t} yielded the same offset as the first record in topic partition "
                f"{topic_partition}, meaning that the timestamp is probably earlier than the "
                f"earliest record time. Skipping."
            )
        else:
            time_to_offset[t] = offset_and_timestamp.offset
    return time_to_offset


async def _get_key_distribution_by_offset(
    topic_partition: TopicPartition,
    bootstrap_servers: str,
    consumer_group: str,
    offset: int,
    num_messages: int,
) -> Counter[str]:
    if num_messages == 0:
        return Counter()
    async with AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers, group_id=consumer_group
    ) as consumer:
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
