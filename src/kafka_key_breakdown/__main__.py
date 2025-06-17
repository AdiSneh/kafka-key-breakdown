import asyncio
import logging
import sys
from collections import Counter
from datetime import datetime, timedelta

from aiokafka import AIOKafkaConsumer, TopicPartition

from .graph import graph
from .utils import consecutive_differences, range_datetime

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
) -> dict[datetime, Counter[str]]:
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=consumer_group)
    await consumer.start()
    # TODO: Make a context manager or some other nicer way to close the consumer
    try:
        intervals = range_datetime(start_time, end_time, interval)
        offsets_for_intervals = dict()
        topic_partition = TopicPartition(topic, partition)
        beginning_offset = (await consumer.beginning_offsets([topic_partition]))[topic_partition]
        end_offset = (await consumer.end_offsets([topic_partition]))[topic_partition]
        for t in intervals:
            offset_and_timestamp = (await consumer.offsets_for_times(
                {topic_partition: int(t.timestamp() * 1000)}
            ))[topic_partition]
            if offset_and_timestamp is None:
                log.warning(
                    f"Timestamp {t} is later than the latest record time "
                    f"for the topic {topic}, partition {partition}. Skipping."
                )
            elif offset_and_timestamp.offset == beginning_offset:
                log.warning(
                    f"Timestamp {t} yielded the same offset as the first record in the "
                    f"topic {topic}, partition {partition}, meaning that the timestamp is probable earlier "
                    f"than the earliest record time. Skipping."
                )
            else:
                offsets_for_intervals[t] = offset_and_timestamp.offset
        if not offsets_for_intervals:
            return dict()

        offsets = list(offsets_for_intervals.values())
        num_messages_for_intervals = [
            min(d, max_num_messages)
            for d in consecutive_differences(offsets)
        ] + [min(end_offset - offsets[-1], max_num_messages)]
        key_distribution = {
            t: (await get_key_distribution_by_offset(
                topic_partition=topic_partition,
                bootstrap_servers=bootstrap_servers,
                consumer_group=consumer_group,
                offset=max(offset - num_messages_for_intervals[i], beginning_offset),
                num_messages=num_messages_for_intervals[i],
            )) if num_messages_for_intervals[i] > 0 else Counter()
            for i, (t, offset) in enumerate(offsets_for_intervals.items())
        }
    finally:
        await consumer.stop()
    return key_distribution


async def get_key_distribution_by_offset(
    topic_partition: TopicPartition,
    bootstrap_servers: str,
    consumer_group: str,
    offset: int,
    num_messages: int,
) -> Counter[str] | None:
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=consumer_group)
    await consumer.start()
    try:
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, offset)
        messages = (
            await consumer.getmany(
                topic_partition,  # type: ignore
                timeout_ms=1000,
                max_records=num_messages,
            )
        )[topic_partition]
    finally:
        await consumer.stop()
    return Counter(m.key.decode() for m in messages)


if __name__ == '__main__':
    graph(
        asyncio.run(get_key_distribution_for_timespan(
            topic="adi",
            partition=0,
            bootstrap_servers="localhost:9092",
            consumer_group="adi",
            start_time=datetime(2025, 6, 17, 19, 39, 0),
            end_time=datetime(2025, 6, 17, 19, 49, 40),
            interval=timedelta(minutes=1),
            max_num_messages=50,
        ))
    )
