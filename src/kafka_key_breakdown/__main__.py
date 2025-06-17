import logging
import sys
from collections import Counter
from datetime import datetime, timedelta

from confluent_kafka import Consumer, OFFSET_END, TopicPartition

from .graph import graph
from .utils import consecutive_differences, range_datetime

log = logging.Logger("Kafka Key Breakdown")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
)
log.addHandler(handler)


def get_key_distribution_for_timespan(
    topic: str,
    partition: int,
    bootstrap_servers: str,
    consumer_group: str,
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    max_num_messages: int,
) -> dict[datetime, Counter[str]]:
    consumer = Consumer(
        **{
            "bootstrap.servers": bootstrap_servers,
            "group.id": consumer_group,
        }
    )
    intervals = range_datetime(start_time, end_time, interval)
    offsets_for_intervals = dict()
    for t in intervals:
        offset = consumer.offsets_for_times(
            [TopicPartition(topic, partition, offset=int(t.timestamp() * 1000))],
            timeout=1,
        )[0].offset
        # TODO: Also slice out the intervals that are earlier than the earliest record time
        if offset == OFFSET_END:
            log.warning(
                f"Timestamp {t} is later than the latest record time "
                f"for the topic {topic}, partition {partition}"
            )
        else:
            offsets_for_intervals[t] = offset
    if not offsets_for_intervals:
        return dict()

    earliest_offset, latest_offset = consumer.get_watermark_offsets(TopicPartition(topic, partition))
    offsets = list(offsets_for_intervals.values())
    num_messages_for_intervals = [
        min(d, max_num_messages)
        for d in consecutive_differences(offsets)
    ] + [min(latest_offset - offsets[-1], max_num_messages)]
    return {
        t: get_key_distribution_by_offset(
            topic=topic,
            partition=partition,
            consumer=consumer,
            offset=max(offset - num_messages_for_intervals[i], earliest_offset),
            num_messages=num_messages_for_intervals[i],
        )
        for i, (t, offset) in enumerate(offsets_for_intervals.items())
    }


def get_key_distribution_by_offset(
    topic: str,
    partition: int,
    consumer: Consumer,
    offset: int,
    num_messages: int,
) -> Counter[str] | None:
    consumer.assign([TopicPartition(topic, partition, offset=offset)])
    messages = consumer.consume(num_messages=num_messages, timeout=1)
    return Counter(m.key().decode() for m in messages)


if __name__ == '__main__':
    graph(
        get_key_distribution_for_timespan(
            topic="adi",
            partition=0,
            bootstrap_servers="localhost:9092",
            consumer_group="adi",
            start_time=datetime(2025, 6, 17, 15, 0, 34),
            end_time=datetime(2025, 6, 17, 15, 17, 34),
            interval=timedelta(minutes=1),
            max_num_messages=50,
        )
    )
