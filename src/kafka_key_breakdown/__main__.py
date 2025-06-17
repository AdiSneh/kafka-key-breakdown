from collections import Counter
from datetime import datetime

from confluent_kafka import Consumer, TopicPartition, OFFSET_END


def get_key_distribution(
    topic: str,
    partition: int,
    bootstrap_servers: str,
    consumer_group: str,
    end_time: datetime,
    num_messages: int,
) -> Counter[str] | None:
    consumer = Consumer(
        **{
            "bootstrap.servers": bootstrap_servers,
            "group.id": consumer_group,
        }
    )
    offset_for_end_time = consumer.offsets_for_times(
        [
            TopicPartition(topic, partition, offset=int(end_time.timestamp() * 1000))
        ],
        timeout=1,
    )[0].offset
    if offset_for_end_time == OFFSET_END:
        return None
    consumer.assign([TopicPartition(topic, partition, offset=offset_for_end_time - num_messages)])
    messages = consumer.consume(num_messages=num_messages, timeout=1)
    return Counter(m.key() for m in messages)


if __name__ == '__main__':
    print(
        get_key_distribution(
            topic="adi",
            partition=0,
            bootstrap_servers="localhost:9092",
            consumer_group="adi",
            end_time=datetime(2025, 6, 17, 15, 17, 34),
            num_messages=100,
        )
    )
