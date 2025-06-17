import asyncio
from datetime import datetime, timedelta
from enum import Enum

from aiokafka import TopicPartition
from typer import Typer

from .output import display_graph, display_table, display_all
from .kafka import get_key_distribution_for_timespan

app = Typer()


class Output(Enum):
    GRAPH = "graph"
    TABLE = "table"
    ALL = "all"


OUTPUT_TO_DISPLAY_FUNCTION = {
    Output.GRAPH: display_graph,
    Output.TABLE: display_table,
    Output.ALL: display_all,
}


@app.command()
def count_keys(
    topic: str,
    partition: int,
    start_time: datetime,
    end_time: datetime,
    # TODO: Auto-generate the consumer group with a unique hash suffix or something
    consumer_group: str = "adi",
    bootstrap_servers: str = "localhost:9092",
    interval_seconds: int = 30,
    max_num_messages: int = 1000,
    concurrency_limit: int = 10,
    output: Output = Output.ALL,
):
    key_distribution = asyncio.run(
        get_key_distribution_for_timespan(
            topic_partition=TopicPartition(topic, partition),
            bootstrap_servers=bootstrap_servers,
            consumer_group=consumer_group,
            start_time=start_time,
            end_time=end_time,
            interval=timedelta(seconds=interval_seconds),
            max_num_messages=max_num_messages,
            concurrency_limit=concurrency_limit,
        )
    )
    OUTPUT_TO_DISPLAY_FUNCTION[output](key_distribution)


if __name__ == "__main__":
    app()
