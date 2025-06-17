import asyncio
from datetime import datetime, timedelta

from .graph import graph
from .kafka import get_key_distribution_for_timespan

if __name__ == "__main__":
    graph(
        asyncio.run(
            get_key_distribution_for_timespan(
                topic="adi",
                partition=0,
                bootstrap_servers="localhost:9092",
                consumer_group="adi",
                start_time=datetime(2025, 6, 17, 22, 45, 0),
                end_time=datetime(2025, 6, 17, 23, 40, 0),
                interval=timedelta(seconds=30),
                max_num_messages=10000,
                concurrency_limit=10,
            )
        )
    )
