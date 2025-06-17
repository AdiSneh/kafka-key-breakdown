from collections import Counter
from datetime import datetime

import pandas as pd
from matplotlib import pyplot as plt


def graph(interval_to_key_distribution: dict[datetime, Counter[str]]) -> None:
    df = pd.DataFrame({
        "Time": interval_to_key_distribution.keys(),
        "Distribution": interval_to_key_distribution.values(),
    })
    df["Time"] = pd.to_datetime(df["Time"])

    all_keys = set()
    for distribution in df["Distribution"]:
        all_keys.update(distribution.keys())

    for key in all_keys:
        df[key] = df["Distribution"].apply(lambda c: c.get(key, 0))

    plt.figure(figsize=(12, 6))
    for key in all_keys:
        plt.plot(df["Time"], df[key], label=key)

    plt.xlabel("Time")
    plt.ylabel("Count")
    plt.title("Kafka Key Distribution Over Time")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
