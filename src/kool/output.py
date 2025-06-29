from collections import Counter
from datetime import datetime

import pandas as pd
from matplotlib import pyplot as plt
from tabulate import tabulate


def display_all(interval_to_key_distribution: dict[datetime, Counter[str]]) -> None:
    display_table(interval_to_key_distribution)
    display_graph(interval_to_key_distribution)


def display_table(interval_to_key_distribution: dict[datetime, Counter[str]]) -> None:
    print(tabulate(interval_to_key_distribution.items(), headers=["Time", "Keys"]))


def display_graph(interval_to_key_distribution: dict[datetime, Counter[str]]) -> None:
    df = pd.DataFrame(
        {
            "Time": interval_to_key_distribution.keys(),
            "Distribution": interval_to_key_distribution.values(),
        }
    )
    df["Time"] = pd.to_datetime(df["Time"])

    all_keys = set()
    for distribution in df["Distribution"]:
        all_keys.update(distribution.keys())

    for key in all_keys:
        df[key] = df["Distribution"].apply(lambda c: c.get(key, 0))

    plt.style.use("dark_background")
    plt.figure(figsize=(12, 6))
    for key in all_keys:
        plt.plot(df["Time"], df[key], label=key, linewidth=3)

    ax = plt.gca()
    ax.set_facecolor("#0a141a")
    plt.xlabel("Time", fontweight="bold")
    plt.ylabel("Count", fontweight="bold")
    plt.title("Kafka Key Distribution Over Time", fontweight="bold")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
