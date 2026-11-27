"""Measure two distinct live broker sessions from scheduled submission to confirmed order update."""

import argparse
import csv
import json
import os
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlparse
from percentiles import summarize

HERE = Path(__file__).resolve().parent


def replies(directory, prefix):
    status = json.loads((directory / "driver.json").read_text())
    if any(status[k] for k in ("wrong_lane", "duplicates", "foreign_events", "errors")):
        raise ValueError(f"Invalid driver result: {status}")
    rows = directory / "orders.tsv"
    with rows.open() as file:
        for row in csv.DictReader(file, delimiter="\t"):
            i = int(row["i"])
            if (
                row["outcome"] != "success"
                or row["symbol"] != prefix + str(i)
                or not (row["clOrdID"] == row["wsClOrdID"] and int(row["clOrdID"]) > 0)
                or not (
                    int(row["due_ns"])
                    <= int(row["dispatch_ns"])
                    <= int(row["handoff_ns"])
                    <= min(int(row["http_ns"]), int(row["ack_ns"]))
                )
            ):
                raise ValueError(f"Unqualified order at row {i}: {row['outcome']}")
            yield i, int(row["lane"]), int(row["due_ns"]), int(row["ack_ns"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", nargs=2, required=True, metavar=("BROKER_A", "BROKER_B"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--transport", choices=("ws", "http"), default="ws")
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=12000)
    args = parser.parse_args()
    brokers = [url.rstrip("/") for url in args.brokers]
    if (
        len(set(brokers)) != 2
        or any(urlparse(url).scheme not in ("http", "https") for url in brokers)
        or min(args.rate, args.seconds, args.runs, args.warmup) <= 0
    ):
        parser.error("Provide two distinct broker URLs and positive workload parameters")
    count = args.rate * args.seconds
    if count % 2:
        parser.error("Use an even number of orders per window")
    target = HERE / "target"
    cp_file = target / "benchmark.cp"
    classpath = os.pathsep.join([str(target / "classes"), cp_file.read_text().strip()])
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    prefix = "B" + uuid.uuid4().hex[:10]
    flags = [
        "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
    ]
    cmd = [
        "java",
        "-Xms512m",
        "-Xmx512m",
        "-XX:ActiveProcessorCount=4",
        *flags,
        "-cp",
        classpath,
        "SimulatorLatency",
        ",".join(brokers),
        "2",
        prefix,
        str(args.warmup),
        str(count),
        str(args.rate),
        "128",
        str(out),
        "refuse",
        str(args.runs),
    ]
    with (out / "jlbh.log").open("w") as log:
        subprocess.run(
            cmd,
            env=dict(os.environ, ORDER_TRANSPORT=args.transport),
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=150 + args.warmup / args.rate + args.runs * args.seconds,
            check=True,
        )
    summarize(replies(out, prefix), out, args.warmup, args.rate, count, args.runs)


if __name__ == "__main__":
    main()
