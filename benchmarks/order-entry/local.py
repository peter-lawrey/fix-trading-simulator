"""Run four trading processes and audit their two FIX sessions and SQL projection on one host."""

import argparse
import csv
import hashlib
import json
import os
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from contextlib import ExitStack, contextmanager
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
JVM = [
    "java",
    "@" + str(ROOT / "queue-jvm.args"),
    "-Xms512m",
    "-Xmx512m",
    "-XX:+UseG1GC",
    "-XX:ActiveProcessorCount=4",
]
ROLES = ("broker", "exchange")


def ports(count):
    with ExitStack() as pool:
        sockets = [pool.enter_context(socket.socket()) for _ in range(count)]
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise RuntimeError("Trading process did not stop cleanly")


def start(ctx, command, env, log):
    output = ctx.enter_context(log.open("w"))
    proc = subprocess.Popen(command, env=env, stdout=output, stderr=subprocess.STDOUT)
    ctx.callback(stop, proc)
    return proc


@contextmanager
def database(bin_dir, disk_root, port, out):
    with tempfile.TemporaryDirectory(dir=disk_root) as tmp:
        tmp = Path(tmp)
        pgdata = tmp / "pgdata"
        password = secrets.token_urlsafe(24)
        pw = tmp / "pw"
        pw.write_text(password)
        pw.chmod(0o600)
        with (out / "initdb.log").open("w") as log:
            subprocess.run(
                [
                    bin_dir / "initdb",
                    "-D",
                    pgdata,
                    "-U",
                    "postgres",
                    "--auth-local=trust",
                    "--auth-host=scram-sha-256",
                    "--pwfile",
                    pw,
                    "--no-locale",
                    "--encoding=UTF8",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
        pw.unlink()
        subprocess.run(
            [
                bin_dir / "pg_ctl",
                "-D",
                pgdata,
                "-l",
                out / "postgres.log",
                "-o",
                f"-h 127.0.0.1 -p {port} -k '{tmp}' -c fsync=on -c synchronous_commit=on",
                "start",
            ],
            check=True,
        )
        try:
            env = dict(
                os.environ,
                SIM_DB_URL=f"jdbc:postgresql://127.0.0.1:{port}/postgres?reWriteBatchedInserts=true",
                SIM_DB_USER="postgres",
                SIM_DB_PASSWORD=password,
            )
            dependencies = ROOT / "reporting-gateway/target/dependency/*"
            with (out / "database.txt").open("w") as log:
                subprocess.run(
                    ["java", "-cp", str(dependencies), str(HERE / "InitSchemas.java"), str(ROOT)],
                    env=env,
                    stdout=log,
                    check=True,
                )
            yield env
        finally:
            subprocess.run([bin_dir / "pg_ctl", "-D", pgdata, "stop", "-m", "fast"], check=True)


def ready(roots, processes):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if any(p.poll() is not None for p in processes):
            raise RuntimeError("Process exited during logon")
        try:
            for root in roots:
                with urllib.request.urlopen(root + "/session", timeout=1) as r:
                    if not json.load(r).get("loggedOn"):
                        break
            else:
                return
        except OSError:
            pass
        time.sleep(0.2)
    raise RuntimeError("Timed out waiting for the two FIX sessions")


def frames(path):
    with path.open("rb") as file:
        while True:
            header = file.read(12)
            if not header:
                return
            if header != b"8=FIX.4.4\x019=":
                raise ValueError(f"Invalid FIX header in {path}")
            digits = bytearray()
            while (byte := file.read(1)) != b"\x01":
                if not byte.isdigit() or len(digits) > 7:
                    raise ValueError("Invalid BodyLength")
                digits += byte
            body = file.read(int(digits))
            trailer = file.read(7)
            if not body.endswith(b"\x01") or not re.fullmatch(rb"10=\d{3}\x01", trailer):
                raise ValueError("Incomplete FIX frame")
            if sum(header + digits + b"\x01" + body) % 256 != int(trailer[3:6]):
                raise ValueError("Bad FIX checksum")
            pairs = [field.split(b"=", 1) for field in body[:-1].split(b"\x01")]
            tags = dict(pairs)
            if len(tags) != len(pairs):
                raise ValueError("Duplicate field in flat benchmark message")
            yield tags


def hash_file(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def verify_stores(out):
    expected = [{}, {}]
    with (out / "timings/orders.tsv").open() as f:
        for r in csv.DictReader(f, delimiter="\t"):
            lane = int(r["lane"])
            symbol = r["symbol"].encode()
            if r["outcome"] != "success" or symbol in expected[lane]:
                raise ValueError("Invalid driver ledger")
            expected[lane][symbol] = r["clOrdID"].encode()
    receipts = {}
    for lane in range(2):
        for sender, target, kind in [("broker", "exchange", b"D"), ("exchange", "broker", b"8")]:
            tx = out / f"{lane}-{sender}/outgoing.fix"
            rx = out / f"{lane}-{target}/incoming.fix"
            digest = hash_file(tx)
            if digest != hash_file(rx):
                raise ValueError(f"FIX capture mismatch {tx} / {rx}")
            pending = expected[lane].copy()
            compids = {"broker": f"BROKER{lane}".encode(), "exchange": f"EXCH{lane}".encode()}
            for t in frames(tx):
                if t[b"49"] != compids[sender] or t[b"56"] != compids[target]:
                    raise ValueError("Wrong session")
                if t[b"35"] not in (b"D", b"8"):
                    continue
                if t[b"35"] != kind or pending.pop(t[b"55"], None) != t[b"11"] or t[b"54"] != b"1":
                    raise ValueError("Unmatched order, reply or lane")
                if kind == b"D" and (float(t[b"38"]) != 10 or float(t[b"44"]) != 23.5):
                    raise ValueError("Wrong order")
                if kind == b"8" and (
                    t[b"39"] != b"0"
                    or t[b"150"] != b"0"
                    or float(t[b"14"]) != 0
                    or float(t[b"151"]) != 10
                    or float(t[b"6"]) != 0
                    or not t[b"17"]
                    or not t[b"37"]
                ):
                    raise ValueError("Wrong execution report")
            if pending:
                raise ValueError(f"{len(pending)} missing {kind!r} messages")
            receipts[f"{lane}-{sender}"] = {"sha256": digest, "business_messages": len(expected[lane])}
    (out / "wire-receipts.json").write_text(json.dumps(receipts, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pg-bin",
        required=True,
        type=Path,
        help="PostgreSQL directory containing initdb, pg_ctl and postgres",
    )
    parser.add_argument(
        "--pg-root",
        type=Path,
        help="Parent filesystem for temporary PostgreSQL data and WAL",
        required=True,
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--journal", choices=("file", "queue"), required=True)
    parser.add_argument("--transport", choices=("ws", "http"), default="ws")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=12000)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    dbport, *available = ports(7)
    gateway = ROOT / "reporting-gateway/target"
    cp = os.pathsep.join(
        [str(gateway / "reporting-gateway-1.0-SNAPSHOT.jar"), str(gateway / "dependency/*")]
    )
    streams = [f"b{i}{r}={out}/{i}-{r}/business-queue" for i in range(2) for r in ROLES]
    roots = [f"http://127.0.0.1:{available[i * 3]}" for i in range(2)]
    with (
        database(args.pg_bin.resolve(), args.pg_root.resolve(), dbport, out) as env,
        ExitStack() as services,
    ):
        projector = start(
            services,
            [
                *JVM,
                "-Dgateway.lingerMs=1",
                "-cp",
                cp,
                "com.helesto.journal.ProjectionGateway",
                *streams,
            ],
            env,
            out / "projector.log",
        )
        with ExitStack() as traders:
            procs = []
            for role in ("exchange", "broker"):
                for i in range(2):
                    broker = role == "broker"
                    role_env = dict(
                        env,
                        SIM_AUTO_TRADE="false",
                        SIM_BUSINESS_JOURNAL=args.journal,
                        SIM_STORE=str(out / f"{i}-{role}"),
                        SIM_SENDER=("BROKER" if broker else "EXCH") + str(i),
                        SIM_TARGET=("EXCH" if broker else "BROKER") + str(i),
                        SIM_FIX_HOST="127.0.0.1",
                        SIM_FIX_PORT=str(available[3 * i + 2]),
                    )
                    cmd = [
                        *JVM,
                        "-Dsim.maxOrders=320000",
                        "-Dquarkus.http.host=127.0.0.1",
                        f"-Dquarkus.http.port={available[3 * i + int(not broker)]}",
                        "-jar",
                        str(ROOT / f"{role}-back-end/target/quarkus-app/quarkus-run.jar"),
                    ]
                    procs.append(start(traders, cmd, role_env, out / f"{i}-{role}.log"))
            ready(roots, procs + [projector])
            subprocess.run(
                [
                    sys.executable,
                    HERE / "run.py",
                    "--brokers",
                    *roots,
                    "--output",
                    out / "timings",
                    "--transport",
                    args.transport,
                    "--rate",
                    str(args.rate),
                    "--seconds",
                    str(args.seconds),
                    "--runs",
                    str(args.runs),
                    "--warmup",
                    str(args.warmup),
                ],
                check=True,
            )
        if projector.poll() is not None:
            raise RuntimeError("Projector exited during measurement")
        with (out / "projection-audit.txt").open("w") as log:
            subprocess.run(
                [*JVM, "-cp", cp, "com.helesto.journal.ProjectionAudit", *streams],
                env=env,
                stdout=log,
                check=True,
                timeout=300,
            )
        verify_stores(out)
        print(
            "Both sessions, all stopped FIX logs, four event streams and projected SQL books verified."
        )


if __name__ == "__main__":
    main()
