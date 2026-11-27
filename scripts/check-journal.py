"""Exercise both admission paths and restart the packaged QuickFIX/J simulator."""
import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=3) as response:
        return json.load(response)


def stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise RuntimeError("Simulator did not stop cleanly")


def run_pair(tmp, ports, phase):
    fix, broker, exchange = ports
    with ExitStack() as resources:
        processes = []
        for role, port in (("exchange", exchange), ("broker", broker)):
            jar = ROOT / f"{role}-back-end/target/quarkus-app/quarkus-run.jar"
            log = resources.enter_context((tmp / f"{role}-{phase}.log").open("w"))
            proc = subprocess.Popen(
                ["java", "-Xmx512m", "-Dquarkus.http.host=127.0.0.1", f"-Dquarkus.http.port={port}", "-jar", str(jar)],
                cwd=tmp, stdout=log, stderr=subprocess.STDOUT,
                env=dict(os.environ, SIM_ROLE=role, SIM_FIX_PORT=str(fix), SIM_FIX_HOST="127.0.0.1",
                         SIM_STORE=str(tmp/role), SIM_AUTO_TRADE="false"))
            processes.append(proc)
            resources.callback(stop, proc)
        deadline = time.monotonic()+50
        while time.monotonic()<deadline and all(p.poll() is None for p in processes):
            try:
                if all(get(p, "/session")["loggedOn"] for p in (exchange, broker)):
                    break
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(.1)
        else:
            raise RuntimeError("The two FIX sessions failed to log on")

        receipt = tmp/"orders.json"
        if phase == "trade":
            libs = ROOT/"broker-back-end/target/quarkus-app/lib/main/*"
            subprocess.run(["java", "-cp", str(libs), str(ROOT/"scripts/OrderEntryContract.java"),
                            f"http://127.0.0.1:{broker}", str(receipt)], check=True, timeout=60)
        expected = {row["clOrdID"]:row["symbol"] for row in json.loads(receipt.read_text())}
        for port in (broker, exchange):
            rows=get(port,"/orders?order=desc&limit=100")
            assert {row["clOrdID"]:row["symbol"] for row in rows}==expected
            assert all(row["ordStatus"]=="0" for row in rows)
            assert len(get(port,"/orders?order=desc&limit=2"))==2


def wire(path, kind):
    import re
    result=Counter()
    for frame in re.findall(rb'8=FIX\.[^\x01]+\x01.*?\x0110=\d{3}\x01',path.read_bytes(),re.S):
        fields=dict(field.split(b'=',1) for field in frame.rstrip(b'\x01').split(b'\x01'))
        if fields[b'35']==kind:
            result[(int(fields[b'11']),fields[b'55'].decode())]+=1
    return result


if __name__=="__main__":
    with tempfile.TemporaryDirectory(prefix="fix-journal-") as directory:
        tmp=Path(directory)
        with ExitStack() as resources:
            sockets=[resources.enter_context(socket.socket()) for _ in range(3)]
            for sock in sockets:sock.bind(("127.0.0.1",0))
            ports=[sock.getsockname()[1] for sock in sockets]
        try:
            run_pair(tmp,ports,"trade")
            run_pair(tmp,ports,"reopen")
            expected=Counter((r["clOrdID"],r["symbol"]) for r in json.loads((tmp/"orders.json").read_text()))
            for role,direction,kind in (("broker","outgoing",b"D"),("exchange","incoming",b"D"),
                                        ("exchange","outgoing",b"8"),("broker","incoming",b"8")):
                assert wire(tmp/role/f"{direction}.fix",kind)==expected,(role,direction)
            print("HTTP and WebSocket orders, fresh-JVM restart, both books and four stopped FIX captures passed")
        except Exception:
            for log in tmp.glob("*.log"):
                print(log.name,log.read_text()[-1800:])
            raise
