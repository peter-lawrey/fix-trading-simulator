import tempfile
import unittest
from pathlib import Path
from local import verify_stores
from percentiles import summarize


def frame(body):
    body = body.replace("|", "\x01").encode()
    raw = b"8=FIX.4.4\x019=" + str(len(body)).encode() + b"\x01" + body
    return raw + f"10={sum(raw) % 256:03}\x01".encode()


class ReceiptTest(unittest.TestCase):
    def test_matching_files_must_still_cover_all_orders(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "timings").mkdir()
            (out / "timings/orders.tsv").write_text(
                "lane\tsymbol\toutcome\tclOrdID\n0\tA\tsuccess\t1\n1\tB\tsuccess\t2\n"
            )
            for i, symbol in enumerate("AB"):
                broker, exchange = out / f"{i}-broker", out / f"{i}-exchange"
                broker.mkdir()
                exchange.mkdir()
                order = frame(
                    f"35=D|49=BROKER{i}|56=EXCH{i}|55={symbol}|11={i + 1}|54=1|38=10|44=23.5|"
                )
                report = frame(
                    f"35=8|49=EXCH{i}|56=BROKER{i}|55={symbol}|11={i + 1}|54=1|39=0|150=0|14=0|151=10|6=0|17=E|37=O|"
                )
                for path in (broker / "outgoing.fix", exchange / "incoming.fix"):
                    path.write_bytes(order)
                for path in (exchange / "outgoing.fix", broker / "incoming.fix"):
                    path.write_bytes(report)
            verify_stores(out)
            for path in (exchange / "outgoing.fix", broker / "incoming.fix"):
                path.write_bytes(b"")
            with self.assertRaisesRegex(ValueError, "missing"):
                verify_stores(out)

    def test_partial_or_shifted_populations_do_not_get_percentiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            good = [(i, i % 2, 100 + i * 500_000, 200 + i * 500_000) for i in range(6)]
            for invalid in [good[:-1], good[:3] + [(3, 1, good[3][2] + 1, good[3][3])] + good[4:]]:
                with self.assertRaises(ValueError):
                    summarize(invalid, Path(tmp), 2, 2000, 2, 2)
