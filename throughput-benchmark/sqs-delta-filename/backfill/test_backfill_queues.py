"""Offline tests: no AWS calls. Run: python3 -m pytest -q  (or unittest)."""

import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.parse
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import backfill_queues as bq  # noqa: E402
from fake_aws import FakeS3, FakeSQS  # noqa: E402

KEYS = [
    "cid-landing-jsongz/year=2026/month=10/day=09/hour=06/"
    "customerId=C123/productCode=AmazonEC2/part-0_001_new.json.gz",
    "cid-landing-jsongz/year=2026/customerId=a b+c/x%y_002_new.json.gz",
    "cid-landing-jsongz/unicode/café-数据_003_new.json.gz",
    "cid-landing-jsongz/odd/~!*'()&$,;:@?#[]_004_new.json.gz",
]


def java_urldecode(s):
    """Model of java.net.URLDecoder.decode(s, UTF_8): '+' -> ' ', %XX runs
    decoded as UTF-8 bytes, every other char literal; malformed % raises."""
    out, i, buf = [], 0, bytearray()
    while i < len(s):
        c = s[i]
        if c == "%":
            if i + 2 >= len(s):
                raise ValueError("incomplete escape")
            buf.append(int(s[i + 1:i + 3], 16))
            i += 3
            continue
        if buf:
            out.append(buf.decode("utf-8"))
            buf = bytearray()
        out.append(" " if c == "+" else c)
        i += 1
    if buf:
        out.append(buf.decode("utf-8"))
    return "".join(out)


def obj(key, i=0):
    return {"Key": key, "Size": 100 + i, "ETag": '"abc%d"' % i,
            "LastModified": datetime(2026, 10, 9, 6, 21, 3, 456789,
                                     tzinfo=timezone.utc)}


class KeyEncodingTest(unittest.TestCase):
    def test_equals_becomes_percent_3d(self):
        enc = bq.encode_key(KEYS[0])
        self.assertIn("year%3D2026/month%3D10", enc)
        self.assertNotIn("=", enc)
        self.assertEqual(enc.count("/"), KEYS[0].count("/"))

    def test_space_plus_percent(self):
        enc = bq.encode_key("a b+c%d")
        self.assertEqual(enc, "a+b%2Bc%25d")

    def test_round_trip_java_urldecoder_model(self):
        for k in KEYS:
            enc = bq.encode_key(k)
            self.assertEqual(java_urldecode(enc), k)
            self.assertEqual(urllib.parse.unquote_plus(enc), k)

    @unittest.skipUnless(shutil.which("java"), "java not installed")
    def test_round_trip_real_java_urldecoder(self):
        src = (
            "import java.net.URLDecoder;import java.nio.charset.StandardCharsets;"
            "import java.io.*;public class Dec{public static void main(String[] a)"
            "throws Exception{BufferedReader r=new BufferedReader(new "
            "InputStreamReader(System.in,StandardCharsets.UTF_8));PrintStream o="
            "new PrintStream(System.out,true,\"UTF-8\");String l;while((l=r.readLine())"
            "!=null)o.println(URLDecoder.decode(l,StandardCharsets.UTF_8));}}")
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "Dec.java")
            with open(path, "w") as f:
                f.write(src)
            stdin = "\n".join(bq.encode_key(k) for k in KEYS) + "\n"
            env = dict(os.environ)
            env.pop("JAVA_TOOL_OPTIONS", None)
            res = subprocess.run(["java", path], input=stdin.encode("utf-8"),
                                 capture_output=True, timeout=120, env=env)
            self.assertEqual(res.returncode, 0, res.stderr.decode())
            got = res.stdout.decode("utf-8").splitlines()
            self.assertEqual(got, KEYS)


class BodyTest(unittest.TestCase):
    def test_shape(self):
        body = bq.build_event_body("bkt", "us-west-2", KEYS[0], 123,
                                   '"etag1"', obj(KEYS[0])["LastModified"])
        d = json.loads(body)
        self.assertEqual(list(d), ["Records"])
        self.assertEqual(len(d["Records"]), 1)
        r = d["Records"][0]
        self.assertEqual(r["eventVersion"], "2.1")
        self.assertEqual(r["eventSource"], "aws:s3")
        self.assertEqual(r["awsRegion"], "us-west-2")
        self.assertEqual(r["eventName"], "ObjectCreated:Put")
        self.assertEqual(r["eventTime"], "2026-10-09T06:21:03.456Z")
        self.assertEqual(r["userIdentity"], {"principalId": "backfill"})
        self.assertEqual(r["s3"]["bucket"],
                         {"name": "bkt", "arn": "arn:aws:s3:::bkt"})
        o = r["s3"]["object"]
        self.assertEqual(o["key"], bq.encode_key(KEYS[0]))
        self.assertEqual(o["size"], 123)
        self.assertEqual(o["eTag"], "etag1")
        self.assertEqual(o["sequencer"], "backfill")

    def test_naive_datetime_treated_as_utc(self):
        self.assertEqual(bq.format_event_time(datetime(2026, 1, 2, 3, 4, 5)),
                         "2026-01-02T03:04:05.000Z")


class PackingTest(unittest.TestCase):
    def recs(self, n, keylen=0):
        return [bq.record_for(obj("p/" + "k" * keylen + "_%05d.json.gz" % i,
                                  i), "bkt", "us-west-2") for i in range(n)]

    def test_per_message_count(self):
        out = bq.pack_records(self.recs(25), 10)
        self.assertEqual([n for _, n in out], [10, 10, 5])
        for body, n in out:
            self.assertEqual(len(json.loads(body)["Records"]), n)

    def test_size_cap_splits_and_preserves_order(self):
        recs = self.recs(2000, keylen=200)
        out = bq.pack_records(recs, 1000)  # 1000 * ~900 B >> 256 KiB
        self.assertGreater(len(out), 2)
        got = []
        for body, n in out:
            self.assertLessEqual(len(body.encode("utf-8")), bq.MAX_BODY_BYTES)
            self.assertLessEqual(n, 1000)
            rs = json.loads(body)["Records"]
            self.assertEqual(len(rs), n)
            got.extend(r["s3"]["object"]["key"] for r in rs)
        self.assertEqual(got, [r["s3"]["object"]["key"] for r in recs])

    def test_exact_cap_boundary(self):
        # Greedy and tight: every body fits, and adding the next record to
        # any non-final body would have crossed the cap.
        recs = self.recs(50)
        for cap in (len(bq.build_body(recs[:3]).encode()),
                    len(bq.build_body(recs[:3]).encode()) - 1, 1500, 4000):
            out = bq.pack_records(recs, 50, max_bytes=cap)
            i = 0
            for body, n in out:
                self.assertEqual(body, bq.build_body(recs[i:i + n]))
                self.assertLessEqual(len(body.encode()), cap)
                if i + n < len(recs):
                    self.assertGreater(
                        len(bq.build_body(recs[i:i + n + 1]).encode()), cap)
                i += n
            self.assertEqual(i, len(recs))
        one = len(bq.build_body(recs[:1]).encode())
        self.assertEqual([n for _, n in bq.pack_records(recs[:2], 50,
                                                        max_bytes=one)],
                         [1, 1])

    def test_single_oversize_record_raises(self):
        with self.assertRaises(ValueError):
            bq.pack_records(self.recs(1), 5, max_bytes=50)

    def test_batch_entries_respects_count_and_bytes(self):
        bodies = bq.pack_records(self.recs(3000, keylen=100), 40)
        total = 0
        for entries, weights in bq.batch_entries(bodies):
            self.assertLessEqual(len(entries), 10)
            self.assertLessEqual(sum(len(e["MessageBody"].encode())
                                     for e in entries), bq.MAX_BATCH_BYTES)
            self.assertEqual(sorted(weights), sorted(e["Id"] for e in entries))
            total += sum(weights.values())
        self.assertEqual(total, 3000)


class PrefixAndRedactTest(unittest.TestCase):
    def test_refuses_empty_and_root(self):
        for p in ["", "/", "//", "  ", None]:
            with self.assertRaises(ValueError):
                bq.normalize_prefix(p)
        with self.assertRaises(ValueError):
            bq.normalize_prefix("s3://b/x/")

    def test_adds_slash(self):
        self.assertEqual(bq.normalize_prefix("cid-landing-jsongz"),
                         "cid-landing-jsongz/")
        self.assertEqual(bq.normalize_prefix("/a/b/"), "a/b/")

    def test_redact(self):
        r = bq.Redactor("my-bucket-111122223333", "111122223333")
        s = r("s3://my-bucket-111122223333/x https://sqs/111122223333/q 999988887777")
        self.assertNotIn("1111", s)
        self.assertNotIn("9999", s)
        self.assertIn("<bucket>", s)


class _FlakySQS:
    """Fails Id "0" with a transient error twice, Id "1" with a sender fault."""

    def __init__(self):
        self.calls = []

    def send_message_batch(self, QueueUrl, Entries):
        self.calls.append([e["Id"] for e in Entries])
        n = len(self.calls)
        ok, failed = [], []
        for e in Entries:
            if e["Id"] == "0" and n <= 2:
                failed.append({"Id": "0", "SenderFault": False, "Code": "X"})
            elif e["Id"] == "1":
                failed.append({"Id": "1", "SenderFault": True, "Code": "Bad"})
            else:
                ok.append({"Id": e["Id"]})
        return {"Successful": ok, "Failed": failed}


class SendBatchTest(unittest.TestCase):
    def test_retries_only_failed_entries(self):
        sqs = _FlakySQS()
        stats = bq.Stats(["q"])
        sleeps = []
        entries = [{"Id": str(i), "MessageBody": "{}"} for i in range(4)]
        lost = bq.send_batch(sqs, "q", "url", entries, stats,
                             sleep=sleeps.append, log=lambda m: None,
                             weights={"0": 5, "1": 3, "2": 1, "3": 1})
        self.assertEqual(sqs.calls, [["0", "1", "2", "3"], ["0"], ["0"]])
        self.assertEqual(len(sleeps), 2)
        self.assertLess(sleeps[0], sleeps[1] * 2)  # backoff grows (jittered)
        self.assertEqual(lost, 3)
        self.assertEqual(stats.sent["q"], 7)
        self.assertEqual(stats.sent_msgs["q"], 3)
        self.assertEqual(stats.failed["q"], 3)


class EndToEndFakeTest(unittest.TestCase):
    QUEUES = ["cid-landing-jsongz-paimon", "cid-landing-jsongz-iceberg"]

    def run_tool(self, n, rpm=1, limit=None, depth=4):
        s3 = FakeS3("bkt", "cid-landing-jsongz/", n)
        sqs = FakeSQS(fail_rate=0.1)
        urls = bq.resolve_queue_urls(sqs, self.QUEUES)
        stats = bq.Stats(list(urls))
        q, stop = queue.Queue(maxsize=100), threading.Event()
        t = threading.Thread(target=bq.list_objects, args=(
            s3, "bkt", "cid-landing-jsongz/", ".json.gz", depth, 8, limit, q,
            stats, stop))
        t.start()
        orig = bq._backoff
        bq._backoff = lambda attempt, sleep: None
        try:
            bq.send_all(sqs, urls, "bkt", "us-west-2", q, 8, stats,
                        log=lambda m: None, records_per_message=rpm)
        finally:
            bq._backoff = orig
        t.join()
        return s3, sqs, urls, stats

    def keys_in(self, bodies):
        out = []
        for b in bodies:
            for r in json.loads(b)["Records"]:
                out.append(urllib.parse.unquote_plus(r["s3"]["object"]["key"]))
        return out

    def test_every_file_to_every_queue_once(self):
        for rpm in (1, 7):
            s3, sqs, urls, stats = self.run_tool(1234, rpm=rpm)
            self.assertEqual(stats.listed, 1234)
            for name, url in urls.items():
                keys = self.keys_in(sqs.messages[url])
                self.assertEqual(len(keys), 1234, (rpm, name))
                self.assertEqual(set(keys), s3.expected_keys)
                self.assertEqual(stats.sent[name], 1234)
                self.assertEqual(stats.failed[name], 0)
                if rpm == 7:
                    self.assertEqual(len(sqs.messages[url]), -(-1234 // 7))

    def test_depth_variants_find_same_objects(self):
        for depth in (0, 2, 5, 9):
            s3, sqs, urls, stats = self.run_tool(300, depth=depth)
            self.assertEqual(stats.listed, 300, depth)

    def test_limit(self):
        s3, sqs, urls, stats = self.run_tool(500, limit=37)
        self.assertEqual(stats.listed, 37)
        for url in urls.values():
            self.assertEqual(len(self.keys_in(sqs.messages[url])), 37)


class CliTest(unittest.TestCase):
    def test_count_only_sends_nothing(self):
        here = os.path.dirname(os.path.abspath(__file__))
        res = subprocess.run(
            [sys.executable, os.path.join(here, "backfill_queues.py"),
             "--bucket", "bkt-123456789012", "--prefix", "cid-landing-jsongz/",
             "--fake-objects", "250", "--count-only"],
            capture_output=True, text=True, cwd=here, timeout=60)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("listed=250", res.stdout)
        self.assertNotIn("123456789012", res.stdout)
        self.assertNotIn("queue ", res.stdout)

    def test_cli_refuses_root_prefix(self):
        here = os.path.dirname(os.path.abspath(__file__))
        res = subprocess.run(
            [sys.executable, os.path.join(here, "backfill_queues.py"),
             "--bucket", "bkt", "--prefix", "/", "--fake-objects", "1"],
            capture_output=True, text=True, cwd=here, timeout=60)
        self.assertEqual(res.returncode, 2)
        self.assertIn("refusing", res.stderr)


if __name__ == "__main__":
    unittest.main()
