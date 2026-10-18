"""In-memory S3 and SQS fakes for offline checks. No network, no AWS.

Used by tests and by `backfill_queues.py --fake-objects N`. FakeS3 only
supports listing; FakeSQS records every message it accepts and injects
transient (non-sender-fault) failures at a configurable rate so the retry
path runs.
"""

import json
import os
import random
import threading
from datetime import datetime, timedelta, timezone


def make_keys(prefix, n, seed=7):
    """n keys in the landing layout, spread over a few hours/customers.

    Also includes a few non-matching keys (wrong suffix, folder markers) and
    one object directly under the prefix to exercise the walk.
    """
    rnd = random.Random(seed)
    keys = []
    for i in range(n):
        day = 1 + i % 3
        hour = (i // 3) % 5
        cust = "cust %d+x" % (i % 4)  # space and '+' must be encoded
        prod = "p=%d" % (i % 2)
        keys.append("%syear=2026/month=10/day=%02d/hour=%02d/customerId=%s/"
                    "productCode=%s/part-%d_%03d_new.json.gz"
                    % (prefix, day, hour, cust, prod, rnd.randint(0, 9999), i))
    extras = [prefix + "year=2026/month=10/_SUCCESS",
              prefix + "year=2026/month=10/day=01/",
              prefix + "year=2026/month=10/day=01/hour=00/notes.txt"]
    return keys, extras


class _Paginator:
    def __init__(self, s3):
        self.s3 = s3

    def paginate(self, Bucket, Prefix="", Delimiter=None, PageSize=1000):
        assert Bucket == self.s3.bucket, "wrong bucket"
        with self.s3.lock:
            self.s3.list_calls += 1
        keys = [k for k in self.s3.sorted_keys if k.startswith(Prefix)]
        contents, prefixes = [], []
        seen = set()
        for k in keys:
            rest = k[len(Prefix):]
            if Delimiter and Delimiter in rest:
                cp = Prefix + rest.split(Delimiter, 1)[0] + Delimiter
                if cp not in seen:
                    seen.add(cp)
                    prefixes.append({"Prefix": cp})
            else:
                contents.append(self.s3.objects[k])
        items = [("c", c) for c in contents] + [("p", p) for p in prefixes]
        if not items:
            yield {"KeyCount": 0}
            return
        for i in range(0, len(items), PageSize):
            chunk = items[i:i + PageSize]
            page = {"KeyCount": len(chunk)}
            c = [x for t, x in chunk if t == "c"]
            p = [x for t, x in chunk if t == "p"]
            if c:
                page["Contents"] = c
            if p:
                page["CommonPrefixes"] = p
            yield page


class FakeS3:
    def __init__(self, bucket, prefix, n, seed=7):
        self.bucket = bucket
        self.lock = threading.Lock()
        self.list_calls = 0
        keys, extras = make_keys(prefix, n, seed)
        self.expected_keys = set(keys)
        base = datetime(2026, 10, 9, 6, 0, 0, 123000, tzinfo=timezone.utc)
        self.objects = {}
        for i, k in enumerate(keys + extras):
            self.objects[k] = {"Key": k, "Size": 1000 + i,
                               "ETag": '"%032x"' % i,
                               "LastModified": base + timedelta(seconds=i)}
        # Noise outside the prefix that must never be picked up.
        other = prefix.rstrip("/") + "-r3/year=2026/x_001_new.json.gz"
        self.objects[other] = {"Key": other, "Size": 1, "ETag": '"0"',
                               "LastModified": base}
        self.sorted_keys = sorted(self.objects)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _Paginator(self)


class FakeSQS:
    def __init__(self, region="us-west-2", fail_rate=None, seed=11):
        self.region = region
        if fail_rate is None:
            fail_rate = float(os.environ.get("FAKE_FAIL_RATE", "0.05"))
        self.fail_rate = fail_rate
        self.rnd = random.Random(seed)
        self.lock = threading.Lock()
        self.messages = {}  # queue url -> [body]
        self.calls = 0

    def get_queue_url(self, QueueName):
        return {"QueueUrl": "https://sqs.%s.amazonaws.com/123456789012/%s"
                            % (self.region, QueueName)}

    def send_message_batch(self, QueueUrl, Entries):
        assert 1 <= len(Entries) <= 10, "batch size %d" % len(Entries)
        ids = [e["Id"] for e in Entries]
        assert len(set(ids)) == len(ids), "duplicate Ids"
        total = sum(len(e["MessageBody"].encode("utf-8")) for e in Entries)
        assert total <= 256 * 1024, "batch payload %d bytes" % total
        ok, failed = [], []
        with self.lock:
            self.calls += 1
            for e in Entries:
                json.loads(e["MessageBody"])  # must be valid JSON
                if self.rnd.random() < self.fail_rate:
                    failed.append({"Id": e["Id"], "SenderFault": False,
                                   "Code": "InternalError",
                                   "Message": "fake transient"})
                else:
                    self.messages.setdefault(QueueUrl, []).append(
                        e["MessageBody"])
                    ok.append({"Id": e["Id"], "MessageId": "m"})
        resp = {"Successful": ok}
        if failed:
            resp["Failed"] = failed
        return resp
