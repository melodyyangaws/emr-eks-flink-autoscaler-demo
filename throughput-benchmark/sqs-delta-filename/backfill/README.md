# SQS backfill for existing landing files

S3 event notifications only announce objects created after they were set up.
To run the SQS-driven Flink jobs against a landing prefix that already exists
(for example `s3://<bucket>/cid-landing-jsongz/`), `backfill_queues.py` lists
the existing `.json.gz` objects once, outside Flink. It then sends one
synthetic S3 `ObjectCreated:Put` event per object to every target queue, so
each job sees every file. The default queues are
`cid-landing-jsongz-paimon` and `cid-landing-jsongz-iceberg` in us-west-2.

> **Purge the queues before you re-run.** Every run sends every file again.
> If a queue still holds messages from an earlier run, or the job has
> checkpointed only part of them, the job will read those files twice and
> write duplicate rows.
>
> ```bash
> aws sqs purge-queue --queue-url "$(aws sqs get-queue-url --queue-name cid-landing-jsongz-paimon --query QueueUrl --output text)"
> aws sqs purge-queue --queue-url "$(aws sqs get-queue-url --queue-name cid-landing-jsongz-iceberg --query QueueUrl --output text)"
> ```
>
> SQS allows one purge per queue every 60 s, and a purge can take up to 60 s
> to finish.

## Usage

`backfill-queues.sh` sources `../../.bench-env`, which in turn sources
`.bench-env.local`, to get `BUCKET_NAME`, `AWS_REGION` and
`AWS_ACCOUNT_ID`. It passes every argument through to the Python script.
The script needs Python 3 and boto3.

```bash
cd throughput-benchmark/sqs-delta-filename/backfill

# 1. Count only: lists the objects and sends nothing. Shows a sample body.
./backfill-queues.sh --prefix cid-landing-jsongz/ --count-only --show-sample

# 2. Small test: the first 20 files, to both queues.
./backfill-queues.sh --prefix cid-landing-jsongz/ --limit 20

# 3. Full run.
./backfill-queues.sh --prefix cid-landing-jsongz/

# One queue only, or a queue URL instead of a name.
./backfill-queues.sh --prefix cid-landing-jsongz/ --queues cid-landing-jsongz-paimon
```

| Option | Default | Meaning |
|---|---|---|
| `--prefix` | required | Key prefix to backfill. A trailing `/` is added if missing, so `cid-landing-jsongz` does not also match `cid-landing-jsongz-r3/`. An empty prefix or `/` is refused. |
| `--queues` | both queues | Comma-separated queue names or URLs. |
| `--suffix` | `.json.gz` | Only keys ending with this suffix are sent. |
| `--list-workers` | 32 | Parallel `ListObjectsV2` threads. |
| `--send-workers` | 32 | Parallel `SendMessageBatch` threads, shared by all queues. |
| `--records-per-message` | 1 | Pack up to N files into one message's `Records` array. See [Packing several files per message](#packing-several-files-per-message). |
| `--discover-depth` | 4 | Number of folder levels walked with `Delimiter='/'` before the leaf folders are listed: 4 stops at `hour=HH/`, 5 at `customerId=.../`. |
| `--limit N` | none | Stop after N matching files. |
| `--count-only` | off | List and count, and print the request count a real run would make. This still reads S3; it is not a dry run of the send. |
| `--show-sample` | off | Print the first message body, with the bucket redacted. |
| `--bucket`, `--region` | `$BUCKET_NAME`, `$AWS_REGION` | Override the values from the env file. |

Progress is printed every 10 s: files listed, files sent per queue, messages,
failures, `SendMessageBatch` calls and rates. A final summary prints the
totals and the elapsed time. The exit code is 1 if any file was not delivered
to any queue.

Output never shows the real bucket name, which is printed as `<bucket>`.
Account IDs, including the one inside queue URLs, are printed as
`<account>`. Message bodies sent to SQS do contain the real bucket name,
because the Flink source needs it.

## How it works

**Listing.** The tool first walks the partition folders
(`year=/month=/day=/hour=`) with `ListObjectsV2` and `Delimiter='/'`, one
level at a time and in parallel. It then lists each leaf folder recursively
with the paginator on a thread pool of `--list-workers` threads. A few
hundred thousand keys take seconds, not the minutes a single sequential
listing would take. Objects that sit above the leaf level are picked up during
the walk. Sending starts while listing is still running. S3 is only listed:
the tool never calls `GetObject`, `PutObject`, `DeleteObject` or `CopyObject`.

**Message body.** The body has the same shape as a real S3 notification, so
the Flink source parses both the same way:

```json
{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-west-2",
 "eventTime":"2026-10-09T06:21:03.456Z","eventName":"ObjectCreated:Put",
 "userIdentity":{"principalId":"backfill"},
 "requestParameters":{"sourceIPAddress":"backfill"},
 "s3":{"s3SchemaVersion":"1.0","configurationId":"backfill",
  "bucket":{"name":"<bucket>","arn":"arn:aws:s3:::<bucket>"},
  "object":{"key":"cid-landing-jsongz/year%3D2026/month%3D10/...json.gz",
   "size":123456,"eTag":"0123...","sequencer":"backfill"}}}]}
```

- `eventTime` is the object's `LastModified` time.
- `eTag` has its surrounding quotes removed, as in real events.
- The key is form-encoded the way S3 encodes it, using
  `urllib.parse.quote_plus(key, safe='/')`. So `=` becomes `%3D` and a space
  becomes `+`. `java.net.URLDecoder.decode(key, UTF_8)` restores the
  original key; the tests check this against a real JVM.
- Backfilled messages can be told apart from real ones by
  `userIdentity.principalId == "backfill"`, `configurationId == "backfill"`
  and `sequencer == "backfill"`.

**Sending.** The tool sends `SendMessageBatch` calls of up to 10 entries,
with every queue receiving the same message sequence. A batch is also capped
at 256 KiB of total body size. If a response lists failed entries, only those
entries are retried, with exponential backoff and jitter, for up to 8
attempts. Entries flagged `SenderFault` are not retried, because they would
fail again. Throttling and network errors on the whole call are retried in
the same way, and botocore's adaptive retry runs underneath.

## Packing several files per message

`--records-per-message N` puts up to N records into one message's `Records`
array. A message body may not exceed 256 KiB, so the tool measures the
encoded size and starts a new message whenever the next record would cross
that limit. A typical record is about 650 bytes, so one message holds about
400 records at most. `SendMessageBatch` has the same 256 KiB limit for the
total payload of a call, so batches of large messages hold fewer than 10
messages.

Trade-offs:

- **Benefit: fewer requests and lower cost.** Fewer messages means fewer
  send, receive and delete calls. The number of send calls stops falling
  at about N = 40, because a batch is then full by bytes rather than by
  entry count. Receive and delete calls on the Flink side keep falling as N
  grows.
- **Cost: a redelivery re-reads all N files.** If a message's visibility
  timeout expires, or the job fails over before the message is deleted, SQS
  redelivers the whole message and the job reads all N files again.
- **Cost: deletes wait for the slowest file.** A message is deleted only
  after all N of its files are checkpointed, so it stays in flight longer.
  Set the visibility timeout to cover the time it takes to process N files.
  The connector tracks this with a per-message reference count.

Keep N = 1 unless the request count matters. That gives one-to-one parity
with real S3 events, which always carry one record.

## Cost

SQS standard queues cost about **$0.40 per million requests**. Each 64 KiB
chunk of a payload is billed as one request. With the default of one file per
message, a backfill of N files to 2 queues makes about **2 × N / 10**
`SendMessageBatch` calls:

| Files | Send calls (2 queues) | Send cost |
|---|---|---|
| 100,000 | 20,000 | about $0.008 |
| 500,000 | 100,000 | about $0.04 |
| 1,000,000 | 200,000 | about $0.08 |

Each batch at N = 1 is under 64 KiB, so it is billed as one request. The
consuming jobs then make receive and delete calls, which cost about the same
again or more, depending on how many messages each receive returns. S3
listing costs $0.005 per 1,000 `LIST` requests, and each request returns up
to 1,000 keys, so listing costs well under a cent. `--count-only` prints the
expected number of send calls before you commit to a run.

## Tests

The tests run offline and make no AWS calls:

```bash
python3 -m pytest -q          # or: python3 -m unittest -v test_backfill_queues
```

They cover:

- Key encoding: `=` becomes `%3D`, and spaces, `+`, `%` and Unicode
  round-trip. They are decoded both through a Python model of
  `URLDecoder` and through a real `java.net.URLDecoder` (skipped if `java`
  is not installed).
- The exact shape of the message body.
- Size-capped packing: each body is at most 256 KiB, packing is greedy and
  tight, record order is kept, and a single oversized record is rejected.
  Batches are capped at 10 entries and 256 KiB.
- Retries: only failed entries are retried, and sender faults are not.
- An end-to-end run over in-memory S3 and SQS fakes (`fake_aws.py`) with
  injected transient failures. Every file reaches every queue exactly once.
  The fakes also check `--limit`, the different discovery depths, the refusal
  of an empty or `/` prefix, and the redaction of the account ID in output.

To exercise the whole CLI offline, use the hidden `--fake-objects N` flag:

```bash
./backfill-queues.sh --prefix cid-landing-jsongz/ --fake-objects 5000 --records-per-message 1
```
