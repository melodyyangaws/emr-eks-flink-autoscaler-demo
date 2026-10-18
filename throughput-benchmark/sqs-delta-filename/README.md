# sqs-delta-filename: a Flink table source for the files announced on SQS

This is a replacement for the continuous `filesystem` source over the gzip NDJSON landing zone. That source lists the
whole S3 prefix on the JobManager at startup, and once the prefix holds more than 200k files the JM crash-loops
with exit 239. It also re-lists everything on every interval. This connector never lists S3. Instead it consumes a
queue of S3 `ObjectCreated` events (S3 → SNS → SQS with raw delivery) and reads only the announced files, using
Flink's own `FileSourceReader` + `TextLineInputFormat`:

- `.gz` is decompressed based on the file extension.
- Each file is one split.
- Restarts resume mid-file.

The table has one column, `line STRING`, and is unbounded.

```sql
ADD JAR 's3a://<bucket>/flink/jars/sqs-delta-filename.jar';

CREATE TEMPORARY TABLE s3json_raw (line STRING) WITH (
  'connector' = 'sqs-delta-filename',
  'queue-url' = 'https://sqs.us-west-2.amazonaws.com/<account>/cid-landing-jsongz-paimon'
);
```

## Options

| Option | Default | |
|---|---|---|
| `queue-url` | (required) | SQS queue URL. Give each job its own queue. |
| `region` | `us-west-2` | |
| `path-scheme` | `s3a` | Files are read as `<scheme>://<bucket>/<key>`, through the job's Hadoop S3A filesystem. |
| `key-prefix` | none | Keys (after URL-decoding) must start with this. |
| `key-suffix` | `.json.gz` | Keys must end with this. Gzip is detected from `.gz`. |
| `poll-threads` | `8` | Concurrent long polls (10 messages each) on the JobManager. |
| `max-pending-splits` | 4 × parallelism | Polling pauses at this many unassigned files. This is a soft cap: a poll already in flight can add up to 10 messages' worth of files. |
| `visibility-timeout` | `15min` | Sent on every ReceiveMessage. |
| `wait-time` | `20 s` | Long-poll wait, from 0 to 20 s. |
| `dedup-grace` | `30min` | How long a committed key is remembered, so that duplicate events for it are dropped. |

Durations use Flink syntax (`15min`, `20 s`, `900s`).

Message bodies are handled as follows:

- **Raw S3 events and the same events in an SNS envelope** (`"Message": "<json>"`) are both accepted. A message can
  carry many `Records`, and backfill messages do.
- **`s3:TestEvent`**, non-`ObjectCreated` records and keys that fail the prefix/suffix filter are deleted
  immediately.
- **Bodies that are not JSON** are left alone, so they reach the DLQ after `maxReceiveCount`.
- **Keys** are URL-decoded (`%3D` becomes `=` and `+` becomes a space).
- **File size** comes from the event, so no HEAD request is needed. A HEAD is made only if `size` is missing.

## IAM

The JobManager role (IRSA, resolved through `DefaultCredentialsProvider`) needs these actions on the queue:

- `sqs:ReceiveMessage`
- `sqs:DeleteMessage`. The connector calls it as `DeleteMessageBatch`.
- `sqs:ChangeMessageVisibility`. The connector calls it as `ChangeMessageVisibilityBatch`, and only on enumerator
  close, to release the messages it still holds.
- `sqs:GetQueueAttributes`

The TaskManagers need only S3 read access, which they already have.

## Delivery semantics

- **A message is deleted only after its data is in a completed checkpoint.** Each message keeps a refcount of the
  files it announced. A file counts as done once it was assigned to a reader at or before checkpoint N and N
  completes: from then on Flink's reader state owns it, including a mid-file position. The message is deleted
  (`DeleteMessageBatch`, async) only when its refcount reaches 0.
- **The enumerator state** holds the unassigned files with their message ids, the assigned but unconfirmed files,
  and the newest receipt handle per message.
- **On restore**, the checkpoint the job restored from has completed, so its assigned files are confirmed and
  their messages are deleted at start. Files assigned after that checkpoint are not in the state. Their messages
  were never deleted, so SQS redelivers them. On close the connector sets their visibility to 0, so that happens
  at once instead of after the visibility timeout.
- **Duplicates are possible, so the pipeline is at-least-once per file.** S3 events are themselves at-least-once.
  Duplicates are dropped when the key is pending, assigned, or committed within `dedup-grace`. A duplicate is
  read again in these cases:
  - It arrives after `dedup-grace`.
  - It arrives after a restart, because the done-set lives in memory only. Only the restored checkpoint's own
    files are re-seeded into it.

  Downstream primary-key upserts (Paimon PK tables, Iceberg upsert) absorb the duplicate rows. Append tables do
  not.
- **Redelivery.** When the same message (same message id) is redelivered while it is tracked, the stored handle is
  replaced, because only the newest handle deletes. When a different message announces a key that is already
  tracked or done, that record is counted as a duplicate. If none of the message's records are new, the message
  is deleted at once.
- **Visibility.** If a message stays undeleted longer than `visibility-timeout` (slow or failing checkpoints, or a
  backfill message announcing hundreds of files), SQS redelivers it. That is harmless, as described above, but
  every redelivery counts toward `maxReceiveCount` (10). After about 10 × 15 min of failing checkpoints a
  message moves to the DLQ and its files are not read. Redrive the DLQ after an outage.
- **Missing files.** A file that is announced but deleted before it is read fails the reader, just as the
  `filesystem` source would.
- **Memory.** The done-set stores 64-bit hashes in time buckets, which comes to tens of MB for about 1M keys
  (600 files/s for 30 min).

## Observability

The JobManager logs one INFO line every 60 s with these cumulative counts:

- received, redelivered
- splits, duplicates, ignored, malformed
- deleted, deleteFailures, pollErrors

The same line also gives the current pending, assigned-unconfirmed, in-flight-message and done-set sizes. The
enumerator metric group also exposes the gauges `pendingSplits` and `inflightHandles`.

## Build

```bash
mvn -q package   # → target/sqs-delta-filename-1.0.0.jar (shaded, ~6 MB)
```

The build compiles with `--release 11`. Flink (1.20.0) and `flink-shaded-jackson` are `provided`. AWS SDK v2
(`sqs`, `sts` for IRSA, `url-connection-client`) is shaded and relocated under
`com.demo.flink.sqs.shaded.software.amazon`, so it cannot clash with `/usr/share/aws/aws-java-sdk-v2/*` on the
EMR image.
