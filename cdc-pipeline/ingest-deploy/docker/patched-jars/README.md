# Patched Paimon 2.1.0 jars

The writer image (`../Dockerfile.cdc-paimon`) replaces the Maven Central
`paimon-flink-1.20-2.1.0.jar` with the one here. Both jars were built 2026-11-06 from Paimon `release-2.1.0` plus a backport of
[apache/paimon PR #10329](https://github.com/apache/paimon/pull/10329)
("[iceberg] Write required Avro metadata to manifest files", a draft, unmerged).
It fixes [issue #8440](https://github.com/apache/paimon/issues/8440): the Iceberg
metadata Paimon writes next to its own (`metadata.iceberg.*`, read by the query
benchmark's `paimon` arm as Paimon-as-iceberg) had manifest files with no Iceberg
key-value metadata (`schema`, `schema-id`, `partition-spec`, `partition-spec-id`,
`format-version`, `content`). DuckDB 2.0 then refuses it: "Manifest ... missing
required Avro key-value metadata field 'schema'". Manifests written before a
writer ran this jar still lack the metadata, so start a new `SESSION_SUFFIX` after
switching (the `_v3final` round, 2026-11-07, read Paimon-as-iceberg on DuckDB this way).

| jar | sha256 |
|---|---|
| `paimon-flink-1.20-2.1.0.jar` | `bbee7fc58b1b8835edeb9e3c38d6da04841aa64fa43c083270e2f22b0c2cc240` |
| `paimon-hive-connector-2.1.0.jar` | `92365b714580032d172178a8f0732d126547991adc8088f494a9c14d9ee81335` |

Only `paimon-flink-1.20-2.1.0.jar` is used, since 2026-11-07. The image used to
install `paimon-hive-connector-3.1` too, and that jar ships its own unrelocated
copy of `IcebergManifestFile` / `AvroFileFormat`, so it had to be patched as well.
The image no longer installs it: it is the Hive engine connector, and every
class it shares with the patched `paimon-flink-1.20` is byte-identical (the
Dockerfile comments have the evidence). `paimon-hive-connector-2.1.0.jar` may
stay in this directory (the Dockerfile copies only the Flink jar), but the build
context is smaller without it. Keep it in S3 in case the connector comes back.
If it does, it must be the patched build too.

The jars are git-ignored; the copy is in
`s3://$BUCKET_NAME/flink/build/patched-jars/`:

```bash
aws s3 cp --recursive s3://$BUCKET_NAME/flink/build/patched-jars/ ingest-deploy/docker/patched-jars/
```

`build-deploy-generic.sh build` refuses to build when this directory holds no jar;
the Dockerfile's `COPY` needs `paimon-flink-1.20-2.1.0.jar` itself.

## Rebuild

JDK 11 (here: `maven:3.9-eclipse-temurin-11`, in-cluster; Paimon targets Java 8 bytecode):

```bash
git clone --depth 1 --branch release-2.1.0 https://github.com/apache/paimon.git && cd paimon
gh api repos/apache/paimon/pulls/10329 -H "Accept: application/vnd.github.v3.diff" > pr10329.diff
git apply pr10329.diff            # applies cleanly to release-2.1.0 (line offsets only)
FLAGS="-Pflink1 -DskipTests -Dcheckstyle.skip -Dspotless.check.skip -Dspotless.apply.skip -Drat.skip -Denforcer.skip"
mvn -B -ntp -T 1C install $FLAGS -pl paimon-flink/paimon-flink-1.20 -am
# only if the Hive connector comes back into the image:
# mvn -B -ntp -T 1C install $FLAGS -pl paimon-hive/paimon-hive-connector-3.1 -am
# the PR's unit tests
mvn -B -ntp test -Pflink1 -Dcheckstyle.skip -Dspotless.check.skip -Drat.skip -Denforcer.skip \
    -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
    -pl paimon-format,paimon-core -Dtest='AvroFileFormatTest,IcebergManifestBlockSizeTest'
```

`-Pflink1` is needed: the profile that lists the Flink 1.x modules is only
`activeByDefault`, and a JDK-activated profile switches it off under JDK 11.
Output: `paimon-flink/paimon-flink-1.20/target/paimon-flink-1.20-2.1.0.jar` (and,
with the Hive line, `paimon-hive/paimon-hive-connector-3.1/target/paimon-hive-connector-3.1-2.1.0.jar`).

Check a jar carries the patch:

```bash
unzip -l <jar> | grep org/apache/paimon/format/SupportsFileMetadata.class   # new in the PR
unzip -p <jar> org/apache/paimon/iceberg/manifest/IcebergManifestFile.class | strings | grep -c partition-spec   # > 0
```

Drop these jars (and the Dockerfile's `COPY patched-jars/` step) once a Paimon
release that contains the fix is used.

> The stored copy is named `paimon-hive-connector-2.1.0.jar`, not Maven's
> `paimon-hive-connector-3.1-2.1.0.jar`, because the Dockerfile used to save the
> downloaded connector under that shorter name, and a patched jar replaces only a
> downloaded jar of exactly the same name. The Dockerfile no longer downloads it
> (2026-11-07); a returning connector download would have to use that name again.
