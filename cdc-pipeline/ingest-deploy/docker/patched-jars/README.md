# Patched Paimon 2.1.0 jars

The writer image (`../Dockerfile.cdc-paimon`) replaces the Maven Central
`paimon-flink-1.20-2.1.0.jar` with the one here. Both jars were built 2026-11-06 from Paimon `release-2.1.0` plus a backport of
[apache/paimon PR #10329](https://github.com/apache/paimon/pull/10329)
("[iceberg] Write required Avro metadata to manifest files", a draft, unmerged).
It fixes [issue #8440](https://github.com/apache/paimon/issues/8440): Paimon's
Iceberg export wrote manifest files with no Iceberg key-value metadata (`schema`,
`schema-id`, `partition-spec`, `partition-spec-id`, `format-version`, `content`).
DuckDB 2.0 then refuses the v3 export: "Manifest ... missing required Avro
key-value metadata field 'schema'".

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

`build-deploy-generic.sh build` refuses to build without them.

## Rebuild

JDK 11 (here: `maven:3.9-eclipse-temurin-11`, in-cluster; Paimon targets Java 8 bytecode):

```bash
git clone --depth 1 --branch release-2.1.0 https://github.com/apache/paimon.git && cd paimon
gh api repos/apache/paimon/pulls/10329 -H "Accept: application/vnd.github.v3.diff" > pr10329.diff
git apply pr10329.diff            # applies cleanly to release-2.1.0 (line offsets only)
FLAGS="-Pflink1 -DskipTests -Dcheckstyle.skip -Dspotless.check.skip -Dspotless.apply.skip -Drat.skip -Denforcer.skip"
mvn -B -ntp -T 1C install $FLAGS -pl paimon-flink/paimon-flink-1.20 -am
mvn -B -ntp -T 1C install $FLAGS -pl paimon-hive/paimon-hive-connector-3.1 -am
# the PR's unit tests
mvn -B -ntp test -Pflink1 -Dcheckstyle.skip -Dspotless.check.skip -Drat.skip -Denforcer.skip \
    -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
    -pl paimon-format,paimon-core -Dtest='AvroFileFormatTest,IcebergManifestBlockSizeTest'
```

`-Pflink1` is needed: the profile that lists the Flink 1.x modules is only
`activeByDefault`, and a JDK-activated profile switches it off under JDK 11.
Outputs: `paimon-flink/paimon-flink-1.20/target/paimon-flink-1.20-2.1.0.jar` and
`paimon-hive/paimon-hive-connector-3.1/target/paimon-hive-connector-3.1-2.1.0.jar (copy it here as paimon-hive-connector-2.1.0.jar)`.

Check a jar carries the patch:

```bash
unzip -l <jar> | grep org/apache/paimon/format/SupportsFileMetadata.class   # new in the PR
unzip -p <jar> org/apache/paimon/iceberg/manifest/IcebergManifestFile.class | strings | grep -c partition-spec   # > 0
```

Drop these jars (and the Dockerfile's `COPY patched-jars/` step) once a Paimon
release that contains the fix is used.

> Named `paimon-hive-connector-2.1.0.jar`, not Maven's `paimon-hive-connector-3.1-2.1.0.jar`: the Dockerfile saves the downloaded connector under that shorter name, and a patched jar replaces only a jar of exactly the same name.
