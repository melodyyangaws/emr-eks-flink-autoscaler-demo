# Trino 484 + the native Paimon connector (apache/paimon-trino), for the CDC
# query benchmark's Trino (deploy-trino.sh, catalog `paimon`). See README.md
# (Trino: native Paimon).
#
# apache/paimon-trino has no release at all (nothing on Maven Central; main is
# Trino 476 / Paimon 1.3.1, a 1.3-SNAPSHOT). This builds it from source:
#   - the open PR #127 head (Trino 483 / Paimon 1.4.2, JDK 25), pinned by commit;
#   - paimon-trino-484.patch on top: Trino 484 + airlift 451, Paimon 2.1.0 (= the
#     Flink writers' version), a <dependencyManagement> import of
#     io.trino:trino-root:484 so the plugin's third-party jars match Trino's
#     (without it Maven's nearest-wins kept httpclient5 5.2.1, and workers died at
#     catalog creation: NoClassDefFoundError ...ssl/TlsSocketStrategy, 2026-11-02;
#     the build checks for that class), and the four API changes that needs
#     (ConnectorMetadata.finishMerge returns Optional<ConnectorOutputMetadata>,
#     createMergeSink takes a MemoryContext, Paimon's SchemaManager is an
#     interface -> FileSystemSchemaManager, Catalog.commitSnapshot gained
#     baseSnapshotUuid). Tests are skipped: their helpers hit the same changes
#     and the benchmark only reads.
#
# Stages
#   build    maven + JDK 25: clone, check out the pinned commit, apply the patch,
#            `mvn package` -> target/paimon-trino-484-2.1.0-plugin.tar.gz
#   runtime  trinodb/trino:484 + that plugin in /usr/lib/trino/plugin/paimon
#            (minus trino-spi, which the server provides parent-first)
#
# amd64 only: Trino's pods run on m5 (trino-values.yaml nodeSelector). The
# plugin is plain Java, so an arm64 build would need only another Kaniko pod.

ARG TRINO_VERSION=484

FROM maven:3.9.16-eclipse-temurin-25 AS build
ARG PAIMON_TRINO_REPO=https://github.com/apache/paimon-trino.git
# PR #127 "Bump Trino to 483 and Paimon to 1.4.2" (head as of 2026-11-03)
ARG PAIMON_TRINO_COMMIT=a16e745e0500c78474388869d12033c9890de237
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git init -q paimon-trino && cd paimon-trino \
 && git remote add origin "${PAIMON_TRINO_REPO}" \
 && git fetch -q --depth 1 origin "${PAIMON_TRINO_COMMIT}" \
 && git checkout -q FETCH_HEAD
COPY paimon-trino-484.patch /src/
RUN cd paimon-trino && git apply /src/paimon-trino-484.patch \
 && mvn -B -q package -Dmaven.test.skip=true -Dcheckstyle.skip -Dspotless.check.skip=true -Drat.skip=true \
 && mkdir -p /out && tar -xzf target/paimon-trino-484-2.1.0-plugin.tar.gz -C /out \
 && rm -f /out/paimon/trino-spi-*.jar \
 && ls /out/paimon/paimon-trino-484-2.1.0.jar /out/paimon/paimon-bundle-2.1.0.jar \
 && for j in /out/paimon/httpclient5-*.jar; do jar tf "$j" | grep -q 'org/apache/hc/client5/http/ssl/TlsSocketStrategy.class' && ok=1 || true; done \
 && [ "${ok:-}" = 1 ] || { echo "plugin has no httpclient5 with TlsSocketStrategy (>= 5.4): $(ls /out/paimon | grep -i httpc)" >&2; exit 1; }

FROM trinodb/trino:${TRINO_VERSION}
COPY --from=build /out/paimon /usr/lib/trino/plugin/paimon
