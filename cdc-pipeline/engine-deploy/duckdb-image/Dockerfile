# DuckDB v2.0.0 built FROM SOURCE for the CDC benchmark's warm server
# (duckdb_cdc_server.py), with iceberg, httpfs, aws and avro STATICALLY linked
# into the Python module. No CLI: the server only imports duckdb. See README.md.
#
# Stages
#   deps       toolchain, vcpkg at DuckDB's pinned release, the DuckDB sources
#              (duckdb-python v2.0.0, whose external/duckdb submodule IS duckdb
#              v2.0.0), the extension sources at the commits the v2.0.0 tag pins,
#              and every vcpkg dependency.
#   py-build   the duckdb Python wheel (scikit-build-core, Release).
#   paimon-build  the Paimon extension (polardb/duckdb-paimon) as a LOADABLE
#              extension, compiled against the same DuckDB tree, with paimon-cpp
#              moved to a commit that reads bitmap64 deletion vectors.
#   runtime    python slim + the wheel + awscli + cache_httpfs + paimon; LOADs
#              iceberg, httpfs and paimon, so an image that cannot never gets pushed.
#
# Builds natively on linux/amd64 and linux/arm64 (the triplet follows
# `uname -m`), so it works under Kaniko (one pod per arch, like
# cdc-pipeline/ingest-deploy/build-deploy-generic.sh) and under `docker buildx` alike. It uses
# no BuildKit-only syntax (no RUN --mount), because Kaniko ignores it.
#
# Parallelism: COMPILE_JOBS parallel C++ compile jobs (make -j; default 60, a
# 16xlarge node). DuckDB's unity build needs ~1.5-2 GiB per job.

ARG PYTHON_VERSION=3.12
# trixie (GCC 14), not bookworm (GCC 12): GCC 12 rejects httpfs 77392a72's
# `return result;` of a unique_ptr<HTTPFSParams> as unique_ptr<HTTPParams>
# (httpfs.cpp:215, first Kaniko build 2026-11-02). Build and runtime stages share
# the suite, so the binaries' glibc matches.
ARG DEBIAN_SUITE=trixie

#################################################################################
FROM python:${PYTHON_VERSION}-${DEBIAN_SUITE} AS deps
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# --- pins (all from the duckdb v2.0.0 tag, checked 2026-11-02) ---------------
ARG DUCKDB_TAG=v2.0.0
ARG DUCKDB_COMMIT=d54cbbe3df17e8e5ce68312f483b4155836112aa
ARG DUCKDB_PYTHON_COMMIT=83c719961098611e769dc4bab0bda2bf149a2d1e
# Makefile `setup-vcpkg` and .github/workflows/Main.yml at v2.0.0: release 2026.06.24
ARG VCPKG_TAG=2026.06.24
ARG VCPKG_COMMIT=cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3
# .github/config/extensions/<name>.cmake GIT_TAG at v2.0.0. Only verified here;
# the source of truth is the tag (scripts/sync_out_of_tree_extensions.py reads it).
ARG ICEBERG_COMMIT=4c0c0874c48ec82a3e29910554891d04bcc0e988
ARG HTTPFS_COMMIT=77392a72077a85897db2519fae11e2c0da972b8a
ARG AWS_COMMIT=16ae9c135300f750478d648c2496f2952a6f2827
ARG AVRO_COMMIT=758fb6063cbb6da49b2e1c16c4044cfcafefc52f
ARG EXTENSIONS="iceberg;httpfs;aws;avro"
# duckdb-python needs CMake >= 3.29; 3.x rather than 4.x because some vcpkg ports
# still declare cmake_minimum_required < 3.5, which CMake 4 rejects.
ARG CMAKE_VERSION=3.31.6
ARG COMPILE_JOBS=60

# ARGs end with their stage; py-build (FROM deps) reads these ENVs.
# VCPKG_FORCE_SYSTEM_BINARIES: required on arm64 (vcpkg ships no aarch64
# cmake/ninja), harmless on amd64.
ENV DDB_TAG=${DUCKDB_TAG} \
    DDB_COMMIT=${DUCKDB_COMMIT} \
    DDB_EXTENSIONS=${EXTENSIONS} \
    COMPILE_JOBS=${COMPILE_JOBS} \
    DEBIAN_FRONTEND=noninteractive \
    VCPKG_ROOT=/opt/vcpkg \
    VCPKG_DISABLE_METRICS=1 \
    VCPKG_FORCE_SYSTEM_BINARIES=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential git ca-certificates curl zip unzip tar pkg-config \
        ninja-build bison flex perl autoconf automake libtool linux-libc-dev \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir "cmake==${CMAKE_VERSION}" \
 && cmake --version | head -1 && ninja --version && gcc --version | head -1

# vcpkg: a FULL clone, because manifest versioning (builtin-baseline, the openssl
# 3.6.0 override) reads port versions out of the registry's git history.
RUN git clone --quiet --branch "${VCPKG_TAG}" https://github.com/microsoft/vcpkg.git "${VCPKG_ROOT}" \
 && test "$(git -C "${VCPKG_ROOT}" rev-parse HEAD)" = "${VCPKG_COMMIT}" \
 && "${VCPKG_ROOT}/bootstrap-vcpkg.sh" -disableMetrics \
 && case "$(uname -m)" in \
        x86_64)  echo x64-linux-release ;; \
        aarch64) echo arm64-linux-release ;; \
        *) echo "unsupported arch $(uname -m)" >&2; exit 1 ;; \
    esac > /opt/vcpkg-triplet \
 && echo "vcpkg triplet: $(cat /opt/vcpkg-triplet)"

# DuckDB sources. duckdb-python v2.0.0 vendors duckdb as a submodule at exactly
# the v2.0.0 commit, so the wheel is built from exactly that tree.
WORKDIR /src
RUN git clone --quiet --branch "${DUCKDB_TAG}" https://github.com/duckdb/duckdb-python.git duckdb-python \
 && test "$(git -C duckdb-python rev-parse HEAD)" = "${DUCKDB_PYTHON_COMMIT}" \
 && git -C duckdb-python submodule update --init --recursive --depth 1 \
 && test "$(git -C duckdb-python/external/duckdb rev-parse HEAD)" = "${DUCKDB_COMMIT}" \
 && echo "duckdb ${DUCKDB_TAG} = $(git -C duckdb-python/external/duckdb rev-parse HEAD)"

# Out-of-tree extensions at the commits the tag pins, synced into
# external/duckdb/extension/external/<name> with DuckDB's own script (it applies
# .github/patches/extensions/<name> too: avro gets one patch commit on top of its
# pin), plus the merged vcpkg manifest of all four, then every vcpkg dependency
# built into /opt/vcpkg_installed (aws-sdk-cpp, curl, openssl 3.6.0, roaring,
# jansson, liblzma, snappy, zlib).
WORKDIR /src/duckdb-python/external/duckdb
# `set -- ${pin}` splits "name commit" on purpose.
# hadolint ignore=SC2086
RUN VCPKG_TOOLCHAIN_PATH="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" \
        python3 scripts/sync_out_of_tree_extensions.py \
        --build-extensions "${EXTENSIONS}" --output-dir /src/vcpkg-manifest \
 && for pin in "iceberg ${ICEBERG_COMMIT}" "httpfs ${HTTPFS_COMMIT}" "aws ${AWS_COMMIT}" "avro ${AVRO_COMMIT}"; do \
        set -- ${pin}; d="extension/external/$1"; \
        git -C "$d" merge-base --is-ancestor "$2" HEAD \
          || { echo "extension $1 is not at its v2.0.0 pin $2" >&2; exit 1; }; \
        echo "extension $1 @ $2 (+$(git -C "$d" rev-list --count "$2"..HEAD) patch commit(s))"; \
    done \
 && cat /src/vcpkg-manifest/vcpkg.json \
 && triplet="$(cat /opt/vcpkg-triplet)" \
 && VCPKG_MAX_CONCURRENCY="${COMPILE_JOBS}" "${VCPKG_ROOT}/vcpkg" install \
        --x-manifest-root=/src/vcpkg-manifest --x-install-root=/opt/vcpkg_installed \
        --triplet="${triplet}" --host-triplet="${triplet}" --clean-after-build \
 && rm -rf "${VCPKG_ROOT}/buildtrees" "${VCPKG_ROOT}/downloads" "${VCPKG_ROOT}/packages" \
 && find /opt/vcpkg_installed/"${triplet}"/lib -maxdepth 1 -name "*.a" | sort

###############################################################################
FROM deps AS py-build
# The wheel's BUILD_EXTENSIONS replaces pyproject's "core_functions;json;parquet;icu",
# so those four are listed again; LINK_EXTENSIONS stays "" = link all of them.
# DUCKDB_NEW_EXTENSION_BUILD=1 makes DuckDB's CMake take the extension sources
# synced in `deps` (extension/external/<name>) instead of fetching them again.
# OVERRIDE_GIT_DESCRIBE=v2.0.0 makes it the 2.0.0 release build (package 2.0.0,
# engine v2.0.0), not a 2.0.0.devN. No LTO: PyPI's Linux wheels are clang+ThinLTO.
WORKDIR /src/duckdb-python
RUN triplet="$(cat /opt/vcpkg-triplet)" \
 && DUCKDB_NEW_EXTENSION_BUILD=1 \
    OVERRIDE_GIT_DESCRIBE="${DDB_TAG}" \
    CMAKE_BUILD_PARALLEL_LEVEL="${COMPILE_JOBS}" \
    CMAKE_GENERATOR=Ninja \
    pip wheel --no-deps --wheel-dir /out/wheels -v . \
        -C build-dir=/tmp/pybuild \
        -C cmake.define.BUILD_EXTENSIONS="core_functions;json;parquet;icu;${DDB_EXTENSIONS}" \
        -C cmake.define.CMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" \
        -C cmake.define.VCPKG_TARGET_TRIPLET="${triplet}" \
        -C cmake.define.VCPKG_HOST_TRIPLET="${triplet}" \
        -C cmake.define.VCPKG_MANIFEST_DIR=/src/vcpkg-manifest \
        -C cmake.define.VCPKG_INSTALLED_DIR=/opt/vcpkg_installed \
        -C cmake.define.VCPKG_MANIFEST_INSTALL=OFF \
        -C cmake.define.VCPKG_BUILD=1 \
 && rm -rf /tmp/pybuild \
 && ls -l /out/wheels

###############################################################################
FROM deps AS paimon-build
# The community binary (paimon 0.1.3 = duckdb-paimon 6762e34b + paimon-cpp
# 53f9c86d) cannot read bitmap64 deletion vectors ("Size not match" on every scan
# of our tables, deletion-vectors.bitmap64=true). paimon-cpp cecd6cd0 (#423,
# 2026-10-13) added them; duckdb-paimon still pins 53f9c86d, so the submodule is
# moved here. duckdb-paimon's own duckdb submodule is d54cbbe3 = our tree, so the
# extension is configured on /src/duckdb-python/external/duckdb directly (no
# second DuckDB checkout) and links curl/openssl from deps' /opt/vcpkg_installed.
ARG DUCKDB_PAIMON_COMMIT=6762e34b3f805f00511e5a21c6a04c9c9a904f35
ARG PAIMON_CPP_COMMIT=cecd6cd05a5b72b2c71c5b311e0f8212aba0b2c0
# Shallow fetch of one commit each (ExternalProject's PATCH_COMMAND needs a git
# repo). The one source patch, for API drift 53f9c86d -> cecd6cd0:
# Options::MANIFEST_FORMAT was removed from paimon/defs.h (2e6935f, #263);
# paimon-cpp still parses the "manifest.format" key itself.
# And one build patch: duckdb-paimon installs paimon-cpp and aws-c-* into lib64,
# but CMake on Debian does not search lib64 (FIND_LIBRARY_USE_LIB64_PATHS is off
# when /etc/debian_version exists), so aws-c-cal's configure could not find
# aws-c-common's package config ("Could not find a package configuration file
# provided by aws-c-common", 2026-11-03). DuckDB's CI builds on manylinux (RHEL),
# where it does. lib64 -> lib in its CMakeLists.txt.
WORKDIR /src/duckdb-paimon
RUN fetch() { git init -q "$1" && git -C "$1" fetch -q --depth 1 "$2" "$3" \
              && git -C "$1" checkout -q FETCH_HEAD && test "$(git -C "$1" rev-parse HEAD)" = "$3"; } \
 && fetch . https://github.com/polardb/duckdb-paimon.git "${DUCKDB_PAIMON_COMMIT}" \
 && rm -rf third_party/paimon-cpp \
 && fetch third_party/paimon-cpp https://github.com/apache/paimon-cpp.git "${PAIMON_CPP_COMMIT}" \
 && sed -i 's/paimon::Options::MANIFEST_FORMAT/"manifest.format"/' src/paimon_storage/paimon_catalog.cpp \
 && grep -q '"manifest.format"' src/paimon_storage/paimon_catalog.cpp \
 && sed -i 's#/lib64#/lib#g; s#CMAKE_INSTALL_LIBDIR=lib64#CMAKE_INSTALL_LIBDIR=lib#' CMakeLists.txt \
 && ! grep -n lib64 CMakeLists.txt
# Two steps so the paimon-cpp sub-build (Arrow, Parquet, ORC, Avro, protobuf, TBB,
# aws-c-*; its own ninja at full width) does not run next to DuckDB's unity build.
# OSS off (we only read S3); no shell, no unittests: only the .duckdb_extension.
# On failure, print the ExternalProject logs: CMake itself only says "Stopping
# after outputting logs" and the real error is in <step>-{configure,build}-err.log.
RUN triplet="$(cat /opt/vcpkg-triplet)" \
 && cmake -G Ninja -S /src/duckdb-python/external/duckdb -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DOVERRIDE_GIT_DESCRIBE="${DDB_TAG}" \
        -DDUCKDB_EXTENSION_CONFIGS=/src/duckdb-paimon/extension_config.cmake \
        -DEXTENSION_STATIC_BUILD=1 -DBUILD_SHELL=0 -DBUILD_UNITTESTS=0 -DENABLE_UNITTEST_CPP_TESTS=FALSE \
        -DPAIMON_ENABLE_OSS=OFF \
        -DVCPKG_BUILD=1 \
        -DCMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" \
        -DVCPKG_TARGET_TRIPLET="${triplet}" -DVCPKG_HOST_TRIPLET="${triplet}" \
        -DVCPKG_MANIFEST_DIR=/src/duckdb-paimon \
        -DVCPKG_INSTALLED_DIR=/opt/vcpkg_installed -DVCPKG_MANIFEST_INSTALL=OFF \
 && { cmake --build build --target paimon_cpp_ep -j "${COMPILE_JOBS}" \
      && cmake --build build --target paimon_loadable_extension -j "${COMPILE_JOBS}" \
      || { find build -name '*-err.log' -size +0 -print0 | xargs -0 -r tail -n 40; exit 1; }; } \
 && mkdir -p /out \
 && cp "$(find build -name paimon.duckdb_extension -print -quit)" /out/ \
 && ls -l /out && tail -c 512 /out/paimon.duckdb_extension | strings \
 && rm -rf build

###############################################################################
FROM python:${PYTHON_VERSION}-slim-${DEBIAN_SUITE} AS runtime
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
ARG DUCKDB_TAG=v2.0.0
# The server shells out to `aws sts assume-role-with-web-identity` (refresh_secret).
# awscli 1.x as the pod's `pip install awscli` today (latest on PyPI 2026-11-02).
ARG AWSCLI_SPEC=awscli==1.46.2
# Installed into the image (~/.duckdb/extensions/v2.0.0/<platform>) so the pod
# needs no download at start; the server's INSTALL ... FROM community then finds
# it. Empty to skip (no network at build time).
ARG PREFETCH_COMMUNITY_EXTENSIONS=cache_httpfs
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONUNBUFFERED=1

COPY --from=py-build /out/wheels/ /tmp/wheels/
COPY --from=paimon-build /out/paimon.duckdb_extension /tmp/paimon.duckdb_extension

# paimon is self-built, so unsigned: it goes where INSTALL would put it
# (~/.duckdb/extensions/v2.0.0/<platform>), and LOAD needs a connection with
# allow_unsigned_extensions=true. duckdb_cdc_server.py no longer loads it: the
# harness reads Paimon only as Paimon-as-iceberg (since 2026-11-09).
# AWSCLI_SPEC is pinned (awscli==...); hadolint cannot see through the ARG.
# hadolint ignore=DL3013
RUN pip install --no-cache-dir /tmp/wheels/duckdb-*.whl "${AWSCLI_SPEC}" \
 && rm -rf /tmp/wheels \
 && for ext in ${PREFETCH_COMMUNITY_EXTENSIONS}; do \
        python3 -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL ${ext} FROM community'); c.execute('LOAD ${ext}')"; \
    done \
 && d="$(python3 -c "import duckdb, os; c=duckdb.connect(); print(os.path.expanduser('~/.duckdb/extensions/%s/%s' % (c.execute('SELECT library_version FROM pragma_version()').fetchone()[0], c.execute('SELECT platform FROM pragma_platform()').fetchone()[0])))")" \
 && mkdir -p "$d" && mv /tmp/paimon.duckdb_extension "$d/" && echo "paimon -> $d" \
 && python3 -c "import duckdb; c=duckdb.connect(config={'allow_unsigned_extensions': 'true'}); print(c.execute('SELECT version()').fetchone()); c.execute('LOAD iceberg'); c.execute('LOAD httpfs'); c.execute('LOAD paimon'); print(c.execute(\"SELECT extension_name, loaded, install_mode, extension_version FROM duckdb_extensions() WHERE extension_name IN ('iceberg', 'httpfs', 'cache_httpfs', 'paimon')\").fetchall())"

LABEL org.opencontainers.image.title="duckdb-cdc-server" \
      org.opencontainers.image.description="DuckDB ${DUCKDB_TAG} from source, iceberg/httpfs/aws/avro statically linked, paimon loadable (Python)" \
      org.opencontainers.image.source="https://github.com/duckdb/duckdb/tree/${DUCKDB_TAG}"
WORKDIR /opt/bench
CMD ["python3", "-u", "/opt/bench/duckdb_cdc_server.py"]
