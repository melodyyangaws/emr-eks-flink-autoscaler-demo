#!/usr/bin/env python3
"""
CID gzipped-NDJSON generator — the simple path.
===============================================

Writes .json.gz files to S3 with the same field set and value distributions
as the real source extract (cid_9011075d_sample_masked) and the same layout
minus its x=00/ shard level, matching the
Flink datagen mockup (../sql-scripts/common/01-datagen-source.sql). No Flink, no
table format: just gzip files on S3, which is what the upstream producer drops.

It does NOT replace the Flink datagen path — that still feeds the Paimon/Iceberg
write benchmark. This is for when the thing under test is a reader of the raw
landing zone (Flink filesystem source, Spark, Glue, COPY INTO, ...).

TARGETS (per row, means)
    uncompressed  ~4,096 B   (real sample: 3,818 B)
    gzip          ~200 B     (real sample: 195 B, i.e. ~19.5x)

WHY THE RATIO IS REACHABLE. The 20x of the real data is not "JSON compresses
well"; it comes from LOCALITY inside each file, which gzip's 32 KB window
(~8 rows) can only exploit when repeats are close together. Measured on the
sample, the fraction of rows whose value equals one in the previous 8 rows:
    endpointGuid 99.5%   processCmd 80.6%   parentCmd 69.4%
    processHashId 53.7%  objectHashId 4.5%
because each real file is ONE agent upload: one customerId, one coupledTraceId,
1-25 endpoints sorted together, ~60 process names, rows in time order. The
generator reproduces exactly that shape (per-file pools, endpoint runs, sticky
processes, monotonic timestamps). Random per-row hex pads — what the Flink
mockup uses to hit 4 KB — would cap the ratio near 5x and are NOT used here.

THROUGHPUT. One worker process renders + gzips ~60-90 MB/s raw (gzip level 6
dominates). 6 GB/s therefore needs ~80 worker processes cluster-wide; see
run-jsongz-gen.sh for the sizing. Rate is capped PER POD by
POD_ROWS_PER_SECOND, split evenly over this pod's workers, so the cluster rate
is replicas x POD_ROWS_PER_SECOND. It is a LIMIT, only reached if there is
enough CPU.

Environment knobs:
    OUTPUT_URI               s3://bucket/prefix  or  file:///tmp/dir  (required)
    POD_ROWS_PER_SECOND      this pod's cap, default 500000 (≈2 GB/s @4 KB)
    DURATION_SECONDS         run length, default 1800 (30 min) and the maximum;
                             1..1800 only shortens it. 0 / run-until-killed is
                             not supported.
    RUN_DEADLINE_EPOCH       optional hard stop (unix seconds) shared by all
                             pods: writing ends at min(start + DURATION_SECONDS,
                             this). A pod restarted or scaled up after it
                             writes nothing.
    IDLE_AFTER_DONE          1 = after the window, sleep until SIGTERM instead
                             of exiting (a Deployment restarts exited pods,
                             which would start a new window). Default 0.
    WORKERS                  processes in this pod, default = CPU count
    POD_NAME                 RNG seed salt, from the downward API (default
                             HOSTNAME); pods need no index to stay distinct
    ROWS_PER_FILE            file-size scale, default 40000: rows are drawn from
                             0.15x .. 1.25x of it (mean ~0.83x), so ~33K rows, ~7 MB
                             gz per file (~1.3 .. 10 MB). The real extract's files
                             are far smaller (sample: 1,890 rows, ~0.4 MB); 2500
                             reproduces that, at ~13x the file count.
    CID_CUSTOMER_COUNT       distinct customerIds, default 16
    CID_HOST_COUNT           distinct endpoints, default 1024
    GZIP_LEVEL               default 6 (the sample's own ratio needs ~6)
    UPLOAD_THREADS           concurrent PUTs per worker, default 4
"""

import bisect
import io
import json
import multiprocessing as mp
import os
import random
import signal
import sys
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Value ladders — the same cumulative frequencies (out of 10,000) as the CASE
# ladders in 01-datagen-source.sql, so both generators emit one distribution.
# ---------------------------------------------------------------------------
def ladder(pairs):
    cum = [c for c, _ in pairs]
    vals = [v for _, v in pairs]
    return lambda r: vals[bisect.bisect_right(cum, r)]

def ladder_tail(pairs, tail):
    cum = [c for c, _ in pairs]
    vals = [v for _, v in pairs]
    def pick(r, rng):
        i = bisect.bisect_right(cum, r)
        return vals[i] if i < len(vals) else tail(rng)
    return pick

# (eventId, eventSubId, eventCaseId) are correlated, so one draw drives them.
EVENT_ID  = ladder([(5847, '1'), (9178, '3'), (9742, '2'), (9935, '14'), (9967, '11'),
                    (9989, '5'), (9995, '4'), (9999, '8'), (10000, '19')])
EVENT_SUB = ladder([(5611, 2), (7334, 204), (8938, 203), (9502, 101), (9695, 1101),
                    (9834, 4), (9931, 3), (9963, 901), (9985, 402), (9991, 301),
                    (9995, 703), (9999, 202), (10000, 1704)])
EVENT_CASE = ladder([(5847, 7), (9178, 3), (9742, 5), (9935, 14), (9967, 2),
                     (9989, 8), (9995, 4), (9999, 9), (10000, 19)])

# (osName, osVer, osDescription, osType, pplat) all follow one r_os draw.
OS_VER = ladder([(3293, 'Amazon Linux 2'), (5068, 'Ubuntu 24.04'), (6392, 'CentOS 7'),
                 (7566, '"Rocky Linux 9.6 (Blue Onyx)"'), (8005, 'Ubuntu 22.04'),
                 (8343, 'Ubuntu Linux 16'), (8615, 'Ubuntu Linux 20'), (8836, 'Debian 9'),
                 (9009, 'CentOS 6'), (9172, 'Amazon Linux 2023'), (9692, '10.0.14393'),
                 (9856, '10.0.20348'), (10000, '10.0.26100')])
OS_DESC = ladder([(1379, 'Ubuntu 24.04 (64 bit) (6.8.0-1067-azure)'),
                  (2699, '"Rocky Linux 9.6 (Blue Onyx)" (64 bit) (5.14.0-570.17.1.el9_6.x86_64)'),
                  (3718, 'Amazon Linux 2 (64 bit) (6.12.77-99.140.amzn2023.x86_64)'),
                  (4226, 'Ubuntu 24.04 (64 bit) (6.8.0-1065-azure)'),
                  (4604, 'CentOS 7 (64 bit) (3.10.0-1127.19.1.el7.x86_64)'),
                  (4913, 'Amazon Linux 2 (64 bit) (6.12.68-92.122.amzn2023.x86_64)'),
                  (5169, 'CentOS 7 (64 bit) (3.10.0-1160.66.1.el7.x86_64)'),
                  (5417, 'Debian 9 (64 bit) (4.9.0-9-amd64)'),
                  (9172, 'CentOS 7 (64 bit) (3.10.0-1160.119.1.el7.x86_64)'),
                  (10000, 'Microsoft Windows Server 2016 (64 bit) build 14393')])
OS_TYPE = ladder([(9172, ''), (9981, '8'), (9992, '4'), (10000, '7')])
TIMEZONE = ladder([(9555, 'UTC+00:00'), (9781, 'UTC+09:00'), (9891, 'UTC+08:00'),
                   (9944, 'UTC-08:00'), (9963, 'UTC+05:30'), (10000, 'UTC-07:00')])
CLOUD = ladder([(6158, 'AWS'), (8812, 'Azure'), (9983, ''), (10000, 'GCP')])
REGION = ladder([(2399, 'Europe (Frankfurt)'), (4066, 'US West (Oregon)'),
                 (5504, 'Asia Pacific (Tokyo)'), (6883, 'US East (N. Virginia)'),
                 (7425, 'Asia Pacific (Singapore)'), (7892, 'Asia Pacific (Mumbai)'),
                 (8321, 'Europe (Ireland)'), (8686, 'Africa (Cape Town)'),
                 (8960, 'US East (Ohio)'), (10000, 'Asia Pacific (Sydney)')])
RISK = ladder([(8369, 'info'), (9826, 'low'), (10000, 'medium')])
PVER = ladder_tail([(2193, '1.0.422'), (3589, '1.0.413'), (4837, '1.0.428'), (5269, '1.0.402'),
                    (5653, '1.0.412'), (6022, '1.0.376'), (6340, '1.0.316'), (6654, '1.0.416')],
                   lambda rng: '1.0.%d' % (300 + rng.randrange(35)))
PROC_NAME = ladder_tail([(1735, '/usr/bin/bash'), (2310, '/usr/bin/node-problem-detector'),
                         (2727, '/var/lang/bin/python3.12'), (3080, '/var/lang/bin/python3.10'),
                         (3432, '/usr/bin/python2.7'),
                         (3747, 'c:\\windows\\system32\\windowspowershell\\v1.0\\powershell.exe'),
                         (4047, '/runc'), (4334, '/usr/sbin/runc'), (4606, '/usr/bin/python3.9'),
                         (4783, '/usr/sbin/sshd'), (4948, '/usr/bin/perl'), (5112, '/hyperkube')],
                        lambda rng: '/usr/bin/%06x' % rng.getrandbits(24))
OBJ_PATH = ladder_tail([(788, '/usr/bin/bash'), (1286, '/usr/bin/node-problem-detector'),
                        (1688, '/runc'), (2004, '/usr/sbin/runc'),
                        (2224, '/usr/sbin/xtables-nft-multi'), (2439, '/usr/bin/cat'),
                        (2652, '/usr/bin/dash'), (2861, '/usr/sbin/sshd'),
                        (3069, '/usr/bin/grep'), (3265, '/usr/bin/timeout')],
                       lambda rng: '/usr/bin/%06x' % rng.getrandbits(24))
PROC_USER = ladder_tail([(7264, 'root'), (7598, 'SYSTEM'), (7730, 'www-data')],
                        lambda rng: 'user-%06x' % rng.getrandbits(24))
OBJ_USER = ladder_tail([(8665, 'root'), (8863, 'www-data'), (8951, 'SYSTEM')],
                       lambda rng: 'user-%06x' % rng.getrandbits(24))
DPT = ladder_tail([(2208, 443), (3340, 53), (3596, 80), (3743, 8080), (3878, 9),
                   (4006, 5666), (4096, 123), (4176, 1433)],
                  lambda rng: 1024 + rng.randrange(64000))
TAG_SETS = ladder([(1311, ['MITRE.T1057']),
                   (2509, ['MITRE.T1124', 'XSAE.F2451']),
                   (3145, ['MITRE.T1112', 'MITRE.T1543.003', 'XSAE.F17784', 'MITRE.T1036.004']),
                   (3701, ['MITRE.T1560.001', 'XSAE.F2307', 'MITRE.T1560']),
                   (4196, ['MITRE.T1082', 'XSAE.F13030']),
                   (10000, ['XSAE.F12793', 'MITRE.T1059.001'])])

# Vocabulary the command lines are built from. Real command lines are made of
# a small vocabulary (paths, flags, container ids that repeat per host), which
# is why they compress; drawing from a vocabulary instead of a random string is
# what keeps the 4 KB row at ~200 B gzipped.
CMD_WORDS = ['/var/lib/docker/overlay2/', '/diff', '--config=/etc/kubernetes/', 'kubelet.conf',
             '--node-ip=', '-c', 'nice', '-n', '19', 'du', '-s', '/bin/sh', 'bash', 'cd', '/data/',
             '&&', 'python3', '-m', 'pip', 'install', '--no-cache-dir', '/opt/app/bin/',
             'java', '-Xmx2g', '-jar', '/usr/share/', 'grep', '-E', 'timeout', '30', 'cat',
             '/proc/', '/status', '--log-level=info', '--cgroup-root=/', 'runc', '--root',
             '/run/containerd/runc/k8s.io', 'exec', '--process', '/tmp/runc-process',
             '--detach', '--pid-file', 'ps', '-o', '%cpu', '%mem', 'iptables', '-w', '-t', 'nat']

def hexs(rng, nbits):
    return '%0*x' % (nbits // 4, rng.getrandbits(nbits))

def guid(rng, ver='4'):
    h = hexs(rng, 128)
    return '%s-%s-%s%s-%s-%s' % (h[:8], h[8:12], ver, h[13:16], h[16:20], h[20:32])

def i64(rng):
    return str(rng.getrandbits(64) - (1 << 63))

def frag(d):
    """A pre-serialised JSON object body (no braces) — rendered once per pool
    entry, then concatenated per row. This is where the speed comes from."""
    return json.dumps(d, separators=(',', ':'))[1:-1]

# Command-line widths carry the 4 KB row calibration, the same lever the Flink
# mockup uses (sample means: processCmd 322 B, processCmdNormalized 236 B,
# parentCmd 137 B, parentCmdNormalized 105 B, objectCmd 105 B). They are wider
# here than there because this generator's other fields are leaner; the widths
# are cheap in gzip terms since a command repeats across its burst.
PROCESS_CMD_LEN = 900
PROCESS_CMD_NORM_LEN = 600
PARENT_CMD_LEN = 360
PARENT_CMD_NORM_LEN = 200
OBJECT_CMD_LEN = 260

def cmdline(rng, head, target_len, ids):
    parts = [head]
    n = len(head)
    while n < target_len:
        w = CMD_WORDS[rng.randrange(len(CMD_WORDS))]
        if rng.random() < 0.15:          # container / overlay ids, shared per file
            w += ids[rng.randrange(len(ids))]
        parts.append(w)
        n += len(w) + 1
    return ' '.join(parts)

# ---------------------------------------------------------------------------
# Per-file pools. One file = one agent upload: one customer, one
# coupledTraceId, a handful of endpoints, a few dozen processes.
# ---------------------------------------------------------------------------
class FileSpec:
    def __init__(self, rng, customer_ids, host_count):
        self.customer_id = customer_ids[rng.randrange(len(customer_ids))]
        self.trace = '00' + hexs(rng, 120)
        r_ev = rng.randrange(10000)
        self.product = 'sds' if r_ev < 9999 else 'ddr'
        tenant = guid(rng)
        # The container/overlay ids a host's command lines refer to. Shared by
        # every command in the file, as on a real host.
        self.ids = [hexs(rng, 64) for _ in range(8)]
        self.binaries = {}
        self.envelope = frag({
            'customerId': self.customer_id, 'productCode': self.product,
            'pname': 'Server & Workload Protection' if self.product == 'sds'
                     else 'Data Detection and Response',
            'plang': 1, 'version': '1.0', 'eventSourceType': 1,
            'coupledTraceId': self.trace, 'partitionKey': tenant, 'tenantGuid': tenant,
            'mgmtInstanceId': tenant, 'fastTrack': False, 'dataReductionRuntime': 'java',
            'filterMatchingRuntime': 'java', 'engType': '', 'productAgent': '', 'packagePath': ''})
        self.endpoints = [self._endpoint(rng, host_count) for _ in range(1 + rng.randrange(24))]
        self.procs = [self._proc(rng) for _ in range(20 + rng.randrange(80))]
        self.parents = [self._parent(rng, p) for p in self.procs[:max(4, len(self.procs) // 3)]]
        for p in self.procs:
            p['parent'] %= len(self.parents)
        self.objects = [self._object(rng) for _ in range(30 + rng.randrange(90))]

    @staticmethod
    def _endpoint(rng, host_count):
        # Endpoint identity is a pure function of the host id, so the same host
        # renders identically in every file — that is CID_HOST_COUNT's meaning.
        hrng = random.Random(rng.randrange(host_count))
        h = hexs(hrng, 128)
        r_os, r_cloud, g_cloud = (hrng.randrange(10000) for _ in range(3))
        linux = r_os < 9172
        d = {'endpointGuid': guid(hrng), 'groupId': guid(hrng),
             'endpointHostName': 'host-' + h[:8],
             'endpointIp': ['10.%d.%d.%d' % (hrng.randrange(256), hrng.randrange(256),
                                             hrng.randrange(256))],
             'endpointMacAddress': [':'.join(h[i:i + 2] for i in range(0, 12, 2)),
                                    ':'.join(h[i:i + 2] for i in range(12, 24, 2))],
             'osName': 'Linux' if linux else 'Windows', 'osVer': OS_VER(r_os),
             'osDescription': OS_DESC(r_os), 'osType': OS_TYPE(r_os),
             'pplat': 9217 if linux else 5889, 'pver': PVER(hrng.randrange(10000), hrng),
             'timezone': TIMEZONE(hrng.randrange(10000)), 'cloudProvider': CLOUD(r_cloud),
             'protectionManager': 'PM-' + h[24:30]}
        if hrng.randrange(100) < 15:    # real: key absent on ~15% of rows
            del d['cloudProvider']
        if g_cloud < 5060:
            d['regionId'] = REGION(hrng.randrange(10000))
            d['vpcId'] = 'vpc-' + h[4:12]
        if g_cloud < 7250:
            d['instanceId'] = 'i-' + h[:17]
        return {'body': frag(d), 'guid': d['endpointGuid'], 'ip': d['endpointIp'][0],
                'windows': not linux}

    def binary(self, rng, path):
        """File hashes are a property of the BINARY, not of the event: every
        process or object at one path shares them, which is why the sample's
        processFileHashSha256 repeats in-window 89% of the time."""
        b = self.binaries.get(path)
        if b is None:
            b = self.binaries[path] = (i64(rng), hexs(rng, 256), hexs(rng, 160), hexs(rng, 128))
        return b

    def _proc(self, rng):
        name = PROC_NAME(rng.randrange(10000), rng)
        g = rng.randrange(10000)
        fid, sha256, sha1, md5 = self.binary(rng, name)
        d = {'processName': name, 'processFilePath': name,
             'processHashId': i64(rng), 'processFileHashId': fid}
        if g < 9120:
            d['processUser'] = PROC_USER(rng.randrange(10000), rng)
        if g < 830:
            d['processUserDomain'] = 'ad-' + hexs(rng, 16)
        if g < 9630:
            d['processFileHashSha256'] = sha256
            d['processFileHashSha1'] = sha1
            d['processFileHashMd5'] = md5
        if g < 9250:
            d['processCmd'] = cmdline(rng, name, PROCESS_CMD_LEN, self.ids)
        if g < 2010:
            d['processCmdNormalized'] = cmdline(rng, name, PROCESS_CMD_NORM_LEN, self.ids)
        # Gated-off keys are ABSENT, not [] — the real data omits them, and a
        # reader testing key existence must see the same thing.
        if g < 590:
            d['processSigner'] = ['Microsoft Windows Publisher']
            d['processSignerValid'] = [True]
            d['processSignerFlagsLibValid'] = [False]
            d['processSignerFlagsAdhoc'] = [False]
            d['processSignerFlagsRuntime'] = [False]
        if g < 8720:
            d['logonUser'] = [d.get('processUser', 'root')]
            d['userDomain'] = ['']
        return {'body': frag(d), 'hash': d['processHashId'], 'name': name,
                'user': d.get('processUser', 'root'), 'parent': rng.randrange(1 << 30)}

    def _parent(self, rng, p):
        fid, sha256, sha1, md5 = self.binary(rng, p['name'])
        d = {'parentHashId': p['hash'], 'parentFileHashId': fid,
             'parentName': p['name'], 'parentFilePath': p['name'], 'parentUser': p['user'],
             'parentFileHashSha256': sha256, 'parentFileHashSha1': sha1,
             'parentFileHashMd5': md5}
        if rng.randrange(10000) < 3520 * 10000 // 3830:
            d['parentCmd'] = cmdline(rng, '/bin/sh -c bash -c cd /data/ &&', PARENT_CMD_LEN,
                                     self.ids)
        if rng.randrange(10000) < 880 * 10000 // 3830:
            d['parentCmdNormalized'] = cmdline(rng, '/bin/sh -c', PARENT_CMD_NORM_LEN, self.ids)
        return frag(d)

    def _object(self, rng):
        # An object is a FILE, so its path and content hashes are stable; only
        # objectHashId / objectPid / times are per event and rendered per row.
        path = OBJ_PATH(rng.randrange(10000), rng)
        fid, sha256, sha1, md5 = self.binary(rng, path)
        d = {'objectName': path, 'objectFilePath': path, 'objectFileHashId': fid,
             'objectFileHashSha256': sha256, 'objectFileHashSha1': sha1,
             'objectFileHashMd5': md5, 'objectRunAsLocalAccount': False,
             'objectUser': OBJ_USER(rng.randrange(10000), rng)}
        if rng.randrange(10000) < 1560:
            d['objectSubTrueType'] = 1
        return {'body': frag(d),
                'cmd': json.dumps(cmdline(rng, 'nice -n 19 du -s /var/lib/docker/overlay2/',
                                       OBJECT_CMD_LEN, self.ids))}

# ---------------------------------------------------------------------------
# Row rendering. Pool fragments are concatenated; only the per-row fields
# (uuid, hash ids, timestamps, pids, ports, object block) are formatted here.
# ---------------------------------------------------------------------------
# Locality knobs, tuned so the gzip ratio lands on the sample's (~20x):
BURST_MEAN = 10        # rows per process burst
REVISIT = 0.7          # P(a burst re-uses one of the last 4 processes)
OBJECT_SWITCH = 0.08   # P(a row touches a new object rather than a recent one)

def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.') \
        + '%03dZ' % (ms % 1000)

def render_rows(rng, spec, n_rows, start_ms):
    # A generator, not a list: a file is tens of thousands of 4 KB rows, and
    # gzip_rows() compresses them as they come instead of holding the raw file.
    ms = start_ms
    ep = spec.endpoints[0]
    burst = 0
    objs = spec.objects[:1]
    recent = spec.procs[:1]
    for _ in range(n_rows):
        # LOCALITY, the thing gzip feeds on. Rows are grouped by endpoint, and a
        # process emits a BURST of events that share its pid, parent, network
        # peer and a few recently touched objects. In-window (previous 8 rows)
        # repeat rates this reproduces, vs the sample: endpointGuid 99.5%,
        # processFileHashSha256 89%, parentFileHashSha256 81%,
        # objectFileHashSha256 64%, processPid 59%, dst 57%.
        if rng.random() < 0.01:
            ep = spec.endpoints[rng.randrange(len(spec.endpoints))]
        if burst <= 0:
            burst = 1 + int(rng.expovariate(1 / BURST_MEAN))
            # Most bursts come from a process seen moments ago (the same daemon
            # firing again); the rest introduce a new one from the file's pool.
            if rng.random() < REVISIT:
                proc = recent[rng.randrange(len(recent))]
            else:
                proc = spec.procs[rng.randrange(len(spec.procs))]
                recent = (recent + [proc])[-4:]
            parent = spec.parents[proc['parent']]
            pid = 1 + rng.randrange(4194304)
            dst = '10.%d.%d.%d' % (rng.randrange(256), rng.randrange(256), rng.randrange(256))
            src_hash = i64(rng)
            spt = 53 if rng.randrange(10000) < 4659 else 1024 + rng.randrange(64000)
            dpt = DPT(rng.randrange(10000), rng)
        burst -= 1
        if rng.random() < OBJECT_SWITCH:
            objs = (objs + [spec.objects[rng.randrange(len(spec.objects))]])[-3:]
        obj = objs[rng.randrange(len(objs))]
        ms += rng.randrange(3)                       # monotonic, many rows per ms
        ts, ts_dt = str(ms), iso(ms)
        recv = ms + 274000
        r_ev = rng.randrange(10000)
        g_parent, g_obj, g_net, g_rare = (rng.randrange(10000) for _ in range(4))

        row = ['{', spec.envelope, ',', ep['body'], ',', proc['body'],
               ',"uuid":"%08x-%04x-7%03x-%04x-%012x"' % (       # UUIDv7, as the source
                   ms >> 16, ms & 0xffff, rng.getrandbits(12),
                   0x8000 | rng.getrandbits(14), rng.getrandbits(48)),
               ',"eventId":"%s","eventSubId":%d,"eventCaseId":%d,"eventHashId":"%s"' % (
                   EVENT_ID(r_ev), EVENT_SUB(r_ev), EVENT_CASE(r_ev), i64(rng)),
               ',"eventTime":"%s","eventTimeDT":"%s","firstSeen":"%s","firstSeenDT":"%s"'
               ',"lastSeen":"%s","lastSeenDT":"%s","receivedTime":"%d","logReceivedTime":"%d"'
               ',"logReceivedTimeDT":"%s","packageTraceId":"%s-%s"' % (
                   ts, ts_dt, ts, ts_dt, ts, ts_dt, recv, recv, iso(recv), ep['guid'],
                   ts_dt[:19].replace('T', '_').replace(':', '-') + '-' + ts[-3:]),
               ',"processPid":%d,"processLaunchTime":"%d","processLaunchTimeDT":"%s"' % (
                   pid, ms - 584, ts_dt)]
        if g_parent < 3830:
            row.append(',' + parent)
            row.append(',"parentPid":%d,"parentLaunchTime":"%d","parentLaunchTimeDT":"%s"' % (
                pid, ms - 1584, ts_dt))
        if g_obj < 9860:
            row.append(',"objectHashId":"%s"' % i64(rng))
        if g_obj < 5200:
            row.append(',' + obj['body'])
            row.append(',"objectPid":%d,"objectLaunchTime":"%d","objectLaunchTimeDT":"%s"' % (
                pid, ms - 584, ts_dt))
        if g_obj < 4520:
            row.append(',"objectFirstSeen":"%s","objectFirstSeenDT":"%s",'
                       '"objectLastSeen":"%s","objectLastSeenDT":"%s"' % (ts, ts_dt, ts, ts_dt))
        if g_obj < 4930:
            row.append(',"objectCmd":' + obj['cmd'])
        if g_obj < 670:
            row.append(',"objectCmdNormalized":"ps -o %%cpu %%mem %d"' % pid)
        if g_net < 3810:
            row.append(',"src":"%s","dst":"%s","spt":%d,"srcHashId":"%s","srcFirstSeen":"%s",'
                       '"srcFirstSeenDT":"%s","srcLastSeen":"%s","srcLastSeenDT":"%s"' % (
                           ep['ip'], dst, spt, src_hash, ts, ts_dt, ts, ts_dt))
            if g_net < 3710:
                row.append(',"dpt":%d' % dpt)
            if g_net < 3690:
                row.append(',"proto":%d' % (2 if rng.randrange(10000) < 8227 else 1))
            if g_net < 1150:
                row.append(',"dstIsp":["Amazon.com"],"dstCountry":["US"],"dstCity":["Boardman"]')
        if g_rare < 280:
            row.append(',"tags":' + json.dumps(TAG_SETS(rng.randrange(10000))))
        if g_rare < 230:
            risk = RISK(rng.randrange(10000))
            row.append(',"filterRiskLevel":"%s","tmFilterRiskLevel":"%s",'
                       '"bitwiseFilterRiskLevel":1' % (risk, risk))
        if g_rare < 190:
            row.append(',"objectBmData":"{\\"provider\\":\\"ORCA\\",'
                       '\\"schema_version\\":1,\\"severity\\":\\"low\\"}"')
        if g_rare < 130:
            row.append(',"platformAssetTags":"{\\"Asset group\\": [\\"DC+Cloud\\", '
                       '\\"TrendIQ\\"]}","customAssetTags":"{}"')
        row.append('}\n')
        yield ''.join(row)

GZIP_CHUNK_ROWS = 1000   # ~4 MB of raw text per compressor call

def gzip_rows(rows, level):
    """Stream rows through one gzip member; returns (gz bytes, raw byte count).

    Same deflate stream as gzip.compress(raw, level, mtime=0); only the header's
    OS byte differs (zlib's, not 0xff). Raw memory is one chunk, not the file:
    at ~4 KB/row a 40,000-row file is ~160 MB raw, times 60 workers a pod.
    """
    z = zlib.compressobj(level, zlib.DEFLATED, 31)
    parts, chunk, raw_len = [], [], 0
    for r in rows:
        chunk.append(r)
        if len(chunk) == GZIP_CHUNK_ROWS:
            b = ''.join(chunk).encode()
            raw_len += len(b)
            parts.append(z.compress(b))
            chunk = []
    if chunk:
        b = ''.join(chunk).encode()
        raw_len += len(b)
        parts.append(z.compress(b))
    parts.append(z.flush())
    return b''.join(parts), raw_len

# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------
class Sink:
    def __init__(self, uri, threads):
        self.uri = uri.rstrip('/')
        self.pool = ThreadPoolExecutor(max_workers=threads)
        self.pending = []
        if self.uri.startswith('s3://'):
            import boto3
            from botocore.config import Config
            bucket, _, self.prefix = self.uri[5:].partition('/')
            self.bucket = bucket
            self.s3 = boto3.client('s3', config=Config(
                max_pool_connections=threads * 2,
                retries={'max_attempts': 10, 'mode': 'adaptive'}))
        elif self.uri.startswith('file://'):
            self.root = self.uri[7:]
        else:
            raise SystemExit('OUTPUT_URI must be s3://... or file://...')

    def _put(self, key, body):
        if self.uri.startswith('s3://'):
            k = '%s/%s' % (self.prefix, key) if self.prefix else key
            # No ContentEncoding: 'gzip' there makes HTTP clients inflate the body
            # transparently, and Flink, which inflates by the .gz extension,
            # would then see plain text. The object IS a .gz file, not an encoding.
            self.s3.put_object(Bucket=self.bucket, Key=k, Body=body,
                               ContentType='application/gzip')
        else:
            path = os.path.join(self.root, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'wb') as f:
                f.write(body)

    def put(self, key, body):
        # Bounded in-flight uploads: if S3 falls behind, the generator waits
        # rather than buffering without limit.
        self.pending = [f for f in self.pending if not f.done()]
        while len(self.pending) >= self.pool._max_workers * 2:
            self.pending[0].result()
            self.pending = [f for f in self.pending if not f.done()]
        self.pending.append(self.pool.submit(self._put, key, body))

    def close(self):
        for f in self.pending:
            f.result()
        self.pool.shutdown()

# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------
def worker(idx, cfg, counters, stop):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # str seeds are hashed (SHA-512), so pod name + worker + ns start time give
    # every worker its own stream, and its own file names, without a pod index.
    rng = random.Random('%s/%d/%d' % (cfg['pod_name'], idx, time.time_ns()))
    crng = random.Random(42)        # customerIds identical across pods and workers
    customers = [guid(crng) for _ in range(cfg['customers'])]
    sink = Sink(cfg['output'], cfg['upload_threads'])
    rows_per_sec = cfg['rate'] / cfg['workers']
    t0 = time.time()
    rows_done = 0
    seq = 0
    while not stop.is_set():
        if time.time() - t0 >= cfg['duration']:
            break
        spec = FileSpec(rng, customers, cfg['hosts'])
        # Spread file sizes with the sample's shape (its 332 .. 3,000 rows is
        # 0.15x .. 1.25x of a 2,500 mean); the mean itself is ROWS_PER_FILE.
        n = max(100, int(rng.triangular(0.15, 1.25, 1.1) * cfg['rows_per_file']))
        now_ms = int(time.time() * 1000)
        body, raw_len = gzip_rows(render_rows(rng, spec, n, now_ms), cfg['gzip_level'])
        hour = datetime.fromtimestamp(now_ms / 1000, timezone.utc)
        seq += 1
        key = ('year=%04d/month=%02d/day=%02d/hour=%02d/customerId=%s/productCode=%s/'
               '%s_%03d_new.json.gz' % (hour.year, hour.month, hour.day, hour.hour,
                                        spec.customer_id, spec.product, spec.trace[:32],
                                        seq % 1000))
        sink.put(key, body)
        rows_done += n
        with counters.get_lock():
            counters[0] += n
            counters[1] += raw_len
            counters[2] += len(body)
            counters[3] += 1
        # Rate cap: sleep off any lead over the per-worker share.
        ahead = rows_done / rows_per_sec - (time.time() - t0)
        if ahead > 0:
            stop.wait(ahead)
    sink.close()

MAX_DURATION_SECONDS = 1800     # fixed 30-min run; DURATION_SECONDS can only shorten it

def main():
    output = os.environ.get('OUTPUT_URI') or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not output:
        raise SystemExit('usage: OUTPUT_URI=s3://bucket/prefix cid_jsongz_gen.py')
    raw_duration = os.environ.get('DURATION_SECONDS', str(MAX_DURATION_SECONDS)).strip()
    if not raw_duration.isdigit() or not 1 <= int(raw_duration) <= MAX_DURATION_SECONDS:
        raise SystemExit('DURATION_SECONDS=%r rejected: the run is fixed at %d s (30 min); '
                         'only a shorter whole-second override 1..%d is allowed'
                         % (raw_duration, MAX_DURATION_SECONDS, MAX_DURATION_SECONDS))
    workers = int(os.environ.get('WORKERS', os.cpu_count()))
    duration = float(raw_duration)
    deadline = os.environ.get('RUN_DEADLINE_EPOCH', '').strip()
    if deadline:
        duration = min(duration, float(deadline) - time.time())
    idle_after = os.environ.get('IDLE_AFTER_DONE', '0') == '1'
    cfg = {
        'output': output,
        'rate': float(os.environ.get('POD_ROWS_PER_SECOND', '500000')),
        'duration': duration,
        'pod_name': os.environ.get('POD_NAME') or os.environ.get('HOSTNAME') or str(os.getpid()),
        'workers': workers,
        'rows_per_file': int(os.environ.get('ROWS_PER_FILE', '40000')),
        'customers': int(os.environ.get('CID_CUSTOMER_COUNT', '16')),
        'hosts': int(os.environ.get('CID_HOST_COUNT', '1024')),
        'gzip_level': int(os.environ.get('GZIP_LEVEL', '6')),
        'upload_threads': int(os.environ.get('UPLOAD_THREADS', '4')),
    }
    if duration <= 0:
        print('IDLE %s: RUN_DEADLINE_EPOCH has passed, writing nothing' % cfg['pod_name'], flush=True)
        idle_forever(idle_after)
        return
    print('cid-jsongz-gen pod %s: %d workers, pod cap %.0f rows/s (%.0f/worker), %.0f s -> %s'
          % (cfg['pod_name'], workers, cfg['rate'], cfg['rate'] / workers, duration, output),
          flush=True)

    counters = mp.Array('d', 4)          # rows, raw bytes, gz bytes, files
    stop = mp.Event()
    procs = [mp.Process(target=worker, args=(i, cfg, counters, stop)) for i in range(workers)]
    for p in procs:
        p.start()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    t0 = last_t = time.time()
    last = [0.0] * 4
    while any(p.is_alive() for p in procs):
        time.sleep(10)
        now = time.time()
        with counters.get_lock():
            cur = list(counters)
        dt = now - last_t
        d = [c - l for c, l in zip(cur, last)]
        if d[0]:
            print('[%6.0fs] %9.0f rows/s  %7.1f MB/s raw  %6.1f MB/s gz  %6.0f files/s  '
                  'row %4.0f B  gz %3.0f B  ratio %.1fx' % (
                      now - t0, d[0] / dt, d[1] / dt / 1e6, d[2] / dt / 1e6, d[3] / dt,
                      d[1] / d[0], d[2] / d[0], d[1] / d[2]), flush=True)
        last, last_t = cur, now
    for p in procs:
        p.join()
    if cur[0]:
        print('DONE %.0f rows, %.2f GB raw, %.2f GB gz, %.0f files, mean row %.0f B / gz %.0f B'
              % (cur[0], cur[1] / 1e9, cur[2] / 1e9, cur[3], cur[1] / cur[0], cur[2] / cur[0]),
              flush=True)
    idle_forever(idle_after)

def idle_forever(enabled):
    """Deployment mode: hold the pod after the window so it is not restarted."""
    if not enabled:
        return
    print('IDLE window closed; sleeping until stopped (scale to 0 / delete)', flush=True)
    # Nothing left to flush: exit at once. An explicit handler, not SIG_DFL,
    # because the container execs python as PID 1, which ignores default
    # SIGTERM and would sit out the whole termination grace period.
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    signal.signal(signal.SIGINT, lambda *_: os._exit(0))
    while True:
        time.sleep(3600)

if __name__ == '__main__':
    main()
