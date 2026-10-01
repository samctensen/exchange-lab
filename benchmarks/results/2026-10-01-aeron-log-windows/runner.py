from pathlib import Path
import csv
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

REPO = Path('/Users/sam/Developer/exchange-lab')
BUILD = Path('/private/tmp/exchange-log-window-11m4e0uw')
JAVA = Path('/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home/bin/java')
OUTPUT = REPO / 'benchmarks/results/2026-10-01-aeron-log-windows'
OUTPUT.mkdir()
(OUTPUT / 'logs').mkdir()
CLASSPATH = (BUILD / 'benchmark-classpath.txt').read_text()
ENV = os.environ.copy()
for key in ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS'):
    ENV.pop(key, None)

def capture(command):
    result = subprocess.run(command, cwd=REPO, env=ENV, text=True, capture_output=True, check=True)
    return (result.stdout + result.stderr).strip()

def stamp():
    return datetime.now(timezone.utc).isoformat()

def exchange_processes():
    rows = capture(['ps', '-axo', 'pid=,comm=,args=']).splitlines()
    found = []
    for row in rows:
        fields = row.strip().split(maxsplit=2)
        if len(fields) == 3 and Path(fields[1]).name == 'java':
            classes = re.findall(r'dev\.sam\.exchange\.[A-Za-z0-9_.]+', fields[2])
            if classes:
                found.append({'pid': int(fields[0]), 'classes': classes})
    return found

validation = {name: 0 for name in ('tests', 'failures', 'errors', 'skipped')}
for result in (BUILD / 'target/surefire-reports').glob('TEST-*.xml'):
    root = ET.parse(result).getroot()
    for name in validation:
        validation[name] += int(root.attrib[name])
if validation != {'tests': 522, 'failures': 0, 'errors': 0, 'skipped': 0}:
    raise RuntimeError(f'Unexpected test results: {validation}')
validation['spotless'] = 'passed'
environment = {
    'started_at': stamp(),
    'base_commit': capture(['git', 'rev-parse', 'HEAD']),
    'build_directory': str(BUILD),
    'java': capture([str(JAVA), '-version']),
    'os': capture(['sw_vers']),
    'cpu': capture(['sysctl', '-n', 'machdep.cpu.brand_string']),
    'logical_cpus': int(capture(['sysctl', '-n', 'hw.logicalcpu'])),
    'physical_cpus': int(capture(['sysctl', '-n', 'hw.physicalcpu'])),
    'memory_bytes': int(capture(['sysctl', '-n', 'hw.memsize'])),
    'load_average_before': os.getloadavg(),
    'existing_exchange_processes': exchange_processes(),
    'jvm_args': ['--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED'],
    'removed_environment_options': ['JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS'],
    'warmup_count': 500,
    'sample_count': 2000,
    'rounds': 5,
    'server_args': ['<fresh-archive-directory>', '--quiet', '--stage-timing=500,2000', '--log-window=<engine_window>'],
    'benchmark_args': ['500', '2000', '<client_window>', 'sleep', '--archive-counters'],
    'engine_client_window_pairs': [[8, 8], [16, 16], [32, 32], [8, 32], [16, 32]],
    'client_idle_strategy': 'SleepingIdleStrategy',
    'timing_variants': ['on'],
    'archive_counters': True,
    'counter_settle_millis': 20,
    'counter_scope': 'One dedicated Archive; deltas after warmup; maximum values are lifetime maxima',
    'timing_sample_counts': {'warmup': 500, 'samples': 2000},
    'cpu_measurement': 'Client benchmark thread and client JVM CPU-time deltas around the measured phase; 100% is one core; server excluded.',
    'engine_idle_strategy': 'BusySpinIdleStrategy',
    'archive_file_sync_level': 2,
    'archive_catalog_sync_level': 2,
    'source_hashes': {},
    'application_validation': validation,
}
for source in (REPO / 'src/main').rglob('*'):
    if source.is_file():
        rel = source.relative_to(REPO)
        if source.read_bytes() != (BUILD / rel).read_bytes():
            raise RuntimeError(f'Stale build source: {rel}')
        environment['source_hashes'][str(rel)] = hashlib.sha256(source.read_bytes()).hexdigest()
environment['engine_jar_sha256'] = hashlib.sha256((BUILD / 'target/exchange-lab-1.0-SNAPSHOT.jar').read_bytes()).hexdigest()
patch = capture(['git', 'diff', 'HEAD', '--', 'src/main']) + '\n'
for filename in ['ArchiveWriteCounters.java']:
    extra = subprocess.run(['git', 'diff', '--no-index', '--', '/dev/null',
                            'src/main/java/dev/sam/exchange/transport/' + filename],
                           cwd=REPO, env=ENV, text=True, capture_output=True)
    if extra.returncode != 1:
        raise RuntimeError('Could not snapshot new source: ' + filename)
    patch += extra.stdout
(OUTPUT / 'implementation.patch').write_text(patch)
(OUTPUT / 'runner.py').write_text(Path(__file__).read_text())
environment['implementation_patch_sha256'] = hashlib.sha256(patch.encode()).hexdigest()
(OUTPUT / 'environment.json').write_text(json.dumps(environment, indent=2) + '\n')
if environment['existing_exchange_processes']:
    raise RuntimeError('Another exchange JVM is running; see environment.json')

rng = random.Random(20261001)
plan = []
for round_number in range(1, 6):
    cases = [(8, 8), (16, 16), (32, 32), (8, 32), (16, 32)]
    rng.shuffle(cases)
    plan.extend((round_number, engine_window, window) for engine_window, window in cases)
rows = []
environment['run_order'] = plan
try:
    for sequence, (round_number, engine_window, window) in enumerate(plan, 1):
        timing = 'on'
        name = f'round-{round_number}-engine-{engine_window}-client-{window}'
        temp = Path(tempfile.mkdtemp(prefix='exchange-windows-measure-', dir='/private/tmp'))
        server_log = OUTPUT / 'logs' / f'{name}.server.log'
        benchmark_log = OUTPUT / 'logs' / f'{name}.benchmark.log'
        common = [str(JAVA), '--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED',
                  f'-Djava.io.tmpdir={temp}', '-cp', CLASSPATH]
        server = None
        started = stamp()
        try:
            with server_log.open('w') as log:
                server = subprocess.Popen(common + ['dev.sam.exchange.transport.AeronEngineServer',
                                                    str(temp / 'archive'), '--quiet', f'--log-window={engine_window}'] +
                                          (['--stage-timing=500,2000'] if timing == 'on' else []),
                                          cwd=BUILD, env=ENV, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 10
            while 'Server ready:' not in server_log.read_text():
                if server.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError(f'{name}: server failed to start; see {server_log}')
                time.sleep(0.01)
            with benchmark_log.open('w') as log:
                result = subprocess.run(common + ['dev.sam.exchange.transport.AeronLatencyBenchmark',
                                                  '500', '2000', str(window), 'sleep', '--archive-counters'],
                                        cwd=BUILD, env=ENV, stdout=log, stderr=subprocess.STDOUT, timeout=45)
            if result.returncode != 0:
                raise RuntimeError(f'{name}: benchmark failed; see {benchmark_log}')
            report = benchmark_log.read_text()
            metrics = {}
            for label in ('p50', 'p99', 'max'):
                match = re.search(rf'(?m)^{label}: ([0-9]+\.[0-9]+) us$', report)
                if not match:
                    raise RuntimeError(f'{name}: missing {label}')
                metrics[label + '_us'] = float(match.group(1))
            metrics['throughput_rps'] = float(re.search(r'(?m)^Throughput: ([0-9]+\.[0-9]+) requests/s$', report).group(1))
            for label, key in [('Client thread CPU', 'client_thread_cpu_pct'), ('Client JVM CPU', 'client_jvm_cpu_pct')]:
                match = re.search(rf'(?m)^{label}: ([0-9]+\.[0-9]+)%$', report)
                if not match:
                    raise RuntimeError(f'{name}: missing CPU counter {label}')
                metrics[key] = float(match.group(1))
            for label, key in [('Archive write bytes (measured phase)', 'archive_write_bytes'),
                               ('Archive write time (measured phase)', 'archive_write_ns'),
                               ('Archive max write time before (lifetime)', 'archive_max_before_ns'),
                               ('Archive max write time after (lifetime)', 'archive_max_after_ns')]:
                match = re.search(rf'(?m)^{re.escape(label)}: ([0-9]+)(?: ns)?$', report)
                if not match:
                    raise RuntimeError(f'{name}: missing counter {label}')
                metrics[key] = int(match.group(1))
            metrics['archive_write_share_pct'] = float(re.search(r'(?m)^Archive write time / client elapsed: ([0-9.]+)%$', report).group(1))
            if metrics['archive_write_bytes'] != 128000 or metrics['archive_write_ns'] <= 0:
                raise RuntimeError(f'{name}: invalid Archive statistics')
            for expected in ('Warmup: 500 requests', 'Samples: 2000 requests', f'Max in flight: {window}',
                             'Attempts per request: 1', 'Client idle strategy: sleep'):
                if expected not in report:
                    raise RuntimeError(f'{name}: missing report field: {expected}')
            if server.poll() is not None:
                raise RuntimeError(f'{name}: server exited during measurement')
            row = {'sequence': sequence, 'round': round_number, 'engine_window': engine_window, 'client_window': window, 'timing': timing,
                   'started_at': started, 'finished_at': stamp(), **metrics}
        finally:
            if server is not None and server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
                    raise RuntimeError(f'{name}: server failed graceful shutdown')
            if server is not None:
                text = server_log.read_text()
                if 'Exception' in text or 'shutdown exceeded' in text or 'Result:' in text:
                    raise RuntimeError(f'{name}: unexpected server output; see {server_log}')
            shutil.rmtree(temp)
        server_report = server_log.read_text()
        if f'Engine log window: {engine_window}' not in server_report:
            raise RuntimeError(f'{name}: incorrect engine window')
        for label, key in [('Log offer', 'log_offer'), ('Recording observation', 'recording_observation'),
                           ('Process and encode', 'process_encode'), ('Reply offer', 'reply_offer'),
                           ('Server total', 'server_total')]:
            for metric in ['mean', 'p50', 'p99', 'max']:
                row[f'{key}_{metric}_us'] = None
            if timing == 'on':
                match = re.search(rf'(?m)^{label}: mean=([0-9.]+) p50=([0-9.]+) p99=([0-9.]+) max=([0-9.]+) us$', server_report)
                if not match:
                    raise RuntimeError(f'{name}: missing stage {label}')
                for metric, value in zip(['mean', 'p50', 'p99', 'max'], match.groups()):
                    row[f'{key}_{metric}_us'] = float(value)
        if timing == 'on':
            if 'Stage timing: 2000/2000 samples, skipped 500/500 completed logged requests' not in server_report:
                raise RuntimeError(f'{name}: incomplete server timing sample')
        elif 'Stage timing:' in server_report:
            raise RuntimeError(f'{name}: timing unexpectedly enabled')
        rows.append(row)
        (OUTPUT / 'runs.json').write_text(json.dumps(rows, indent=2) + '\n')
        with (OUTPUT / 'runs.csv').open('w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=list(row))
            writer.writeheader()
            writer.writerows(rows)
        print(f"{sequence:02d}/25 engine={engine_window} client={window} round={round_number}: "
              f"{metrics['throughput_rps']:.1f} req/s, p50={metrics['p50_us']:.1f} us, "
              f"p99={metrics['p99_us']:.1f} us, thread CPU={metrics['client_thread_cpu_pct']:.1f}%, "
              f"Archive write time={metrics['archive_write_share_pct']:.1f}% of elapsed", flush=True)
finally:
    environment['finished_at'] = stamp()
    environment['load_average_after'] = os.getloadavg()
    environment['exchange_processes_after'] = exchange_processes()
    environment['completed_runs'] = len(rows)
    environment['sources_still_match'] = all(
        hashlib.sha256((REPO / name).read_bytes()).hexdigest() == digest
        for name, digest in environment['source_hashes'].items())
    (OUTPUT / 'environment.json').write_text(json.dumps(environment, indent=2) + '\n')
print(f'Saved results to {OUTPUT}', flush=True)
