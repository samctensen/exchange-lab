from pathlib import Path
from datetime import datetime, timezone
import csv, hashlib, json, os, random, re, shutil, subprocess, tempfile, time
import xml.etree.ElementTree as ET

REPO = Path('/Users/sam/Developer/exchange-lab')
BUILD = Path('/private/tmp/exchange-grpc-benchmark-ycehkf44')
JAVA = Path('/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home/bin/java')
OUTPUT = REPO / 'benchmarks/results/2026-10-01-grpc-windows'
ENV = os.environ.copy()
for key in ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS'):
    ENV.pop(key, None)

def capture(command):
    r = subprocess.run(command, cwd=REPO, env=ENV, text=True, capture_output=True, check=True)
    return (r.stdout + r.stderr).strip()

def stamp(): return datetime.now(timezone.utc).isoformat()

def exchange_processes():
    found = []
    for row in capture(['ps', '-axo', 'pid=,comm=,args=']).splitlines():
        fields = row.strip().split(maxsplit=2)
        if len(fields) == 3 and Path(fields[1]).name == 'java':
            classes = re.findall(r'dev\.sam\.exchange\.[A-Za-z0-9_.]+', fields[2])
            if classes: found.append({'pid': int(fields[0]), 'classes': classes})
    return found

def await_text(process, log, marker):
    deadline = time.monotonic() + 15
    while marker not in log.read_text():
        if process.poll() is not None or time.monotonic() >= deadline:
            raise RuntimeError(f'Failed startup: {log}')
        time.sleep(.01)

def stop(process, name):
    if process is not None and process.poll() is None:
        process.terminate()
        try: process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(timeout=5)
            raise RuntimeError(f'Failed graceful shutdown: {name}')

def metric(text, label, suffix='', integer=False):
    m = re.search(rf'(?m)^{re.escape(label)}: ([0-9]+(?:\.[0-9]+)?){re.escape(suffix)}$', text)
    if not m: raise RuntimeError(f'Missing metric {label}')
    return int(m.group(1)) if integer else float(m.group(1))

validation = {k: 0 for k in ['tests', 'failures', 'errors', 'skipped']}
reports = list((BUILD/'target/surefire-reports').glob('TEST-*.xml'))
for p in reports:
    root = ET.parse(p).getroot()
    for k in validation: validation[k] += int(root.attrib[k])
if validation != {'tests': 552, 'failures': 0, 'errors': 0, 'skipped': 0}:
    raise RuntimeError(f'Unexpected verification: {validation}')
classpath = next(p.attrib['value'] for p in ET.parse(reports[0]).getroot().findall('./properties/property')
                 if p.attrib['name'] == 'java.class.path')
existing = exchange_processes()
if existing: raise RuntimeError(f'Other exchange processes: {existing}')
OUTPUT.mkdir()
(OUTPUT/'logs').mkdir()
(OUTPUT/'runner.py').write_text(Path(__file__).read_text())
environment = {
    'started_at': stamp(), 'base_commit': capture(['git','rev-parse','HEAD']),
    'build_directory': str(BUILD), 'java': capture([str(JAVA),'-version']), 'os': capture(['sw_vers']),
    'cpu': capture(['sysctl','-n','machdep.cpu.brand_string']),
    'logical_cpus': int(capture(['sysctl','-n','hw.logicalcpu'])),
    'physical_cpus': int(capture(['sysctl','-n','hw.physicalcpu'])),
    'memory_bytes': int(capture(['sysctl','-n','hw.memsize'])),
    'existing_exchange_processes': existing, 'load_average_before': os.getloadavg(),
    'application_validation': {**validation, 'spotless':'passed'},
    'archive_file_sync_level': 2, 'archive_catalog_sync_level': 2,
    'engine_idle_strategy':'BusySpinIdleStrategy', 'gateway_idle_strategy':'SleepingIdleStrategy',
    'gateway_queue_capacity':128, 'gateway_retry_config':{'timeout_seconds':5,'max_attempts':3},
    'benchmark_application_rpc_attempts':1, 'benchmark_channel_retries':'disabled',
    'sample_count':2000, 'baseline_warmup_count':500, 'probe_warmup_count':0,
    'baseline_rpc_deadline_ms':5000, 'deadline_probe_rpc_deadline_ms':10,
    'diagnostic_scope':'Gateway counters include warmup and shutdown drain; engine stage timing skips warmup only in baseline runs.',
    'probe_caveat':'Pressure and deadline probes start cold with no warmup; they describe failure behavior, not steady-state latency.',
    'grpc_transport':'plaintext localhost TCP; ephemeral port; three separate JVMs',
    'jvm_args':['--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED','-Djava.io.tmpdir=<unique-run-directory>'],
    'source_hashes':{},
}
for p in (REPO/'src/main').rglob('*'):
    if p.is_file():
        rel=p.relative_to(REPO)
        if p.read_bytes() != (BUILD/rel).read_bytes(): raise RuntimeError(f'Stale build {rel}')
        environment['source_hashes'][str(rel)] = hashlib.sha256(p.read_bytes()).hexdigest()
environment['engine_jar_sha256']=hashlib.sha256((BUILD/'target/exchange-lab-1.0-SNAPSHOT.jar').read_bytes()).hexdigest()
patch = capture(['git','diff','HEAD','--','src/main'])+'\n'
for name in capture(['git','ls-files','--others','--exclude-standard','--','src/main']).splitlines():
    result = subprocess.run(['git','diff','--no-index','--','/dev/null',name],cwd=REPO,env=ENV,text=True,capture_output=True)
    if result.returncode != 1: raise RuntimeError(f'Cannot snapshot {name}')
    patch += result.stdout
(OUTPUT/'implementation.patch').write_text(patch)
environment['implementation_patch_sha256']=hashlib.sha256(patch.encode()).hexdigest()
rng=random.Random(20261001)
plan=[]
for round_number in range(1,6):
    cases=[(8,8),(16,16),(32,32),(8,32),(16,32)]
    rng.shuffle(cases)
    for backend,client in cases:
        plan.append({'profile':'baseline','round':round_number,'engine_window':backend,'gateway_window':backend,
                     'client_window':client,'warmup':500,'deadline_ms':5000})
for round_number in range(1,4):
    cases=[8,16,32];rng.shuffle(cases)
    for window in cases:
        plan.append({'profile':'pressure','round':round_number,'engine_window':window,'gateway_window':window,
                     'client_window':256,'warmup':0,'deadline_ms':5000})
    plan.append({'profile':'deadline','round':round_number,'engine_window':8,'gateway_window':8,
                 'client_window':32,'warmup':0,'deadline_ms':10})
environment['run_order']=plan
rows=[]
try:
    for seq, case in enumerate(plan,1):
        name=f"{seq:02d}-{case['profile']}-round-{case['round']}-backend-{case['engine_window']}-client-{case['client_window']}"
        temp=Path(tempfile.mkdtemp(prefix='exchange-grpc-measure-',dir='/private/tmp'))
        logs={key:OUTPUT/'logs'/f'{name}.{key}.log' for key in ['engine','gateway','benchmark']}
        common=[str(JAVA),'--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED',f'-Djava.io.tmpdir={temp}','-cp',classpath]
        engine=gateway=None
        started=stamp()
        try:
            with logs['engine'].open('w') as log:
                engine=subprocess.Popen(common+['dev.sam.exchange.transport.AeronEngineServer',str(temp/'archive'),
                    '--quiet',f"--log-window={case['engine_window']}",f"--stage-timing={case['warmup']},2000"],
                    cwd=BUILD,env=ENV,stdout=log,stderr=subprocess.STDOUT)
            await_text(engine,logs['engine'],'Server ready:')
            with logs['gateway'].open('w') as log:
                gateway=subprocess.Popen(common+['dev.sam.exchange.gateway.GrpcGatewayServer','--port=0',
                    f"--max-in-flight={case['gateway_window']}",'--queue-capacity=128','--diagnostics'],
                    cwd=BUILD,env=ENV,stdout=log,stderr=subprocess.STDOUT)
            await_text(gateway,logs['gateway'],'gRPC gateway listening on port ')
            port=re.search(r'listening on port (\d+)',logs['gateway'].read_text()).group(1)
            with logs['benchmark'].open('w') as log:
                client=subprocess.run(common+['dev.sam.exchange.gateway.GrpcLatencyBenchmark',str(case['warmup']),
                    '2000',str(case['client_window']),str(case['deadline_ms']),port],cwd=BUILD,env=ENV,
                    stdout=log,stderr=subprocess.STDOUT,timeout=60)
            if client.returncode: raise RuntimeError(f'Benchmark failed: {name}')
            if engine.poll() is not None or gateway.poll() is not None: raise RuntimeError(f'Service exited: {name}')
        finally:
            try: stop(gateway,name+' gateway')
            finally:
                try: stop(engine,name+' engine')
                finally: shutil.rmtree(temp)
        report=logs['benchmark'].read_text()
        row={'sequence':seq,**case,'started_at':started,'finished_at':stamp()}
        for label,key,suffix in [('Completed','completed',' requests'),('Successful','successful',' requests'),
                                 ('RPC failures','rpc_failures',''),('Peak outstanding','peak_outstanding','')]:
            row[key]=metric(report,label,suffix,True)
        for code in ['DEADLINE_EXCEEDED','UNAVAILABLE','CANCELLED','INTERNAL','UNKNOWN']:
            row['status_'+code.lower()]=metric(report,'Status '+code,integer=True)
        for label,key in [('Successful throughput','successful_rps'),('Completion throughput','completion_rps')]:
            row[key]=metric(report,label,' requests/s')
        for kind in ['Terminal','Success']:
            for stat in ['p50','p99','max']:
                row[f'{kind.lower()}_{stat}_us']=metric(report,kind+' '+stat,' us') if kind=='Terminal' or row['successful'] else None
        gateway_report=logs['gateway'].read_text()
        for label,key in [('accepted','accepted'),('queue full','queue_full'),('activated','activated'),
                          ('queue high-water (observed)','queue_high_water'),('active high-water','active_high_water'),
                          ('successful offers','offers'),('retries','retries'),('offer timeouts','offer_timeouts'),
                          ('reply timeouts','reply_timeouts')]:
            row['gateway_'+key]=metric(gateway_report,'Gateway '+label,integer=True)
        row['queue_wait_mean_us']=metric(gateway_report,'Gateway queue wait mean',' us')
        row['queue_wait_max_us']=metric(gateway_report,'Gateway queue wait max',' us')
        engine_report=logs['engine'].read_text()
        sample=re.search(r'Stage timing: (\d+)/2000 samples, skipped (\d+)/(\d+) completed logged requests',engine_report)
        if not sample: raise RuntimeError(f'Missing engine count: {name}')
        row['engine_samples']=int(sample.group(1));row['engine_warmup']=int(sample.group(2))
        for label,key in [('Recording observation','recording_observation'),('Server total','server_total')]:
            match=re.search(rf'(?m)^{label}: mean=([0-9.]+) p50=([0-9.]+) p99=([0-9.]+) max=([0-9.]+) us$',engine_report)
            for stat,value in zip(['mean','p50','p99','max'],match.groups() if match else [None]*4):
                row[f'{key}_{stat}_us']=float(value) if value is not None else None
        assert row['completed']==2000 and row['successful']+row['rpc_failures']==2000
        assert row['gateway_active_high_water']<=case['gateway_window'] and row['gateway_queue_high_water']<=128
        assert row['gateway_retries']==0 and row['gateway_reply_timeouts']==0 and row['gateway_offer_timeouts']==0
        assert row['gateway_accepted']==row['gateway_activated']==row['gateway_offers']
        assert row['gateway_accepted']==row['engine_samples']+row['engine_warmup']
        if case['profile']=='baseline':
            assert row['rpc_failures']==row['gateway_queue_full']==0
            assert row['engine_samples']==2000 and row['engine_warmup']==500
        for text in [gateway_report,engine_report]:
            if 'Exception' in text or 'shutdown exceeded' in text or 'Result:' in text:
                raise RuntimeError(f'Unexpected service output: {name}')
        rows.append(row)
        (OUTPUT/'runs.json').write_text(json.dumps(rows,indent=2)+'\n')
        with (OUTPUT/'runs.csv').open('w',newline='') as file:
            writer=csv.DictWriter(file,fieldnames=list(row));writer.writeheader();writer.writerows(rows)
        print(f"{seq:02d}/{len(plan)} {case['profile']} backend={case['engine_window']} client={case['client_window']}: "
              f"{row['successful_rps']:.0f} success/s, success p99={row['success_p99_us']} us, "
              f"errors={row['rpc_failures']}, queue full={row['gateway_queue_full']}, "
              f"deadlines={row['status_deadline_exceeded']}, queue peak={row['gateway_queue_high_water']}",flush=True)
finally:
    environment['finished_at']=stamp();environment['completed_runs']=len(rows)
    environment['exchange_processes_after']=exchange_processes()
    environment['load_average_after']=os.getloadavg()
    environment['sources_still_match']=all(hashlib.sha256((REPO/name).read_bytes()).hexdigest()==digest
                                           for name,digest in environment['source_hashes'].items())
    (OUTPUT/'environment.json').write_text(json.dumps(environment,indent=2)+'\n')
print(f'Saved {len(rows)} runs to {OUTPUT}',flush=True)
