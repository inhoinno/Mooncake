#!/usr/bin/env python3
"""One-file, two-host TRANSFER_ONLY lab preparation and RDMA smoke test.

Run this file with Python, never source it into an SSH login shell. It delegates
payload work to run_callosum_relay.sh and the existing metadata benchmark.
No DAX write occurs before explicit extent authorization. A failed hardware run
retains services, mappings and journals; this program never reclaims arenas.
"""
import argparse
import hashlib
import importlib.util
import inspect
import ipaddress
import json
import os
from pathlib import Path
import re
import resource
import secrets
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlparse
from urllib.error import URLError


SOURCE_FILES = (
    'scripts/callosum_runtime.py', 'scripts/callosum_dispatch.py',
    'scripts/callosum_initiation.py', 'scripts/callosum_worker.py',
    'scripts/callosum_transport_benchmark.py', 'scripts/validate_relay_trace.py',
    'scripts/callosum_benchmark_report.py',
    'scripts/callosum_write_mini.py',
    'scripts/callosum_write_matrix.py', 'scripts/callosum_dram_benchmark.py',
    'scripts/callosum_benchmark_prepare.py',
    'mooncake-store/src/pool_relay.cpp', 'mooncake-store/include/pool_relay.h',
    'mooncake-integration/pool_relay/bindings.cpp',
    'mooncake-integration/pool_relay/relay_batch.h',
    'mooncake-integration/pool_relay/scatter_layout.h',
    'mooncake-integration/transfer_engine/transfer_engine_py.cpp',
)
DELIVERY_ROOT_FILES = (
    'scripts/callosum_lab.sh', 'scripts/capture_preflight.sh',
    'scripts/rdma-write-matrix.sh', 'scripts/cxl-write-mini.sh',
    'experiments/two_worker_metadata_lookup/lab.example.json',
    'experiments/two_worker_metadata_lookup/run.sh',
    'experiments/two_worker_metadata_lookup/benchmark.py',
    'experiments/cxl_write_mini/matrix.example.json',
    'docs/DRAM_RDMA_BASELINE_RUNBOOK.md',
)
PACKAGES = (
    'build-essential', 'cmake', 'git', 'pkg-config', 'python3-dev', 'python3-venv',
    'libgoogle-glog-dev', 'libgflags-dev', 'libjsoncpp-dev', 'libnuma-dev',
    'libibverbs-dev', 'libasio-dev', 'libyaml-cpp-dev',
    'daxctl', 'rdma-core', 'ibverbs-utils', 'iproute2',
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            h.update(block)
    return h.hexdigest()


def clean_env():
    env = dict(os.environ)
    for key in ('LD_PRELOAD', 'LD_LIBRARY_PATH', 'PYTHONPATH', 'PYTHONHOME',
                'PYTHONOPTIMIZE', 'CPATH', 'CPLUS_INCLUDE_PATH', 'C_INCLUDE_PATH',
                'LIBRARY_PATH', 'M1_TEST_ARTIFACT_DIR', 'METADATA_TEST_ARTIFACT_DIR',
                'CALLOSUM_CONTROL_TOKEN', 'CALLOSUM_DESTRUCTIVE_ACK'):
        env.pop(key, None)
    # The RDMA agent maps only the manifest extent. An inherited CXL variable
    # would make Transfer Engine auto-install a second CXL backend elsewhere.
    for key in tuple(env):
        if key.startswith(('MC_CXL_', 'FAKETRACT_')):
            env.pop(key)
    env['CTEST_PARALLEL_LEVEL'] = '1'
    return env


class Runner:
    def __init__(self, run):
        self.run_dir = Path(run)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.log = (self.run_dir / 'commands.log').open('a', buffering=1)
        self.log_lock = threading.RLock()
        self.stage = 'initialization'
        self.services_may_be_live = False
        self.configuration_effects = []
        self.commands_started = []
        self.children = {}
        self.device_open_checked = False

    def say(self, text):
        with self.log_lock:
            print(text, flush=True)
            self.log.write(text + '\n')

    def call(self, stage, command, env=None, capture=False, input_text=None, log_stdout=True):
        self.stage = stage
        command = [str(v) for v in command]
        self.commands_started.append(stage)
        self.say('\n[stage] ' + stage + '\n$ ' + shlex.join(command))
        # No shell, errexit, nounset or pipefail. Child failure is explicit.
        if capture:
            result = subprocess.run(command, env=env, text=True, capture_output=True, input=input_text)
            with self.log_lock:
                self.log.write((result.stdout if log_stdout else '[raw JSON saved in trial artifact]\n') + result.stderr)
            require(result.returncode == 0,
                    f'{stage}: exit={result.returncode}\n{result.stderr[-3000:]}')
            return result.stdout
        require(input_text is None, 'Private stdin requires capture=True')
        with subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, errors='replace') as child:
            for line in child.stdout:
                print(line, end='', flush=True)
                self.log.write(line)
            code = child.wait()
        require(code == 0, f'{stage}: exit={code}; see {self.run_dir}/commands.log')


def source_path(root):
    root = Path(root).resolve()
    source = root / 'Mooncake-dev'
    require((source / 'scripts/callosum_runtime.py').is_file(),
            f'Not the Callosum root: {root}')
    return source


def package_files(package):
    return subprocess.check_output(['dpkg', '-L', package], text=True).splitlines()


def check_glog_dependencies(dependencies, library):
    selected = re.findall(r'libglog[^\s]*\s+=>\s+(/\S+)', dependencies)
    require(selected and all(Path(p).resolve() == Path(library).resolve() for p in selected),
            'New engine does not resolve to the selected package-owned glog library')
    require('not found' not in dependencies, 'New engine still has unresolved native dependencies')


def prepare(root, run, args, runner):
    require(sys.platform.startswith('linux'), 'Build/prepare requires the Linux lab host')
    source = source_path(root)
    require(not (run / 'setup.json').exists(),
            'setup.json already exists; use start/test, or a new preparation campaign')
    runner.call('source-head', ['git', '-C', source, 'log', '-1', '--oneline'])
    runner.call('source-status', ['git', '-C', source, 'status', '--short'])
    runner.call('launcher-tests', [args.python, source/'scripts/test_callosum_lab.py', '-v'], clean_env())
    # Build the selected checkout. Historical backports must be delivered as
    # source changes, never replayed over current or partially updated files.
    runner.stage = 'native-dependencies'
    missing = []
    for package in PACKAGES:
        p = subprocess.run(['dpkg-query', '-W', '-f=${Status}', package], capture_output=True, text=True)
        if p.returncode or p.stdout.strip() != 'install ok installed':
            missing.append(package)
    if missing:
        require(not args.skip_apt, 'Missing packages: ' + ' '.join(missing))
        prefix = [] if os.geteuid() == 0 else ['sudo']
        runner.call('install-only-missing-packages', prefix + ['apt-get', 'install', '-y'] + missing)
    runner.call('submodules', ['git', '-C', source, 'submodule', 'update', '--init',
                              'extern/pybind11', 'extern/yalantinglibs'])
    build = Path(tempfile.mkdtemp(prefix='build-callosum-lab.', dir=source))
    env = clean_env()
    runner.call('python-venv', [args.python, '-m', 'venv', build / '.venv'], env)
    python = build / '.venv/bin/python'
    files = package_files('libgoogle-glog-dev')
    headers = next((Path(p) for p in files if p.endswith('/glog/logging.h')), None)
    library = next((Path(p) for p in files if p.endswith('/libglog.so')), None)
    cfg = next((Path(p) for p in files if p.endswith(('/glog-config.cmake', '/glogConfig.cmake'))), None)
    require(headers and library and cfg and headers.is_file() and library.is_file() and cfg.is_file(),
            'Installed glog development package is incomplete; retain commands.log')
    # Copy package-owned headers, so /usr/local/include cannot shadow them.
    includes = build / 'matched-headers'
    includes.mkdir()
    shutil.copytree(headers.parent, includes / 'glog')
    runner.say(f'[glog] headers={headers}; config={cfg}; library={library.resolve()}')
    (run / 'glog.json').write_text(json.dumps(dict(headers=str(headers), config=str(cfg),
        library=str(library.resolve()), header_sha256=digest(headers),
        library_sha256=digest(library.resolve())), indent=2))
    # Reuse the canonical revision-checked YLT installer, not an old build graph.
    roots = [source / b / '_deps/yalantinglibs-install'
             for b in ('build-m1-cpu', 'build-todo1-cpu', 'build-gpu-multipath')]
    prefix = next((p for p in roots if p.is_dir()), source / 'build-callosum-deps/yalantinglibs-install')
    runner.call('yalantinglibs', ['bash', source / 'scripts/bootstrap_todo0_lab.sh',
                '--prefix-root', prefix, '--jobs', args.jobs], env)
    ylt = prefix / 'current/lib/cmake/yalantinglibs'
    require((ylt / 'yalantinglibsConfig.cmake').is_file(), 'YLT installation did not publish its config')
    engine_build = build / 'engine'
    flags = dict(CMAKE_BUILD_TYPE='Release', BUILD_SHARED_LIBS='OFF',
        WITH_TE='ON', WITH_STORE='OFF', WITH_STORE_RUST='OFF', WITH_EP='OFF',
        BUILD_UNIT_TESTS='OFF', BUILD_EXAMPLES='OFF', BUILD_BENCHMARK='OFF',
        USE_CUDA='OFF', USE_HIP='OFF', USE_NVMEOF='OFF', USE_MNNVL='OFF',
        USE_NCCL_DEVICE='OFF', USE_NCCL_HOST='OFF', USE_VRAM_SEGMENT='OFF',
        USE_MUSA='OFF', USE_MACA='OFF', USE_CXL='ON', USE_TENT='OFF',
        USE_HTTP='OFF', USE_ETCD='OFF', USE_REDIS='OFF', WITH_METRICS='OFF',
        Python3_EXECUTABLE=str(python), PYTHON_EXECUTABLE=str(python),
        glog_DIR=str(cfg.parent), yalantinglibs_DIR=str(ylt),
        CMAKE_CXX_FLAGS=f'-I"{includes}"')
    runner.call('engine-configure', ['cmake', '-S', source, '-B', engine_build] +
                [f'-D{k}={v}' for k, v in flags.items()], env)
    runner.call('engine-build', ['cmake', '--build', engine_build, '--target', 'engine', '-j', args.jobs], env)
    suffix = runner.call('python-extension-suffix', [python, '-c',
        'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))'], env, capture=True).strip()
    engine = engine_build / ('mooncake-integration/engine' + suffix)
    require(engine.is_file(), 'engine build did not produce the selected Python extension')
    package = build / 'python/mooncake'
    package.mkdir(parents=True)
    # No old engine.so/store.so and no cross-host binaries enter this overlay.
    for file in (source / 'mooncake-wheel/mooncake').glob('*.py'):
        shutil.copy2(file, package / file.name)
    shutil.copy2(engine, package / 'engine.so')
    shutil.copy2(engine_build / 'mooncake-common/libasio.so', package / 'libasio.so')
    native = build / 'native'
    env.update(PYTHON_BIN=str(python), CALLOSUM_NATIVE_BUILD=str(native), BUILD_JOBS=str(args.jobs),
        PYTHONPATH=os.pathsep.join(map(str, (native, build / 'python', source / 'scripts'))),
        LD_LIBRARY_PATH=os.pathsep.join(map(str, (package, library.resolve().parent))))
    runner.call('native-build', ['bash', source / 'scripts/run_callosum_relay.sh', 'build'], env)
    dependencies = runner.call('engine-dependencies', ['ldd', package / 'engine.so'], env, capture=True)
    runner.say(dependencies)
    check_glog_dependencies(dependencies, library)
    runner.call('engine-import-and-origin', [python, '-c', '''
import hashlib,json,os,pathlib
import mooncake.engine as e
import callosum_relay_native as n
p=pathlib.Path(e.__file__).resolve()
if p.parent != pathlib.Path(os.environ['CALLOSUM_PACKAGE']).resolve(): raise RuntimeError('stale engine import')
if n.ABI_VERSION != 2: raise RuntimeError('native ABI mismatch')
if pathlib.Path(n.__file__).resolve().parent != pathlib.Path(os.environ['CALLOSUM_NATIVE_BUILD']).resolve(): raise RuntimeError('stale native import')
t=e.TransferEngine()
if t.relay_abi_version()!=2 or not hasattr(t,'relay_write_batch_drained') or not hasattr(t,'relay_rdma_available'): raise RuntimeError('engine relay ABI missing')
print(json.dumps({'engine':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'native':n.__file__,'abi':2,'hardware_validated':False}))
'''], dict(env, CALLOSUM_PACKAGE=str(package)))
    core = build / 'core'
    runner.call('core-configure', ['cmake', '-S', source / 'mooncake-store/tests/pool_relay_standalone',
                                 '-B', core, '-DCMAKE_BUILD_TYPE=Release'], env)
    runner.call('core-build', ['cmake', '--build', core, '-j', args.jobs], env)
    runner.call('core-tests', ['ctest', '--test-dir', core, '--output-on-failure', '--no-tests=error'], env)
    runner.call('runtime-tests', ['ctest', '--test-dir', native, '--output-on-failure', '--no-tests=error'], env)
    runner.call('initiation-tests', [python, source / 'scripts/test_callosum_initiation.py', '-v'], env)
    for scenario in ('two_worker_metadata_lookup', 'four_worker_two_pool'):
        runner.call(scenario, ['bash', Path(root) / 'experiments' / scenario / 'run.sh', 'software-test'], env)
    receipt = dict(status='PASS', scope='SOFTWARE_ONLY', python=str(python), build=str(build),
        env={k:env[k] for k in ('PYTHON_BIN', 'CALLOSUM_NATIVE_BUILD', 'PYTHONPATH', 'LD_LIBRARY_PATH')},
        source_hashes={p:digest(source / p) for p in SOURCE_FILES})
    (run / 'setup.json').write_text(json.dumps(receipt, indent=2))
    (run / 'env.sh').write_text(''.join(f'export {k}={shlex.quote(v)}\n' for k,v in receipt['env'].items()))
    runner.say(f'[PASS] software preparation; env={run}/env.sh; no DAX/RDMA/GPU transfer performed')


def inventory():
    def probe(command):
        if not shutil.which(command[0]):
            return dict(status='SKIP', reason='command unavailable')
        p = subprocess.run(command, text=True, capture_output=True, timeout=20)
        return dict(status='PASS' if p.returncode == 0 else 'FAIL', output=p.stdout, error=p.stderr)
    dax = probe(['daxctl', 'list'])
    geometry = {}
    def visit(value):
        if isinstance(value, list):
            for row in value:
                visit(row)
        elif isinstance(value, dict):
            if 'chardev' in value:
                geometry['/dev/'+value['chardev']] = dict(size=value.get('size'),align=value.get('align'),mode=value.get('mode'))
            for row in value.values():
                if isinstance(row, (list,dict)):
                    visit(row)
    if dax['status']=='PASS':
        visit(json.loads(dax['output']))
    return dict(host=socket.gethostname(), hcas=sorted(p.name for p in Path('/sys/class/infiniband').glob('*')),
        devices=sorted(str(p) for p in Path('/dev').glob('dax*.*') if stat.S_ISCHR(p.stat().st_mode)),
        geometry=geometry,memlock=resource.getrlimit(resource.RLIMIT_MEMLOCK),
        dax=dax, network=probe(['ip', '-br', 'address']),
        rdma=probe(['rdma', 'link', 'show']))


def ask(label, default=None):
    value = input(label + (f' [{default}]' if default is not None else '') + ': ').strip()
    return value or (str(default) if default is not None else '')


def host_fields(label, inv):
    print('\n' + label + ' inventory:\n' + json.dumps(inv, indent=2))
    ip = ask(label + ' reachable control/engine IPv4 address')
    require(ipaddress.ip_address(ip).version == 4, 'This bounded launcher currently requires IPv4')
    engine_ip = ask(label + ' reachable Transfer Engine IPv4 address', ip)
    require(ipaddress.ip_address(engine_ip).version == 4, 'Engine address must be IPv4')
    path = ask(label + ' authorized DAX path', inv['devices'][0] if len(inv['devices']) == 1 else None)
    require(path in inv['devices'], 'Select a character DAX device from this host inventory')
    geometry = inv['geometry'].get(path, {})
    require(type(geometry.get('size')) is int and type(geometry.get('align')) is int,
            'daxctl must report device size and alignment in bytes before writes')
    require(geometry.get('mode')=='devdax', 'The selected device must already be devdax; no reconfiguration is performed')
    hca = ask(label + ' RDMA HCA', inv['hcas'][0] if len(inv['hcas']) == 1 else None)
    require(hca in inv['hcas'], 'Select an inventoried RDMA HCA')
    offset = int(ask(label + ' exclusive test OFFSET (bytes; no default)'))
    size = int(ask(label + ' exclusive test SIZE (bytes; no default)'))
    align = geometry['align']
    require(offset >= 0 and size >= 4*1024**2 and align >= 4096 and align & (align-1) == 0
            and offset % align == 0 and size % align == 0, 'Invalid/alignment-conflicting test extent')
    require(offset + size <= geometry['size'], 'Approved extent exceeds the inventoried device size')
    owner = ask(label + ' exclusive extent OWNER')
    identity = ask(label + ' physical device serial/inventory identity (not just dax filename)')
    require(owner and identity, 'Owner and inventory-backed physical identity are required')
    return dict(ip=ip, engine_ip=engine_ip, path=path, hca=hca, pool=dict(capacity=size, map_offset=offset,
        mapping_alignment=align, owner=owner, physical_pool_id=identity))


def manifest(a, b):
    require(a['pool']['physical_pool_id'] != b['pool']['physical_pool_id'], 'Pools must be independent')
    require(a['ip'] != b['ip'], 'Select two physical hosts, not two processes on one host')
    nodes = [dict(id=n, pool=p, url=f'http://{h["ip"]}:19101', path=h['path'],
        te_endpoint=f'{h["engine_ip"]}:19301', rdma_device=h['hca'], can_send=True, can_receive=True)
        for n,p,h in (('a1','A',a), ('b1','B',b))]
    return dict(evidence_class='TRANSFER_ONLY', abi_version=2, mode='rdma',
        path_mode='direct_dax_write', master=f'http://{a["ip"]}:19100', metadata='P2PHANDSHAKE',
        batch_cap=32, max_inflight_bytes=67108864, pools=dict(A=a['pool'],B=b['pool']), nodes=nodes)


def install_configuration(config_path, token_path, config, secret):
    """Create a private shared pair, or accept an exact existing pair. Never overwrite."""
    # Keep dependencies local: the initializer sends this function over the
    # existing authorized SSH channel, not a new service or public endpoint.
    import hmac
    import json
    import os
    from pathlib import Path
    config_path, token_path = Path(config_path), Path(token_path)
    paths=(config_path,token_path)
    if (not all(p.is_absolute() and p.parent.is_dir() and not p.is_symlink() for p in paths)
            or config_path==token_path or not isinstance(config,dict)
            or not isinstance(secret,str) or len(secret)<32):
        raise RuntimeError('Invalid configuration destinations or shared token')
    present=[p.exists() for p in paths]
    if any(present):
        if not all(present) or not all(p.is_file() for p in paths):
            raise RuntimeError('Partial/existing configuration: preserve files and resolve ownership; no overwrite')
        if (token_path.stat().st_mode & 0o077
                or json.loads(config_path.read_text())!=config
                or not hmac.compare_digest(token_path.read_text().strip(),secret)):
            raise RuntimeError('Existing configuration/token conflict; no overwrite')
        return dict(status='EXISTS_MATCHED',config=str(config_path),token_file=str(token_path))
    for path,text in ((config_path,json.dumps(config,indent=2)+'\n'),(token_path,secret+'\n')):
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
    return dict(status='CREATED',config=str(config_path),token_file=str(token_path))


def initialize_configuration(root, run, args, runner):
    """Explicit first-run setup. Inventory is read-only; configuration copy uses SSH."""
    runner.stage='configuration-initialization'
    require(sys.platform.startswith('linux'), 'Initialize on an authorized Linux lab host')
    root=Path(root).resolve()
    require(args.config and args.token_file, 'Configuration and token paths are required')
    config_path,token_path=Path(args.config).resolve(),Path(args.token_file).resolve()
    try:
        config_relative=config_path.relative_to(root)
        token_relative=token_path.relative_to(root)
    except ValueError:
        raise RuntimeError('Initialization destinations must be inside the selected checkout')
    require(config_path!=token_path and config_path.parent.is_dir() and token_path.parent.is_dir(),
            'Distinct existing configuration/token parent directories required')
    existing=(config_path.exists(),token_path.exists())
    require(existing[0]==existing[1], 'Partial configuration exists; preserve files instead of replacing them')
    template_path=root/'experiments/two_worker_metadata_lookup/lab.example.json'
    config=json.loads((config_path if existing[0] else template_path).read_text())
    launch=config.get('lab_run',{})
    nodes=config.get('nodes',[])
    require(len(nodes)==2 and launch.get('profile')=='two_worker_smoke',
            'This initialization profile requires the two-worker template')
    require(args.node_id and args.node_id==launch.get('controller'),
            '--initialize must run on the configured controller node; other nodes use the copied shared files')
    require(len({n.get('id') for n in nodes})==2 and
            all(isinstance(n.get('id'),str) and isinstance(n.get('pool'),str) for n in nodes),
            'Invalid template node records')
    peer_node=next(n for n in nodes if n['id']!=args.node_id)
    peer=args.peer or ask('Authorized peer SSH target (configuration/inventory only)')
    peer_root=Path(args.remote_root or ask('Peer Callosum checkout root',str(root)))
    require(peer_root.is_absolute(), 'Peer checkout root must be absolute')
    remote_config,remote_token=peer_root/config_relative,peer_root/token_relative
    # The peer must already have this repository. This call reads inventory only.
    remote_inv=json.loads(runner.call('peer-read-only-inventory',remote_command(peer,
        ['python3',peer_root/'Mooncake-dev/scripts/callosum_lab.py','--action','inventory']),
        env=clean_env(),capture=True))
    local_inv=inventory()
    require(local_inv['host']!=remote_inv['host'], 'Peer is the same host; two physical workers required')
    inventories={args.node_id:local_inv,peer_node['id']:remote_inv}
    runner.stage='configuration-initialization'
    if not existing[0]:
        fields={n['id']:host_fields(n['id'],inventories[n['id']]) for n in nodes}
        for node in nodes:
            host=fields[node['id']]
            node.update(path=host['path'],rdma_device=host['hca'],
                        url=f'http://{host["ip"]}:19101',te_endpoint=f'{host["engine_ip"]}:19301')
            config['pools'][node['pool']]=host['pool']
        config['master']=f'http://{fields[args.node_id]["ip"]}:19100'
        secret=secrets.token_hex(32)
    else:
        require(token_path.is_file() and token_path.stat().st_mode & 0o077==0,
                'Existing TOKEN_FILE must be private')
        secret=token_path.read_text().strip()
    validate_launch_config(config,args.node_id)
    for node in nodes:
        validate_node_extent(config,node['id'],inventories[node['id']])
    runner.say('[proposed-config]\n'+json.dumps(config,indent=2))
    runner.say(f'[config-copy] local={config_path}, {token_path}; peer={peer}:{remote_config}, {remote_token}')
    require(ask('Confirm exclusive test extents, independent devices and authorized config copy. Type INITIALIZE CONFIG')
            =='INITIALIZE CONFIG','Configuration initialization not authorized; no files or devices changed')
    (run/'initialization-inventory.json').write_text(json.dumps(inventories,indent=2))
    runner.configuration_effects.append(dict(location='local',result='INSTALL_ATTEMPTED; existing/partial files must be preserved'))
    installed=install_configuration(str(config_path),str(token_path),config,secret)
    runner.configuration_effects[-1]['result']=installed['status']
    # Token goes through stdin, never argv, output or the command log.
    installer=inspect.getsource(install_configuration)+"""
import json,sys
bundle=json.load(sys.stdin)
print(json.dumps(install_configuration(sys.argv[1],sys.argv[2],bundle['config'],bundle['token'])))
"""
    runner.configuration_effects.append(dict(location='peer',result='INSTALL_ATTEMPTED; inspect peer if SSH fails'))
    runner.call('peer-private-config-install',remote_command(peer,
        ['python3','-c',installer,remote_config,remote_token]),
        env=clean_env(),capture=True,input_text=json.dumps(dict(config=config,token=secret)))
    runner.say('[CONFIG READY] Shared manifest/token installed on both hosts. Initialization stops here. No service started; no DAX mapped. Run --check-config on each host before the separate hardware commands.')


def load_setup(run):
    data = json.loads((run / 'setup.json').read_text())
    require(data['status'] == 'PASS', 'Preparation is not PASS')
    env = clean_env()
    env.update(data['env'])
    env.update(MC_NUM_QP_PER_EP='16', CALLOSUM_DESTRUCTIVE_ACK='1',
               CALLOSUM_CONTROL_TOKEN=(run / 'control.token').read_text().strip())
    return data, env


def transport_sweep(root, args):
    """Reuse a live campaign under the caller's credentials. Never repair permissions."""
    parent = Path(root)/'runs'
    parent.mkdir(exist_ok=True)
    run = Path(args.run_dir).resolve() if args.run_dir else Path(tempfile.mkdtemp(prefix='cxl-write-sweep-',dir=parent))
    if args.run_dir:
        require(not run.exists(), 'Sweep evidence directory already exists; preserve it')
        run.mkdir(parents=True)
    runner = Runner(run)
    remote_mutation_possible = False
    try:
        from callosum_benchmark_report import enrich_trial, write_request_csv, write_report
        runner.stage = 'campaign-receipt'
        require(args.setup_run, '--setup-run must select the existing healthy campaign')
        campaign = Path(args.setup_run).resolve()
        receipt, env = load_setup(campaign)
        python = receipt['env']['PYTHON_BIN']
        require(os.access(python, os.X_OK),
                'Campaign interpreter is not executable by this caller. Use the same sudo execution '
                'context as campaign creation; do not chmod/chown the build recursively.')
        config = campaign/'topology.json'
        manifest = json.loads(config.read_text())
        nodes = {n['id']:n for n in manifest['nodes']}
        source = args.source or manifest['lab_run']['source']
        receiver = args.receiver or manifest['lab_run']['requester']
        require(source in nodes and receiver in nodes and nodes[source]['pool'] != nodes[receiver]['pool'],
                'Select a source and receiver from distinct pools')
        require(args.repetitions > 0 and args.unit in ('MB','MiB') and args.sizes and
                len(set(args.sizes)) == len(args.sizes), 'Invalid sweep settings')
        from callosum_transport_benchmark import block_bytes, TOTAL_BYTES
        sizes = [block_bytes(size,args.unit) for size in args.sizes]
        require(max(sizes) <= manifest.get('max_inflight_bytes',67108864),
                'Block exceeds the running campaign budget; do not hot-edit its manifest')
        relay = source_path(root)/'scripts/run_callosum_relay.sh'
        def command(stage, *values):
            options = dict(capture=True)
            if stage.startswith('block-'):
                options['log_stdout'] = False
            result = json.loads(runner.call(stage,['bash',relay,*values,'--config',config],env,**options))
            (run/(stage+'.json')).write_text(json.dumps(result,indent=2)+'\n')
            return result
        effective = command('effective-config','check')
        status = command('status-before','status')
        require(status.get('fingerprint') == effective['config']['fingerprint'] and
                status.get('topology_verified') and not status.get('startup_aborted') and
                set(status.get('registered_nodes',[])) == set(nodes) and
                all(o['state'] in ('COMMITTED','DRAINED_RETAINED') for o in status.get('operations',[])),
                'Campaign is not healthy and ready; retain services/journals and inspect status-before.json')
        require(status.get('benchmark_request_timing_versions',{}).get(source) == 1,
                'Running sender has no per-request timing. Existing aggregate files can be reported offline. '
                'Do not hot-restart live agents; prepare an authorized updated campaign for latency measurement.')
        key = 'transport-' + secrets.token_hex(12)
        # RPC uncertainty can include a new allocation even if the client receives no reply.
        remote_mutation_possible = True
        command('source-seed','seed','--node',source,'--key',key,'--bytes',max(sizes))
        rows = []
        for size in args.sizes:
            for repetition in range(args.repetitions):
                row = command(f'block-{size}{args.unit}-rep-{repetition}',
                    'transport-benchmark','--node',source,'--receiver',receiver,'--key',key,
                    '--block-size',size,'--block-unit',args.unit)
                require(row.get('status') == 'PASS' and row.get('bytes') == TOTAL_BYTES and
                        row.get('actual_transport') == 'rdma' and row.get('path_mode') == 'direct_dax_write',
                        'Transport result failed byte/path validation')
                require(row.get('request_timing_version') == 1 and 'requests' in row,
                        'Missing per-request timing; do not infer latency samples from aggregate time')
                row.update(repetition=repetition,direction=f'{source}->{receiver}',warmup_blocks=0,warmup_bytes=0)
                row = enrich_trial(row)
                (run/f'block-{size}{args.unit}-rep-{repetition}.json').write_text(json.dumps(row,indent=2)+'\n')
                write_request_csv(run/f'requests-{size}{args.unit}-rep-{repetition}.csv',row,len(rows))
                rows.append(row)
                runner.say(f"[PASS] {size} {args.unit} rep={repetition}: {row['GBps']:.3f} GB/s")
        summary = dict(status='PASS',evidence_class='TRANSFER_ONLY',path_mode='direct_dax_write',
                       source=source,receiver=receiver,results=rows,active_qps_verified=False,
                       services_retained=True,evidence=str(run))
        summary['report'] = write_report(run,rows)
        (run/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        runner.say((run/'report.md').read_text())
        runner.say(f'[evidence] {run}')
        return 0
    except Exception as error:
        failure = dict(status='FAIL',stage=runner.stage,error=str(error),evidence=str(run),
            local_services_started=False,local_devices_mapped=False,
            remote_mutation_possible=remote_mutation_possible,
            action=('Retain live services, arenas and journals. No automatic retry or reclaim.'
                    if remote_mutation_possible else
                    'No seed/WRITE RPC was sent. Correct input/access and rerun under the campaign execution account. '
                    'No ownership or permission repair was attempted.'))
        (run/'failure.json').write_text(json.dumps(failure,indent=2)+'\n')
        runner.say(json.dumps(failure,indent=2))
        return 1
    finally:
        runner.log.close()


def process_stamp(pid):
    try:
        return Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]
    except (OSError, IndexError):
        return None


def start_service(root, run, role, node, runner):
    source = source_path(root)
    receipt, env = load_setup(run)
    for file, expected in receipt['source_hashes'].items():
        require(digest(source / file) == expected, f'Source changed since preparation: {file}')
    name = 'master' if role == 'master' else node
    runner.stage = 'start-' + name
    authorization = json.loads((run / 'authorization.json').read_text())
    require(authorization.get('confirmed') is True and
            authorization.get('config_sha256') == digest(run / 'topology.json'),
            'Missing or changed exclusive-extent authorization; no DAX service started')
    marker = run / f'{name}.process.json'
    require(not marker.exists(), 'Service already launched; do not restart or reuse the retained arena')
    c = json.loads((run / 'topology.json').read_text())
    if role == 'agent':
        soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        if soft != hard:
            resource.setrlimit(resource.RLIMIT_MEMLOCK, (hard, hard))
        selected = next(n for n in c['nodes'] if n['id']==node)
        required = c['pools'][selected['pool']]['capacity']
        require(os.geteuid()==0 or hard==resource.RLIM_INFINITY or hard>=required,
                f'Memlock hard limit={hard} bytes is smaller than the approved mapping={required}. No DAX service started.')
        (run / f'{node}-transport-env.json').write_text(json.dumps({
            key:env.get(key,'unset; native default') for key in
            ('MC_NUM_QP_PER_EP','MC_MTU','MC_GID_INDEX')},indent=2))
    url = c['master'] if role == 'master' else next(n['url'] for n in c['nodes'] if n['id']==node)
    endpoint = urlparse(url)
    with socket.socket() as sock:
        sock.settimeout(1)
        require(sock.connect_ex((endpoint.hostname,endpoint.port)) != 0,
                f'Port already occupied: {url}; no existing process will be killed')
    state = run / 'state'
    require(not (state / ('master.sqlite' if role=='master' else f'agent-{node}.jsonl')).exists(),
            'Old service journal exists. Retain it; do not reuse its possibly active DMA extent')
    command = ['bash', str(source / 'scripts/run_callosum_relay.sh'), role,
               '--config', str(run / 'topology.json'), '--state-dir', str(state)]
    if node:
        command += ['--node', node]
    console = run / f'{name}-console.log'
    with console.open('x') as out:
        # Set before spawning: marker persistence can fail after a child starts.
        runner.services_may_be_live = True
        child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=out,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        runner.children[name] = child
    marker.write_text(json.dumps(dict(pid=child.pid, stamp=process_stamp(child.pid),
        state_dir=str(state), config=str(run / 'topology.json'))))
    # Record ownership before waiting. On failure leave all resources/journals intact.
    deadline = time.monotonic() + 90
    source_str = str(source / 'scripts')
    if source_str not in sys.path:
        sys.path.insert(0, source_str)
    import callosum_runtime as runtime
    os.environ['CALLOSUM_CONTROL_TOKEN'] = env['CALLOSUM_CONTROL_TOKEN']
    while time.monotonic() < deadline:
        require(child.poll() is None, f'{name} exited; see {console}')
        if role == 'agent':
            if f'"registered": "{node}"' in console.read_text(errors='replace'):
                runner.say(f'[READY] {name}; pid={child.pid}')
                return
        else:
            try:
                runtime.rpc(url, 'status', timeout=2)
                runner.say(f'[READY] master; pid={child.pid}')
                return
            except Exception:
                pass
        time.sleep(.2)
    raise RuntimeError(f'{name} readiness timeout; retain resources and inspect {console}')


def check_stop_gate(summary, status, config_hash):
    require(summary.get('status') == 'PASS' and summary.get('config_sha256') == config_hash,
            'No matching successful smoke proof; retain possibly active DMA resources')
    operations = status.get('operations', [])
    require(len(operations) == 2 and all(o.get('state') == 'COMMITTED' for o in operations),
            'Incomplete or unexpected operations; refuse stop and retain mappings')


def stop_owned(run):
    results = []
    for marker in run.glob('*.process.json'):
        record = json.loads(marker.read_text())
        pid = record['pid']
        stamp = process_stamp(pid)
        if stamp is None or record['stamp'] is None or stamp != record['stamp']:
            results.append(dict(service=marker.stem, status='NOT_OWNED_OR_NOT_RUNNING'))
            continue
        cmdline = Path(f'/proc/{pid}/cmdline').read_bytes().decode(errors='replace').split('\0')
        require(record['state_dir'] in cmdline and record['config'] in cmdline,
                'PID command no longer matches the owned service; refuse signal')
        os.kill(pid, signal.SIGTERM)
        results.append(dict(service=marker.stem, status='STOP_SIGNAL_SENT'))
    return dict(services=results, reclaimed=False,
                warning='Stopping does NOT prove DMA drain. Retain all journals and extents; no restart/reuse.')


def rollback_empty_startup(root, run, runner):
    """Stop only this invocation's unused coordinator; never clear a campaign."""
    master = runner.children.get('master')
    if master is None:
        return dict(status='NOT_APPLICABLE',reclaimed=False)
    process_fd=None
    try:
        for name, child in runner.children.items():
            require(name=='master' or child.poll() is not None,
                    'local participant remains alive; retain all services')
        record=json.loads((run/'master.process.json').read_text())
        require(master.pid==record['pid'] and master.poll() is None
                and process_stamp(master.pid)==record['stamp'] and record['stamp'] is not None,
                'coordinator process ownership is unverified')
        require(hasattr(os,'pidfd_open') and hasattr(signal,'pidfd_send_signal'),
                'safe PID-bound signalling unavailable; retain coordinator')
        process_fd=os.pidfd_open(master.pid,0)
        require(process_stamp(master.pid)==record['stamp'], 'coordinator PID changed')
        cmdline=Path(f'/proc/{master.pid}/cmdline').read_bytes().decode(errors='replace').split('\0')
        require(record['state_dir'] in cmdline and record['config'] in cmdline,
                'coordinator command does not match owned campaign')
        sys.path.insert(0,str(source_path(root)/'scripts'))
        runtime=importlib.import_module('callosum_runtime')
        config=runtime.config(run/'topology.json')
        secret=(run/'control.token').read_text().strip()
        # The authenticated RPC seals registration under the authority lock.
        # A status snapshot alone cannot authorize termination.
        previous=os.environ.get('CALLOSUM_CONTROL_TOKEN')
        try:
            os.environ['CALLOSUM_CONTROL_TOKEN']=secret
            sealed=runtime.rpc(config['master'],'abort_empty_startup',
                dict(fingerprint=config['fingerprint']),timeout=5)
        finally:
            if previous is None:
                os.environ.pop('CALLOSUM_CONTROL_TOKEN',None)
            else:
                os.environ['CALLOSUM_CONTROL_TOKEN']=previous
        require(sealed.get('startup_aborted') is True and sealed.get('no_participants_admitted') is True,
                'coordinator did not prove empty sealed startup')
        signal.pidfd_send_signal(process_fd,signal.SIGTERM)
        master.wait(timeout=5)
        result=dict(status='STOPPED_EMPTY_COORDINATOR',reclaimed=False,
            note='Admission sealed; owned coordinator exited. Keep files, registrations and arenas unchanged.')
    except Exception as error:
        result=dict(status='RETAINED',reason=str(error),reclaimed=False)
    finally:
        if process_fd is not None:
            os.close(process_fd)
    (run/'startup-cleanup.json').write_text(json.dumps(result,indent=2))
    return result


def remote_command(peer, command, tty=False, options=()):
    require(peer and not peer.startswith('-') and re.fullmatch(r'[A-Za-z0-9_.@:-]+',peer), 'Invalid SSH target')
    return ['ssh'] + list(options) + (['-tt'] if tty else []) + [peer, shlex.join(map(str, command))]


def validate_launch_config(config, node_id):
    """Validate data before package installation, builds or service launch."""
    require(isinstance(config, dict), 'CONFIG must contain a JSON object')
    require(config.get('mode') == 'rdma' and config.get('path_mode') == 'direct_dax_write'
            and config.get('evidence_class') == 'TRANSFER_ONLY', 'Explicit TRANSFER_ONLY direct-DAX RDMA config required')
    require(config.get('abi_version') == 2 and type(config.get('abi_version')) is int,
            'abi_version must be integer 2')
    nodes, pools, launch = config.get('nodes'), config.get('pools'), config.get('lab_run')
    require(isinstance(nodes,list) and len(nodes) in (2,4) and all(isinstance(n,dict) for n in nodes)
            and isinstance(pools,dict) and len(pools)==2 and all(isinstance(p,dict) for p in pools.values()),
            'Require two/four node records and two pool records')
    def identity(value):
        return isinstance(value,str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}',value)
    require(all(identity(n.get('id')) and identity(n.get('pool')) for n in nodes)
            and all(identity(p) for p in pools), 'Invalid node/pool identity')
    by_id = {n['id']:n for n in nodes}
    require(len(by_id)==len(nodes) and node_id in by_id, 'NODE_ID must select one unique configured node')
    require(isinstance(launch,dict) and set(launch) <= {'controller','profile','source','requester','wait_seconds'},
            'lab_run must contain supported launch settings')
    require(isinstance(launch.get('controller'),str) and launch['controller'] in by_id,
            'lab_run.controller must name a configured node')
    require(launch.get('profile') in ('two_worker_smoke','services_only'), 'Unsupported lab_run.profile')
    require(type(launch.get('wait_seconds',1800)) is int and 1 <= launch.get('wait_seconds',1800) <= 86400,
            'lab_run.wait_seconds must be integer 1..86400')
    require(isinstance(config.get('metadata'),str) and config['metadata']=='P2PHANDSHAKE',
            'This lab launcher requires metadata=P2PHANDSHAKE')
    require(type(config.get('batch_cap',32)) is int and 1 <= config.get('batch_cap',32) <= 256
            and type(config.get('max_inflight_bytes',67108864)) is int
            and config.get('max_inflight_bytes',67108864) >= 65536, 'Invalid batch/in-flight bounds')
    require(type(config.get('max_object_bytes',1073741824)) is int
            and config.get('max_object_bytes',1073741824) >= 1048576, 'Object budget is too small for smoke')
    endpoints=[]
    def control_endpoint(value):
        require(isinstance(value,str), 'Control endpoint must be an explicit IPv4 HTTP URL')
        parsed=urlparse(value)
        require(parsed.scheme=='http' and parsed.hostname and parsed.port and not parsed.username
                and not parsed.password and not parsed.query and not parsed.fragment
                and parsed.path in ('','/'), 'Invalid control endpoint')
        require(ipaddress.ip_address(parsed.hostname).version==4, 'Use explicit IPv4 control addresses')
        return parsed
    master=control_endpoint(config.get('master'))
    for node in nodes:
        require(node['pool'] in pools and type(node.get('can_send')) is bool
                and type(node.get('can_receive')) is bool, 'Invalid pool membership/capabilities')
        require(isinstance(node.get('path'),str) and Path(node['path']).is_absolute()
                and isinstance(node.get('rdma_device'),str) and node['rdma_device'].strip(),
                'Absolute DAX path and RDMA device required per node')
        endpoint=control_endpoint(node.get('url'))
        endpoints.append((endpoint.hostname,endpoint.port))
        require(isinstance(node.get('te_endpoint'),str), 'Explicit TE IPv4:port required')
        te=urlparse('http://'+node['te_endpoint'])
        require(te.hostname and te.port and not te.username and not te.password and not te.path
                and not te.query and not te.fragment and ipaddress.ip_address(te.hostname).version==4,
                'Invalid TE endpoint')
    require(len(set(endpoints+[(master.hostname,master.port)]))==len(endpoints)+1
            and len({n['te_endpoint'] for n in nodes})==len(nodes), 'Duplicate service/TE endpoint')
    require(master.hostname==urlparse(by_id[launch['controller']]['url']).hostname,
            'Master endpoint must belong to the configured controller node')
    for pool_id,pool in pools.items():
        require(isinstance(pool.get('owner'),str) and pool['owner'].strip()
                and isinstance(pool.get('physical_pool_id'),str) and pool['physical_pool_id'].strip(),
                'Extent owner and physical identity required')
        offset,size,align=pool.get('map_offset'),pool.get('capacity'),pool.get('mapping_alignment')
        require(type(offset) is int and type(size) is int and type(align) is int
                and offset>=0 and size>=4*1024**2 and offset+size<2**63
                and align>=4096 and align & (align-1)==0 and offset%align==0 and size%align==0,
                'Invalid exclusive extent/alignment')
        require(sum(n['pool']==pool_id for n in nodes)==len(nodes)//2, 'Each pool needs half the nodes')
    require(len({p['physical_pool_id'] for p in pools.values()})==2,
            'Independent physical pool identities required')
    if launch['profile']=='two_worker_smoke':
        require(len(nodes)==2, 'two_worker_smoke needs two nodes; services_only also supports four')
        require(isinstance(launch.get('source'),str) and launch['source'] in by_id
                and isinstance(launch.get('requester'),str) and launch['requester'] in by_id,
                'Configured smoke source/requester required')
        src,dst=by_id[launch['source']],by_id[launch['requester']]
        require(src['pool']!=dst['pool'] and all(n['can_send'] and n['can_receive'] for n in nodes),
                'Bidirectional smoke requires independent pools and both capabilities')
    return by_id[node_id],launch


def validate_node_extent(config, node_id, inv):
    selected,_=validate_launch_config(config,node_id)
    path = selected['path']
    require(path in inv['devices'] and selected['rdma_device'] in inv['hcas'],
            'Configured local DAX/HCA is absent from inventory')
    pool, geometry = config['pools'][selected['pool']], inv['geometry'].get(path, {})
    offset, size, align = pool.get('map_offset'), pool.get('capacity'), pool.get('mapping_alignment')
    require(type(offset) is int and type(size) is int and type(align) is int
            and offset >= 0 and size >= 4*1024**2 and align >= 4096 and align & (align-1) == 0
            and offset % align == 0 and size % align == 0, 'Invalid exclusive extent/alignment')
    require(geometry.get('mode') == 'devdax' and type(geometry.get('size')) is int
            and geometry.get('align') == align and offset + size <= geometry['size'],
            'Configured extent disagrees with local daxctl geometry')
    network=inv.get('network',{})
    require(network.get('status')=='PASS', 'Local IP inventory is unavailable')
    addresses=set(re.findall(r'\b(?:\d{1,3}\.){3}\d{1,3}(?=/\d+)',network.get('output','')))
    require(urlparse(selected['url']).hostname in addresses
            and urlparse('http://'+selected['te_endpoint']).hostname in addresses,
            'Configured control/TE IP is not assigned to this host')
    return selected


def wait_rpc(runtime, url, method, seconds, runner):
    deadline = time.monotonic() + seconds
    runner.say(f'[wait] {url}/{method}; deadline={seconds}s')
    last_error = None
    while time.monotonic() < deadline:
        try:
            return runtime.rpc(url, method, timeout=2)
        except (URLError, ConnectionError, TimeoutError) as error:
            last_error = str(error)
            time.sleep(1)
    raise RuntimeError(f'{url}/{method} unavailable: {last_error}; retain services and inspect both host logs')


def validate_node_traces(root, run, config, runtime):
    spec=importlib.util.spec_from_file_location('lab_trace_validator',
        source_path(root)/'scripts/validate_relay_trace.py')
    validator=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(validator)
    events=[json.loads(line) for line in (run/'state/master-None.jsonl').read_text().splitlines() if line]
    results=[]
    with (run/'payload-traces.jsonl').open('x') as log:
        for direction in ('forward','reverse'):
            rows=[json.loads(line) for line in (run/direction/'samples.jsonl').read_text().splitlines() if line]
            require(len(rows)==1 and rows[0].get('status')=='PASS', 'Missing/failed smoke sample')
            row=rows[0]
            source_id=config['lab_run']['source' if direction=='forward' else 'requester']
            source_pool=next(n['pool'] for n in config['nodes'] if n['id']==source_id)
            for node in config['nodes']:
                payload=runtime.rpc(node['url'],'payload_trace',dict(transfer_id=row['transfer_id']))
                require(isinstance(payload,list) and all(isinstance(e,dict)
                        and e.get('node')==node['id'] and e.get('sender')==node['id']
                        and node['pool']==source_pool and e.get('transfer_id')==row['transfer_id']
                        for e in payload), 'Unexpected trace response')
                for event in payload:
                    log.write(json.dumps(event)+'\n')
                events.extend(payload)
            verdict=validator.validate(events,row['transfer_id'],row['bytes'],'rdma')
            results.append(dict(direction=direction,**verdict,
                consume_rpc_ms=row['consume_rpc_ns']/1e6,local_reuse_rpc_ms=row['local_reuse_rpc_ns']/1e6))
    return results


class ConfigurationError(RuntimeError):
    def __init__(self, issues):
        self.issues = issues
        super().__init__('; '.join(issues))


def port_in_use(url):
    endpoint=urlparse(url)
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex((endpoint.hostname,endpoint.port))==0


def startup_checks(config, node_id, runner):
    """Read/open-only checks before builds; no mmap, payload write or process kill."""
    runner.stage='local-startup-validation'
    node,launch=validate_launch_config(config,node_id)
    issues=[]
    try:
        descriptor=os.open(node['path'],os.O_RDWR | os.O_CLOEXEC)
    except OSError as error:
        issues.append(f'DAX read/write open denied or unavailable: {node["path"]}: {error.strerror}. Use an authorized device account; no mapping was made')
    else:
        os.close(descriptor)
        runner.device_open_checked=True
    _,hard=resource.getrlimit(resource.RLIMIT_MEMLOCK)
    required=config['pools'][node['pool']]['capacity']
    if os.geteuid()!=0 and hard!=resource.RLIM_INFINITY and hard<required:
        issues.append(f'Memlock hard limit={hard} bytes is smaller than mapping={required}; obtain authorized locked-memory permission')
    urls=[node['url']]
    if node_id==launch['controller']:
        urls.append(config['master'])
    for url in urls:
        if port_in_use(url):
            issues.append(f'Port already occupied: {url}. Existing services are retained; use the existing campaign or inspect its owner/journal before stopping it. No process was killed')
    if issues:
        raise ConfigurationError(issues)


def check_peer_campaign(status, fingerprint, node_id):
    require(status.get('fingerprint')==fingerprint and status.get('startup_aborted') is False
            and isinstance(status.get('registered_nodes'),list),
            'coordinator campaign/version mismatch or admission closed; retain previous campaign')
    require(node_id not in status['registered_nodes'],
            'node already registered in this campaign; do not restart it or reuse its targets')


def configuration_inputs(root, args):
    """Read-only input gate. Collect independent errors without importing native code."""
    issues, config, secret = [], None, None
    config_path = Path(args.config).absolute() if args.config else None
    token_path = Path(args.token_file).absolute() if args.token_file else None
    if config_path and token_path and config_path.resolve() == token_path.resolve():
        issues.append('CONFIG and TOKEN_FILE must be distinct files')
    if not args.node_id:
        issues.append('NODE_ID is required')
    if not config_path or not config_path.is_file():
        issues.append(f'Missing CONFIG: {config_path}. Use --initialize on the configured controller')
    else:
        try:
            config = json.loads(config_path.read_text())
            if not isinstance(config, dict):
                raise ValueError('CONFIG must contain a JSON object')
        except (OSError, ValueError) as error:
            issues.append(f'Invalid CONFIG: {error}')
            config = None
    if not token_path or not token_path.is_file():
        issues.append(f'Missing TOKEN_FILE: {token_path}. Use the same private token on both hosts; --initialize creates/copies the pair')
    else:
        try:
            if token_path.is_symlink() or token_path.stat().st_mode & 0o077:
                issues.append('TOKEN_FILE must be a private regular file (mode 0600 or stricter), not a symlink')
            secret = token_path.read_text().strip()
            if len(secret) < 32:
                issues.append('TOKEN_FILE requires at least 32 characters; do not generate a different token on each host')
        except (OSError, UnicodeError):
            issues.append('TOKEN_FILE cannot be read as text')
    selected = launch = None
    if config is not None:
        # Completeness is independent of semantic validation. Report all unknown
        # template fields, not just the first null IP or device encountered.
        required = {'': ('evidence_class','abi_version','mode','path_mode','master','metadata','nodes','pools','lab_run')}
        def fields(record, prefix, names):
            for name in names:
                value = record.get(name)
                if value is None or value == '':
                    issues.append(f'CONFIG {prefix}{name} is required')
        fields(config, '', required[''])
        nodes = config.get('nodes')
        if isinstance(nodes, list):
            for i, node in enumerate(nodes):
                if isinstance(node, dict):
                    fields(node, f'nodes[{i}].', ('id','pool','url','path','te_endpoint','rdma_device','can_send','can_receive'))
            if args.node_id and not any(isinstance(n,dict) and n.get('id') == args.node_id for n in nodes):
                issues.append('NODE_ID does not select a configured node')
        pools = config.get('pools')
        if isinstance(pools, dict):
            for identity, pool in pools.items():
                if isinstance(pool, dict):
                    fields(pool, f'pools.{identity}.', ('capacity','map_offset','mapping_alignment','owner','physical_pool_id'))
        launch = config.get('lab_run')
        if isinstance(launch, dict):
            fields(launch, 'lab_run.', ('controller','profile'))
            if launch.get('profile') == 'two_worker_smoke':
                fields(launch, 'lab_run.', ('source','requester'))
        try:
            selected, launch = validate_launch_config(config, args.node_id)
            sys.path.insert(0, str(source_path(root)/'scripts'))
            importlib.import_module('callosum_runtime').config(config_path)
        except (RuntimeError, ValueError, TypeError, KeyError, AttributeError) as error:
            issues.append(f'CONFIG contract: {error}')
    if issues:
        raise ConfigurationError(issues)
    return config, selected, launch, secret


def check_delivery(root):
    """Audit the actual two-repository delivery boundary, without changing Git."""
    root = Path(root).resolve()
    source = source_path(root)
    def git(path, *argv):
        return subprocess.check_output(['git','-C',str(path),*argv],text=True).strip()
    top = Path(git(root, 'rev-parse','--show-toplevel'))
    relative = str(source.relative_to(top))
    issues = []
    pinned = git(top, 'ls-tree','HEAD','--',relative).split()
    head = git(source,'rev-parse','HEAD')
    if len(pinned) < 3 or pinned[0] != '160000' or pinned[2] != head:
        issues.append('Parent committed gitlink does not select the active Mooncake commit')
    modules = subprocess.run(['git','-C',str(top),'show','HEAD:.gitmodules'],text=True,capture_output=True)
    if modules.returncode:
        issues.append('Parent HEAD has no committed .gitmodules; fresh submodule initialization cannot reproduce this checkout')
    else:
        paths = subprocess.run(['git','config','--file','-','--get-regexp',r'submodule\..*\.path'],
                               input=modules.stdout,text=True,capture_output=True)
        if not any(line.split(maxsplit=1)[-1] == relative for line in paths.stdout.splitlines()):
            issues.append('Committed .gitmodules does not declare the Mooncake path')
    for repo, files in ((root, DELIVERY_ROOT_FILES),
            (source, SOURCE_FILES+('scripts/callosum_lab.py','scripts/run_callosum_relay.sh',
                'scripts/test_callosum_lab.py','scripts/test_callosum_benchmark_prepare.py'))):
        for file in files:
            if subprocess.run(['git','-C',str(repo),'cat-file','-e',f'HEAD:{str((repo/file).relative_to(top)) if repo==root else file}'],
                              capture_output=True).returncode:
                issues.append(f'Not committed: {repo.name}/{file}')
            elif git(repo,'status','--porcelain','--',file):
                issues.append(f'Local changes not in HEAD: {repo.name}/{file}')
    return dict(status='FAIL' if issues else 'PASS',scope='SOURCE_DELIVERY_LOCAL_ONLY',
                parent_head=git(top,'rev-parse','HEAD'),mooncake_head=head,
                parent_gitlink=pinned[2] if len(pinned)>2 else None,
                parent_gitlink_mode=pinned[0] if pinned else None,issues=issues,
                pushed='UNVERIFIED',remote_deployment='UNVERIFIED')


def check_source(root):
    """Check the active source, without claiming parent-pin reproducibility.

    The independent host commands can select a newer committed child checkout.
    This is not a reproducible parent release. Keep that distinction explicit;
    missing/modified required inputs still fail. No Git state is changed.
    """
    result=check_delivery(root)
    mismatch='Parent committed gitlink does not select the active Mooncake commit'
    warnings=[]
    if result['parent_gitlink_mode']=='160000' and mismatch in result['issues']:
        result['issues'].remove(mismatch)
        warnings.append(dict(code='PARENT_PIN_STALE',
            message='Active Mooncake checkout differs from the parent pin. '
                    'Record both revisions for this experiment; the publisher must update the pin for a reproducible release.'))
    result.update(status='FAIL' if result['issues'] else 'PASS',scope='SOURCE_CHECKOUT_LOCAL_ONLY',
        warnings=warnings,parent_release_reproducible=not warnings and not result['issues'],
        hardware_verified=False,peer_verified=False,
        next_stage='benchmark-prepare' if not result['issues'] else 'resolve-required-source-inputs')
    return result


def node_run(root, run, args, runner):
    """Independent terminals; configuration selects services and the smoke driver."""
    runner.stage='configuration-validation'
    config,selected,launch,secret=configuration_inputs(root,args)
    require(sys.platform.startswith('linux'), 'Run the node launcher on the Linux lab host')
    require(os.environ.get('CALLOSUM_DESTRUCTIVE_ACK') == '1',
            'Set CALLOSUM_DESTRUCTIVE_ACK=1 only for owned exclusive extents without outstanding DMA')
    require(not (run/'topology.json').exists(), 'Campaign already started; retain journals and do not restart')
    source = source_path(root)
    sys.path.insert(0,str(source/'scripts'))
    runtime = importlib.import_module('callosum_runtime')
    inv = inventory()
    (run/'inventory.json').write_text(json.dumps(inv,indent=2))
    runner.stage='local-device-validation'
    node = validate_node_extent(config,args.node_id,inv)
    startup_checks(config,args.node_id,runner)
    os.environ['CALLOSUM_CONTROL_TOKEN']=secret
    fingerprint=runtime.config(args.config)['fingerprint']
    if args.node_id!=launch['controller'] and port_in_use(config['master']):
        runner.stage='existing-coordinator-validation'
        check_peer_campaign(runtime.rpc(config['master'],'status',timeout=5),fingerprint,args.node_id)
    runner.call('host-preflight',['bash',root/'scripts/capture_preflight.sh','--output',run/'preflight',
        '--checkout',source,'--python',args.python,'--dax',selected['path']],clean_env())
    if not (run/'setup.json').exists():
        prepare(root,run,args,runner)
    runner.say('[extent] '+json.dumps(dict(node_id=args.node_id,path=node['path'],pool=config['pools'][node['pool']])))
    (run/'topology.json').write_text(json.dumps(config,indent=2))
    (run/'authorization.json').write_text(json.dumps(dict(confirmed=True,
        config_sha256=digest(run/'topology.json'),wall_time_ns=time.time_ns(),origin='operator_environment')))
    fd = os.open(run/'control.token',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as stream:
        stream.write(secret+'\n')
    receipt, env = load_setup(run)
    relay = source/'scripts/run_callosum_relay.sh'
    runner.call('effective-config',['bash',relay,'check','--config',run/'topology.json'],env)
    # Import only the source control client here. The child services use the new
    # native modules from the preparation receipt, not the wrapper interpreter.
    os.environ['CALLOSUM_CONTROL_TOKEN'] = secret
    wait_seconds = launch.get('wait_seconds',1800)
    runner.stage='service-readiness'
    if args.node_id != launch['controller']:
        status=wait_rpc(runtime,config['master'],'status',wait_seconds,runner)
        check_peer_campaign(status,fingerprint,args.node_id)
        start_service(root,run,'agent',args.node_id,runner)
        runner.say(f'[READY] node={args.node_id}; evidence={run}; services retained')
        return
    start_service(root,run,'master',None,runner)
    start_service(root,run,'agent',args.node_id,runner)
    runner.stage='peer-readiness'
    for peer in config['nodes']:
        if peer['id']==args.node_id:
            continue
        descriptor = wait_rpc(runtime,peer['url'],'health',wait_seconds,runner)
        require(descriptor.get('id')==peer['id'], 'Unexpected peer identity')
        # Existing idempotent register verifies live health, manifest and source ABI.
        runtime.rpc(config['master'],'register',descriptor)
    if launch['profile']=='services_only':
        runner.say(f'[READY] all configured nodes; evidence={run}; no transfer test performed')
        return
    runner.call('authorized-topology-preflight',['bash',relay,'preflight','--config',run/'topology.json'],env)
    benchmark = root/'experiments/two_worker_metadata_lookup/run.sh'
    for direction,src,dst,size,chunk in [('forward',launch['source'],launch['requester'],1048576,65536),
                                       ('reverse',launch['requester'],launch['source'],65537,4096)]:
        runner.call(direction,['bash',benchmark,'--config',run/'topology.json','--source',src,
            '--requester',dst,'--sizes-bytes',size,'--iterations','1','--warmup','0',
            '--chunk-bytes',chunk,'--output',run/direction],env)
    status = runtime.rpc(config['master'],'status')
    require(status.get('topology_verified') and len(status.get('operations',[]))==2
            and all(o['state']=='COMMITTED' for o in status['operations']), 'Incomplete smoke transfer')
    dispatch = {n['id']:runtime.rpc(n['url'],'dispatch_status') for n in config['nodes']}
    require(all(len(d)==1 and set(d.values())=={'DRAINED'} for d in dispatch.values()),
            'Each source must report one drained dispatch')
    (run/'final-status.json').write_text(json.dumps(dict(status=status,dispatch=dispatch),indent=2))
    runner.stage='trace-validation'
    results=validate_node_traces(root,run,config,runtime)
    summary=dict(status='PASS',scope='TRANSFER_ONLY',mode='real_dax_rdma',
        config_sha256=digest(run/'topology.json'),results=results,
        checks=['forward_sha256','reverse_sha256','publication','destination_local_reuse','source_dispatch_drained'],
        combined_trace_gate='PASS',
        store_compatible=False,gpu_delivery_proven=False,serving_proven=False,
        performance_baseline='NOT_RUN',active_qps_verified=False,services_retained=True,evidence=str(run))
    (run/'summary.json').write_text(json.dumps(summary,indent=2))
    runner.say(json.dumps(summary,indent=2))


def full_run(root, run, args, runner):
    require(sys.platform.startswith('linux'), 'Run this workflow on an authorized Linux lab host')
    require(not (run / 'topology.json').exists(), 'Campaign already configured; retain it and use a new authorized campaign')
    peer = args.peer or ask('Authorized peer SSH target')
    # One authenticated SSH connection for this run; retain normal host-key checks.
    control_dir = Path(tempfile.mkdtemp(prefix='callosum-ssh-'))
    ssh_options = ['-o','ControlMaster=auto','-o','ControlPersist=300',
                   '-o','ControlPath='+str(control_dir/'socket')]
    def remote(command, tty=False):
        return remote_command(peer,command,tty,ssh_options)
    def copy(command):
        return ['scp'] + ssh_options + list(map(str,command))
    remote_root = args.remote_root or str(root)
    remote_run = str(Path(remote_root) / 'runs' / run.name)
    require(Path(remote_root).is_absolute() and re.fullmatch(r'/[A-Za-z0-9_./-]+',remote_root),
            'Remote root must be a simple absolute path without shell metacharacters')
    runner.call('ssh-access', remote(['test','-f', str(Path(remote_root)/'Mooncake-dev/CMakeLists.txt')]))
    runner.call('remote-runs-parent', remote(['mkdir','-p',str(Path(remote_root)/'runs')]))
    runner.call('remote-run-directory', remote(['mkdir',remote_run]))
    remote_script = str(Path(remote_run) / 'callosum_lab.py')
    runner.call('copy-single-script', copy([Path(__file__).resolve(), f'{peer}:{remote_script}']))
    remote_base = ['python3', remote_script, '--root', remote_root, '--run-dir', remote_run]
    (run / 'peer.json').write_text(json.dumps(dict(peer=peer,root=remote_root,run=remote_run,script=remote_script)))
    if not (run/'setup.json').exists():
        prepare(root, run, args, runner)
    remote_options = ['--jobs', str(args.jobs)] + (['--skip-apt'] if args.skip_apt else [])
    runner.call('remote-prepare', remote(remote_base + remote_options + ['--action','prepare'], tty=True))
    local_receipt = json.loads((run/'setup.json').read_text())
    remote_receipt = json.loads(runner.call('remote-setup-info', remote(
        remote_base + ['--action','info']), capture=True))
    require(local_receipt['source_hashes'] == remote_receipt['source_hashes'],
            'Source hashes differ across hosts; no DAX services started')
    remote_inv = json.loads(runner.call('remote-inventory', remote(
        remote_base + ['--action','inventory']), capture=True))
    runner.stage = 'exclusive-extent-authorization'
    local_inv = inventory()
    require(local_inv['host'] != remote_inv['host'], 'SSH target is the same host; two physical workers are required')
    c = manifest(host_fields('Local node', local_inv), host_fields('Peer node', remote_inv))
    print('\nPROPOSED EXCLUSIVE DAX WRITE EXTENTS:\n' + json.dumps(c, indent=2))
    require(ask('Confirm independent physical devices and exclusive extents with NO outstanding DMA/quarantine. Type AUTHORIZE DAX WRITES')
            == 'AUTHORIZE DAX WRITES', 'No write authorization; no DAX services started')
    (run/'topology.json').write_text(json.dumps(c, indent=2))
    (run/'authorization.json').write_text(json.dumps(dict(confirmed=True, wall_time_ns=time.time_ns(),
        config_sha256=digest(run/'topology.json')), indent=2))
    secret = run/'control.token'
    fd = os.open(secret, os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)
    with os.fdopen(fd,'w') as stream:
        stream.write(secrets.token_hex(32)+'\n')
    runner.call('copy-private-config', copy([run/'topology.json',run/'authorization.json',secret,f'{peer}:{remote_run}/']))
    runner.call('remote-token-permission', remote(['chmod','600',str(Path(remote_run)/'control.token')]))
    start_service(root,run,'master',None,runner)
    start_service(root,run,'agent','a1',runner)
    runner.call('remote-agent-start',remote(remote_base+['--action','start-b1']))
    receipt, env = load_setup(run)
    relay = source_path(root)/'scripts/run_callosum_relay.sh'
    runner.call('authorized-topology-preflight',['bash',relay,'preflight','--config',run/'topology.json'],env)
    benchmark = Path(root)/'experiments/two_worker_metadata_lookup/run.sh'
    for direction,src,dst,size,chunk in [('forward','b1','a1',1048576,65536),
                                         ('reverse','a1','b1',65537,4096)]:
        runner.call(direction,['bash',benchmark,'--config',run/'topology.json','--source',src,
            '--requester',dst,'--sizes-bytes',size,'--iterations','1','--warmup','0',
            '--chunk-bytes',chunk,'--output',run/direction],env)
    final_status = json.loads(runner.call('final-status',
        ['bash',relay,'status','--config',run/'topology.json'],env,capture=True))
    (run/'final-status.json').write_text(json.dumps(final_status,indent=2))
    require(final_status.get('topology_verified') and len(final_status.get('operations',[]))==2
            and all(o['state']=='COMMITTED' for o in final_status['operations']),
            'Both smoke transfers must be committed; retain any uncertain operations')
    traces=run/'remote-trace'
    traces.mkdir()
    runner.call('collect-remote-trace',copy([f'{peer}:{remote_run}/state/agent-b1.jsonl',str(traces)+'/']))
    runner.stage = 'rdma-trace-validation'
    spec=importlib.util.spec_from_file_location('lab_trace_validator',source_path(root)/'scripts/validate_relay_trace.py')
    validator=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(validator)
    paths=[run/'state/master-None.jsonl',run/'state/agent-a1.jsonl',traces/'agent-b1.jsonl']
    events=[json.loads(line) for p in paths for line in p.read_text().splitlines() if line]
    results=[]
    for direction in ('forward','reverse'):
        rows=[json.loads(line) for line in (run/direction/'samples.jsonl').read_text().splitlines() if line]
        require(rows and all(r.get('status')=='PASS' for r in rows), 'Missing or failed benchmark samples')
        for row in rows:
            verdict=validator.validate(events,row['transfer_id'],row['bytes'],'rdma')
            results.append(dict(direction=direction,**verdict,consume_rpc_ms=row['consume_rpc_ns']/1e6,
                                local_reuse_rpc_ms=row['local_reuse_rpc_ns']/1e6))
    summary=dict(status='PASS',scope='TRANSFER_ONLY',mode='real_dax_rdma',results=results,
        config_sha256=digest(run/'topology.json'),
        store_compatible=False,serving_proven=False,gpu_delivery_proven=False,
        performance_baseline='NOT_RUN',active_qps_verified=False,services_retained=True)
    (run/'summary.json').write_text(json.dumps(summary,indent=2))
    runner.call('copy-success-proof',copy([run/'summary.json',f'{peer}:{remote_run}/summary.json']))
    runner.say('\n'+json.dumps(summary,indent=2))
    runner.say(f'[PASS] bidirectional RDMA smoke test. Evidence: {run}')
    runner.say('Services remain alive. Do not reuse the extents or delete their journals.')


def main(argv=None):
    parser=argparse.ArgumentParser(description=
        'Callosum host-local preparation and TRANSFER_ONLY smoke gate. Select NODE_ID from CONFIG.')
    parser.add_argument('--root',default=str(Path.cwd()),help='Callosum root containing Mooncake-dev')
    parser.add_argument('--peer',help='Authorized peer SSH target; prompted if omitted')
    parser.add_argument('--remote-root',help='Peer checkout root; default: same absolute path')
    parser.add_argument('--run-dir',help='Evidence/campaign directory; default: fresh runs/rdma-*')
    parser.add_argument('--python',default=sys.executable,help='Host interpreter used to create a fresh venv')
    parser.add_argument('--jobs',type=int,default=4)
    parser.add_argument('--skip-apt',action='store_true',help='Refuse missing packages instead of installing them')
    parser.add_argument('--action',choices=['run','node','prepare','inventory','info','start-b1','stop'],default='run')
    parser.add_argument('--node-id',default='',help='Select a node record; hostnames never select behavior')
    parser.add_argument('--config',help='Approved common manifest for independent-terminal node mode')
    parser.add_argument('--token-file',help='Private shared control token file; never place the token in argv')
    parser.add_argument('--initialize',action='store_true',
        help='Prepare/copy shared configuration through authorized SSH, then STOP; no services or DAX mapping')
    parser.add_argument('--check-config',action='store_true',
        help='Check manifest/token and node selection only; no build, SSH, service, inventory or device access')
    parser.add_argument('--check-delivery',action='store_true',
        help='Check active source inputs; stale parent pin is a warning. Use --strict-pin for a release audit')
    parser.add_argument('--strict-pin',action='store_true',
        help='With --check-delivery, require the committed parent pin to match the active child checkout')
    parser.add_argument('--check-source',action='store_true',
        help='Alias for the default --check-delivery source check; no Git changes')
    parser.add_argument('--transport-sweep',action='store_true',
        help='Use a retained healthy campaign for a CXL WRITE sweep; no build or service restart')
    parser.add_argument('--setup-run',help='Existing successful campaign for --transport-sweep')
    parser.add_argument('--source',help='Sweep source node ID; default: campaign source')
    parser.add_argument('--receiver',help='Sweep receiver node ID; default: campaign requester')
    parser.add_argument('--sizes',nargs='+',type=int,default=[1,2,4,8,16,32,64])
    parser.add_argument('--unit',choices=['MB','MiB'],default='MiB')
    parser.add_argument('--repetitions',type=int,default=1)
    args=parser.parse_args(argv)
    root=Path(args.root).resolve()
    require(sum((args.initialize,args.check_config,args.check_delivery,args.check_source,args.transport_sweep))<=1,
            'Use only one of --initialize, --check-config, --check-delivery, --check-source or --transport-sweep')
    require(not (args.initialize or args.check_config) or args.action=='node',
            '--initialize and --check-config require --action node; do not combine them with build/prepare actions')
    require(not args.strict_pin or args.check_delivery,'--strict-pin requires --check-delivery')
    if args.check_delivery or args.check_source:
        result=(check_delivery if args.strict_pin else check_source)(root)
        print(json.dumps(result,indent=2))
        return 0 if result['status']=='PASS' else 1
    if args.transport_sweep:
        source_path(root)
        return transport_sweep(root,args)
    if args.action=='inventory':
        print(json.dumps(inventory()))
        return 0
    run=Path(args.run_dir).resolve() if args.run_dir else None
    if args.action=='info':
        require(run is not None,'--run-dir required')
        print((run/'setup.json').read_text())
        return 0
    if args.action=='stop':
        require(run is not None,'--run-dir required')
        summary=json.loads((run/'summary.json').read_text())
        receipt, env=load_setup(run)
        status=json.loads(subprocess.check_output(['bash',source_path(root)/'scripts/run_callosum_relay.sh',
            'status','--config',run/'topology.json'],env=env,text=True))
        check_stop_gate(summary,status,digest(run/'topology.json'))
        peer_file=run/'peer.json'
        if peer_file.exists():
            peer=json.loads(peer_file.read_text())
            code=subprocess.call(remote_command(peer['peer'],['python3',peer['script'],
                '--root',peer['root'],'--run-dir',peer['run'],'--action','stop']))
            require(code==0,'Remote stop failed; retain all resources')
        print(json.dumps(stop_owned(run),indent=2))
        return 0
    require(args.jobs>0,'--jobs must be positive')
    source_path(root)
    if run is None:
        (root/'runs').mkdir(exist_ok=True)
        run=Path(tempfile.mkdtemp(prefix='rdma-',dir=root/'runs'))
    runner=Runner(run)
    try:
        if args.action=='prepare':
            if (run/'setup.json').exists():
                runner.say('[PASS] preparation receipt exists; do not rebuild under live services')
            else:
                prepare(root,run,args,runner)
        elif args.action=='node':
            if args.check_config:
                runner.stage='configuration-validation'
                config,node,launch,_=configuration_inputs(root,args)
                result=dict(status='PASS',scope='CONFIG_ONLY',node_id=node['id'],pool=node['pool'],
                    starts_master=node['id']==launch['controller'],profile=launch['profile'],
                    config_sha256=digest(args.config),hardware_verified=False,
                    next_stage='host-preflight',services_started=False,devices_accessed=False)
                (run/'configuration-check.json').write_text(json.dumps(result,indent=2))
                runner.say(json.dumps(result,indent=2))
            elif args.initialize:
                initialize_configuration(root,run,args,runner)
            else:
                node_run(root,run,args,runner)
        elif args.action=='start-b1':
            start_service(root,run,'agent','b1',runner)
        else:
            full_run(root,run,args,runner)
        return 0
    except (Exception,KeyboardInterrupt) as error:
        started=runner.services_may_be_live or any(run.glob('*.process.json'))
        cleanup=rollback_empty_startup(root,run,runner)
        unused_stopped=cleanup['status']=='STOPPED_EMPTY_COORDINATOR'
        failure=dict(status='FAIL',stage=runner.stage,error=str(error),evidence=str(run),
            startup_cleanup=cleanup,
            issues=getattr(error,'issues',[str(error)]),
            local_services_started_this_run=started,
            side_effects=dict(evidence_directory_created=True,
                configuration=runner.configuration_effects,commands_started=runner.commands_started,
                device_open_checked=runner.device_open_checked,
                device_access_possible=started),
            retry_after_input_correction=not started or (unused_stopped and set(runner.children)=={'master'}),
            action=('The unused owned coordinator was stopped after admission was sealed. Keep all evidence; no mappings or arenas were reclaimed. Correct the startup error; earlier campaigns still require lifetime checks.' if unused_stopped else
                    'Retain all services, mappings, logs and journals. No automatic retry/reclaim.' if started
                    else 'This invocation started no local service and mapped no DAX. Correct all listed inputs and rerun. Preserve any existing configuration; earlier campaigns require a separate lifetime check.'))
        (run/'failure.json').write_text(json.dumps(failure,indent=2))
        runner.say('\n'+json.dumps(failure,indent=2))
        return 1
    finally:
        runner.log.close()


if __name__=='__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('[FAIL] '+str(error),file=sys.stderr)
        sys.exit(1)
