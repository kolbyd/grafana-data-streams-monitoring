"""Package a Webpack/Mage build with executable permissions and source checks."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import zipfile


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--plugin-dir', type=Path, default=Path('kolbyd-datastreamsmonitoring-app'))
parser.add_argument('--output', type=Path, default=Path('artifacts'))
parser.add_argument('--tag', help='Require an exact v<package.json version> release tag')
parser.add_argument('--source-ref', help='Include plugin-only and complete source ZIPs from this Git ref')
args = parser.parse_args()
plugin = args.plugin_dir.resolve()
dist = plugin / 'dist'
metadata = json.loads((dist / 'plugin.json').read_text(encoding='utf-8'))
package = json.loads((plugin / 'package.json').read_text(encoding='utf-8'))
plugin_id, version = metadata['id'], metadata['info']['version']
if not re.fullmatch(r'[a-z0-9]+-[a-z0-9]+-app', plugin_id):
    raise SystemExit('Unexpected plugin ID')
if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?', version):
    raise SystemExit('Build metadata must contain a resolved version')
if version != package['version'] or (args.tag and args.tag != 'v' + version):
    raise SystemExit('Build, package and release tag versions must match')
binary = dist / 'gpx_data_streams_monitoring_linux_amd64'
if binary.read_bytes()[:4] != b'\x7fELF':
    raise SystemExit('Expected the SDK Mage Linux AMD64 backend')
manifest = dist / 'go_plugin_build_manifest'
for line in manifest.read_text(encoding='utf-8').splitlines():
    digest, relative = line.split(':', 1)
    source = (plugin / relative).resolve()
    if not source.is_relative_to(plugin) or hashlib.sha256(source.read_bytes()).hexdigest() != digest:
        raise SystemExit('Go build manifest differs from source: ' + relative)
shutil.copyfile(plugin / 'THIRD_PARTY_NOTICES.txt', dist / 'THIRD_PARTY_NOTICES.txt')
args.output.mkdir(parents=True, exist_ok=True)
archive = args.output / f'{plugin_id}-{version}-linux-amd64.zip'
with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
    for file in sorted(dist.rglob('*')):
        if file.is_symlink():
            raise SystemExit('Distribution must not contain symlinks')
        if not file.is_file():
            continue
        info = zipfile.ZipInfo.from_file(file, plugin_id + '/' + file.relative_to(dist).as_posix())
        info.create_system = 3
        info.external_attr = (0o100755 if file.name.startswith('gpx_') else 0o100644) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        bundle.writestr(info, file.read_bytes())
archives = [archive]
if args.source_ref:
    repo = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
    relative = plugin.relative_to(repo).as_posix()
    for name, tree in [(f'{plugin_id}-{version}-source.zip', args.source_ref + ':' + relative),
                       (f'grafana-data-streams-monitoring-{version}-source.zip', args.source_ref)]:
        target = args.output.resolve() / name
        subprocess.run(['git', 'archive', '--format=zip', '--output=' + str(target), tree], cwd=repo, check=True)
        archives.append(target)
for file in archives:
    for algorithm in ['sha1', 'sha256']:
        file.with_name(file.name + '.' + algorithm).write_text(
            hashlib.new(algorithm, file.read_bytes()).hexdigest() + '\n', encoding='utf-8')
print(json.dumps({'archive': str(archive), 'id': plugin_id, 'version': version, 'sourceArchives': len(archives) - 1}))
