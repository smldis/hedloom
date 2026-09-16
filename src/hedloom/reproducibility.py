"""Compact reproducibility records with explicitly reusable environment captures."""
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256, new as new_hash
from importlib import metadata
from pathlib import Path
from urllib.parse import unquote, urlsplit
import base64
import inspect
import json
import os
import platform
import subprocess
import sys


@dataclass(frozen=True)
class Reproducibility:
    """Submission evidence; supplied files are copied, never executed.

    Relative paths use the submission working directory. The dependency
    environment is cached by the session; source and supplied files are fresh.
    """

    enabled: bool = True
    project_root: str | Path | None = None
    text: str = ""
    files: tuple[str | Path, ...] = ()


@dataclass(frozen=True)
class EnvironmentSnapshot:
    """Immutable captured environment. ``to_data`` returns a detached copy."""

    _json: str

    def to_data(self):
        return json.loads(self._json)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _project_root(value=None):
    # A cache lookup must not stat a network-mounted directory.
    return Path(os.path.abspath(value if value is not None else os.getcwd()))


def _options(value):
    value = Reproducibility() if value is None else value
    if not isinstance(value, Reproducibility):
        raise TypeError('reproducibility must be a Reproducibility instance or None')
    if not isinstance(value.text, str):
        raise TypeError('reproducibility text must be a string')
    return value


def _git(directory, *args):
    return subprocess.run(
        ['git', '-C', str(directory), *args], check=True, capture_output=True,
        timeout=10,
    ).stdout


def _snapshot(path, files, gaps, *, required=False):
    path = Path(path).resolve()
    if str(path) in files:
        return
    try:
        payload = path.read_bytes()
    except OSError as error:
        if required:
            raise ValueError(f'cannot capture reproducibility file {path}: {error}') from error
        gaps.append(f'cannot capture {path}: {error}')
        return
    files[str(path)] = {'sha256': sha256(payload).hexdigest(), 'encoding': 'base64',
                        'content': base64.b64encode(payload).decode('ascii')}


def capture_environment(*, project_root=None):
    """Discover dependencies once. Reuse explicitly until the environment changes.

    Reads installed versions, the nearest project manifest, editable package
    manifests and Git revisions/patches. No lockfiles or network queries.
    """
    captured_at = _now()
    root = _project_root(project_root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError('project_root must be a directory')
    gaps, files, repositories = [], {}, {}
    directories = {root}
    # The nearest project declaration is sufficient; do not collect all ancestors.
    for ancestor in (root, *root.parents):
        manifest = ancestor / 'pyproject.toml'
        if manifest.is_file():
            _snapshot(manifest, files, gaps)
            break

    packages = []
    for dist in metadata.distributions():
        package = {'name': dist.metadata['Name'], 'version': dist.version}
        try:
            direct = dist.read_text('direct_url.json')
            if direct:
                origin = json.loads(direct)
                package['direct_url'] = origin
                if origin.get('dir_info', {}).get('editable'):
                    url = urlsplit(origin.get('url', ''))
                    if url.scheme == 'file' and url.netloc in ('', 'localhost'):
                        directory = Path(unquote(url.path)).resolve()
                        directories.add(directory)
                        manifest = directory / 'pyproject.toml'
                        if manifest.is_file():
                            _snapshot(manifest, files, gaps)
                    else:
                        gaps.append(f'cannot locate editable distribution {package["name"]}')
        except (OSError, ValueError) as error:
            gaps.append(f'cannot read installation origin for {package["name"]}: {error}')
        packages.append(package)

    for directory in sorted(directories):
        try:
            repo = _git(directory, 'rev-parse', '--show-toplevel').decode().strip()
            if repo in repositories:
                continue
            commit = _git(repo, 'rev-parse', 'HEAD').decode().strip()
            status = _git(repo, 'status', '--porcelain=v1', '--untracked-files=all')
            patch = _git(repo, 'diff', '--binary', '--no-ext-diff', '--no-textconv', 'HEAD', '--') if status else b''
            repositories[repo] = {
                'commit': commit, 'dirty': bool(status),
                'tracked_patch_base64': base64.b64encode(patch).decode('ascii'),
            }
            if status:
                gaps.append(f'dirty repository {repo}: tracked patch saved; untracked files only saved when explicitly captured')
        except (OSError, subprocess.SubprocessError) as error:
            gaps.append(f'Git revision unavailable for {directory}: {type(error).__name__}')

    return EnvironmentSnapshot(json.dumps({
        'captured_at': captured_at, 'scope': 'submit-host',
        'python_version': platform.python_version(), 'project_root': str(root),
        'packages': sorted(packages, key=lambda item: (item['name'] or '', item['version'])),
        'files': files, 'repositories': repositories, 'gaps': gaps,
    }, sort_keys=True))


def _study_sources(paths, files, gaps):
    """Observe Git anew for each submission; reference only exact clean blobs."""
    repositories, directories, references = {}, {}, {}
    for source in sorted(paths):
        path = Path(source).resolve()
        try:
            directory = str(path.parent)
            if directory not in directories:
                directories[directory] = _git(path.parent, 'rev-parse', '--show-toplevel').decode().strip()
            repo = directories[directory]
            if repo not in repositories:
                commit = _git(repo, 'rev-parse', 'HEAD').decode().strip()
                dirty = bool(_git(repo, 'status', '--porcelain=v1', '--untracked-files=all'))
                repositories[repo] = {'commit': commit, 'dirty': dirty}
            observation = repositories[repo]
            relative = path.relative_to(repo).as_posix()
            if not observation['dirty'] and str(path) not in files:
                tree = _git(repo, 'ls-tree', '-z', observation['commit'], '--', relative)
                if tree:
                    entry, stored_path = tree.rstrip(b'\0').split(b'\t', 1)
                    mode, kind, oid = entry.decode().split()
                    if mode in ('100644', '100755') and kind == 'blob' and stored_path == os.fsencode(relative):
                        payload = path.read_bytes()
                        algorithm = 'sha256' if len(oid) == 64 else 'sha1'
                        actual = new_hash(algorithm, b'blob ' + str(len(payload)).encode() + b'\0' + payload).hexdigest()
                        if actual == oid:
                            references[str(path)] = {'repository': repo, 'commit': observation['commit'],
                                                     'path': relative, 'blob': oid}
                            continue
                        observation['dirty'] = True
        except (OSError, ValueError, subprocess.SubprocessError):
            pass  # Git is optional: preserve source bytes when references cannot suffice.
        _snapshot(path, files, gaps)
    for repo, observation in repositories.items():
        if observation['dirty']:
            gaps.append(f'dirty study repository {repo}: available source bytes saved; other local files are not automatically archived')
    return references, repositories


def capture(options=None, implementations=(), *, environment=None):
    """Compose cached dependency evidence with fresh study/configuration bytes."""
    options = _options(options)
    if not options.enabled:
        return {'status': 'disabled'}
    if environment is None:
        environment = capture_environment(project_root=options.project_root)
    if not isinstance(environment, EnvironmentSnapshot):
        raise TypeError('environment must be an EnvironmentSnapshot or None')
    dependency_data = environment.to_data()
    gaps, files = [], {}
    for path in options.files:
        _snapshot(path, files, gaps, required=True)
    sources = set()
    source = getattr(sys.modules.get('__main__'), '__file__', None)
    if source and Path(source).is_file():
        sources.add(str(Path(source).resolve()))
    for body in implementations:
        try:
            source = inspect.getsourcefile(inspect.unwrap(body))
        except TypeError:
            source = None
        if source and Path(source).is_file():
            sources.add(str(Path(source).resolve()))
        else:
            gaps.append(f'no source file for {getattr(body, "__qualname__", type(body).__name__)}')
    references, repositories = _study_sources(sources, files, gaps)
    all_gaps = dependency_data['gaps'] + gaps
    return {
        'status': 'partial' if all_gaps else 'captured', 'captured_at': _now(),
        'environment': dependency_data, 'cwd': os.getcwd(), 'argv': list(sys.argv),
        'text': options.text, 'files': files, 'sources': references,
        'study_repositories': repositories, 'gaps': all_gaps,
    }
