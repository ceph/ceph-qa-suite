import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
     Setup downstream repo's thats already released to customers
     rhbuild:
        1.3.1
     repo:
        2.0: 'repo_url'

    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('set-repo', {}))

    supported_repos = {'1.3.1': 'https://paste.fedoraproject.org/350766/14600017/raw/',
                       '1.3.2': 'https://paste.fedoraproject.org/350766/14600017/raw/',
                       }
    if config.get('repo'):
        log.info("Updating repo")
        repo = config.get('repo')
        supported_repos.update(repo)
    log.info("Backing up current repo's and disable firewall")
    for remote in ctx.cluster.remotes.iterkeys():
        if remote.os.package_type == 'rpm':
            remote.run(args=['mkdir', 'repo'], check_status=False)
            remote.run(args=['sudo', 'cp', run.Raw('/etc/yum.repos.d/*'), 'repo/'])
            remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
            remote.run(args=['sudo', 'systemctl', 'stop', 'firewalld'], check_status=False)

    build = config.get('rhbuild')
    repo_url = supported_repos[build]
    log.info("Setting the repo for build %s", build)
    for remote in ctx.cluster.remotes.iterkeys():
        if remote.os.package_type == 'rpm':
            remote.run(
                args=[
                    'sudo',
                    'wget',
                    '-nv',
                    '-O',
                    '/etc/yum.repos.d/rh_ceph.repo',
                    repo_url])
            remote.run(args=['sudo', 'yum', 'clean', 'metadata'])

    try:
        yield
    finally:
        log.info("Resotring repo's")
        for remote in ctx.cluster.remotes.iterkeys():
            if remote.os.package_type == 'rpm':
                remote.run(args=['sudo', 'cp', run.Raw('repo/*'), '/etc/yum.repos.d/'])
                remote.run(args=['sudo', 'rm', '/etc/yum.repos.d/rh_ceph.repo'])
                remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
                remote.run(args=['sudo', 'rm', '-rf', 'repo'])
