"""
build_ganesha task -- check out, build, and install
"""
import logging
import contextlib
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Check-out, build, and install nfs-ganesha.

    For example::

        tasks:
        - ceph:
        - build_ganesha:
            branch: next
            commit-sha: "9427e31caa80acd96e0f2941d4f7dbf42764b9d7"

    :param ctx: Context
    :param config: Configuration
    """
    assert isinstance(config, dict)

    # cons up a build script and run it

    repo = 'https://github.com/nfs-ganesha/nfs-ganesha.git'
    branch="next"
    commit="9427e31caa80acd96e0f2941d4f7dbf42764b9d7"
    gsh_prefix="/opt/ganesha" # where to install ganesha
    ceph_prefix="/usr/local" # where this process installed (?) ceph

    if config.get('branch') is not None:
        branch = config.get('branch')

    if config.get('commit-sha') is not None:
        commit = config.get('commit-sha')

    build_gsh="""
git clone {repo} -b {branch}
""".format(repo=repo,branch=branch,commit=commit,gsh_prefix=gsh_prefix,ceph_prefix=ceph_prefix)

    clients = ctx.cluster.only(teuthology.is_type('client'))
    log.debug('clients is %r', clients)
    for remote in clients.remotes.iteritems():
        log.debug('remote is %r', remote)
        try:
           remote[0].run(
               args=[
                   build_gsh,
               ],
           )

        finally:
            pass
