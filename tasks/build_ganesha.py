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
    testdir = teuthology.get_testdir(ctx)
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

    build_gsh = ['cd', testdir, '&&', 'git', 'clone', repo, '-b', branch]
    go_to_nfsdir = ['pushd', 'nfs-ganesha']
    git_checkout_cmd = ['git', 'checkout', '${commit}', '-b', '"working-{commit}"']
    
    # the latest dev-2.3 still has the ntirpc submodule--this
    # shoudn't stop working, though
    git_submodule_cmd = ['git', 'submodule', 'update', '--init', '--recursive']
    go_to_builddir = ['mkdir', 'build', '&&', 'pushd', 'build']
    
    # build ceph with only Ceph FSAL support--we won't be using VFS,
    # so save a few cycles
    cmake_command = ['cmake', 
        '-DCMAKE_INSTALL_PREFIX="{gsh_prefix}"',
        '-DUSE_FSAL_CEPH=ON',
        '-DUSE_FSAL_PROXY=OFF',
        '-DUSE_FSAL_GPFS=OFF',
        '-DUSE_FSAL_ZFS=OFF',
        '-DUSE_FSAL_LUSTRE=ON',
        '-DUSE_FSAL_XFS=OFF',
        '-DUSE_FSAL_VFS=OFF',
        '-DUSE_FSAL_PANFS=OFF',
        '-DUSE_FSAL_GLUSTER=OFF',
        '-DCEPH_PREFIX="{ceph_prefix}"',
        '-DCMAKE_C_FLAGS=\"-O2', '-g', '-gdwarf-4"',
        '../src']
    make_cmd = ['make', '&&', 'make', 'install']

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
            remote[0].run(
                args=[
                    'rm', '-rf', '--', 'testdir/next',
                ],
            )
            pass
