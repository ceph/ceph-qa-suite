"""
build_ganesha task -- check out, build, and install
"""
import logging
import pipes
import os

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
pushd nfs-ganesha
git checkout ${commit} -b "working-{commit}"

# the latest dev-2.3 still has the ntirpc submodule--this
# shoudn't stop working, though
git submodule update --init --recursive
mkdir build
pushd build

CEPH_PREFIX="/cache/ceph-rgw"
GANESHA_PREFIX="/opt/ganesha"

# build ceph with only Ceph FSAL support--we won't be using VFS,
# so save a few cycles
cmake -DCMAKE_INSTALL_PREFIX="{gsh_prefix}"  \
    -DUSE_FSAL_CEPH=ON \
    -DUSE_FSAL_PROXY=OFF \
    -DUSE_FSAL_GPFS=OFF \
    -DUSE_FSAL_ZFS=OFF \
    -DUSE_FSAL_LUSTRE=ON \
    -DUSE_FSAL_XFS=OFF \
    -DUSE_FSAL_VFS=OFF \
    -DUSE_FSAL_PANFS=OFF \
    -DUSE_FSAL_GLUSTER=OFF \
    -DCEPH_PREFIX="{ceph_prefix}" \
    -DCMAKE_C_FLAGS="-O2 -g -gdwarf-4" \
    ../src

make && make install
""".format(repo=repo,branch=branch,commit=commit,gsh_prefix=gsh_prefix,ceph_prefix=ceph_prefix)

    clients = ctx.cluster.only(teuthology.is_type('client'))
    for remote in clients.remotes.iteritems():
        try:
            remote.run(args=[build_gsh])

        finally:
            pass
