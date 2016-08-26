import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)


def setup_installer(ctx, config):
    """
     Setup a installer node that acts as a ceph-ansible installer
     for other nodes, CAN"T mix RPM/DEB systems
     Assumes all nodes are either RPM or DEB systems.

    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('setup-installer', {}))
    (ceph_installer,) = ctx.cluster.only(
        teuthology.get_first_mon(ctx, config)).remotes.iterkeys()
    if ceph_installer.os.package_type == 'rpm':
        if config.get('rhscon_repo'):
            log.info("Setting up the rhscon repo")
            rhscon_repo = config.get('rhscon_repo')
            ctx.cluster.run(args=['sudo', 'rm', run.Raw('/etc/yum.repos.d/*')],
                            check_status=False)
            #remove old systemd files, known issue
            ctx.cluster.run(args=['sudo', 'rm', '-rf',
                                  run.Raw('/etc/systemd/system/ceph*')],
                            check_status=False)
            ctx.cluster.run(args=['sudo', 'rm', '-rf',
                                  run.Raw('/etc/systemd/system/multi-user.target.wants/ceph*')],
                            check_status=False)
            ceph_installer.run(
                    args=[
                        'sudo',
                        'wget',
                        '-nv',
                        '-O',
                        '/etc/yum.repos.d/rh_ceph_rhscon.repo',
                        rhscon_repo])
            ceph_installer.run(args=['sudo', 'yum', 'clean', 'metadata'])
            ceph_installer.run(args=['sudo', 'yum', 'install', '-y',
                                     'ceph-ansible'])
        else:
            # repo is set by internal tasks
            ceph_installer.run(args=['sudo', 'yum', 'clean', 'metadata'])
            ceph_installer.run(args=['sudo', 'yum', 'install', '-y',
                                     'ceph-ansible'])
        if config.get('rhbuild_repo'):
            log.info("Setting up the rhbuild repo")
            rhbuild_repo = config.get('rhbuild_repo')
            ctx.cluster.run(
                        args=[
                            'sudo',
                            'wget',
                            '-nv',
                            '-O',
                            '/etc/yum.repos.d/rh_ceph.repo',
                            rhbuild_repo])
            ctx.cluster.run(args=['sudo', 'yum', 'clean', 'metadata'])
    else:
        ceph_installer.run(args=['sudo', 'apt-get', 'install',
                                 '-y', 'ceph-ansible'])
        # more work to do
