"""
Set up client keyring
"""
import logging
import contextlib

from teuthology import misc as teuthology
from teuthology.orchestra import remote, run
#from teuthology import contextutil

log = logging.getLogger(__name__)


def create_keyring(ctx):
    """
    Set up key ring on remote sites
    """
    log.info('Setting up client nodes...')
    clients = ctx.cluster.only(teuthology.is_type('client'))
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    for remote, roles_for_host in clients.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            client_keyring = '/etc/ceph/ceph.client.{id}.keyring'.format(id=id_)
            remote.run(
                args=[
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-authtool',
                    '--create-keyring',
                    '--gen-key',
                    # TODO this --name= is not really obeyed, all unknown "types" are munged to "client"
                    '--name=client.{id}'.format(id=id_),
                    client_keyring,
                    run.Raw('&&'),
                    'sudo',
                    'chmod',
                    '0644',
                    client_keyring,
                    ],
                )


@contextlib.contextmanager
def task(ctx, config):
    testdir = teuthology.get_testdir(ctx)
    log.info('Setting up client nodes...')
    conf_path = '/etc/ceph/ceph.conf'
    #admin_keyring_path = '/etc/ceph/ceph.client.admin.keyring'
    admin_keyring_path = config.get('keyring_path', '/etc/ceph/ceph.keyring')
    if isinstance(config, dict):
        # If this task is being run on an existing cluster, mon0 needs to have
        # been specified in its config. If the task is not given a config,
        # assume we're running in the conventional mode.
        first_mon = config.get('mon0')
        if not first_mon:
            raise RuntimeError("This task needs 'mon0' specified")
        mon0_remote = remote.Remote(name=first_mon)
        ctx.cluster.add(mon0_remote, ['mon0'])
    else:
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon0_remote,) = ctx.cluster.only(first_mon).remotes.keys()
    conf_data = teuthology.get_file(
        remote=mon0_remote,
        path=conf_path,
        sudo=True,
        )
    admin_keyring = teuthology.get_file(
        remote=mon0_remote,
        path=admin_keyring_path,
        sudo=True,
        )

    clients = ctx.cluster.only(teuthology.is_type('client'))
    for remot, roles_for_host in clients.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            client_keyring = \
                '/etc/ceph/ceph.client.{id}.keyring'.format(id=id_)
            mon0_remote.run(
                args=[
                    'cd',
                    '{tdir}'.format(tdir=testdir),
                    run.Raw('&&'),
                    'sudo', 'bash', '-c',
                    run.Raw('"'), 'ceph',
                    'auth',
                    'get-or-create',
                    'client.{id}'.format(id=id_),
                    'mds', 'allow',
                    'mon', 'allow *',
                    'osd', 'allow *',
                    run.Raw('>'),
                    client_keyring,
                    run.Raw('"'),
                    ],
                )
            key_data = teuthology.get_file(
                remote=mon0_remote,
                path=client_keyring,
                sudo=True,
                )
            teuthology.sudo_write_file(
                remote=remot,
                path=client_keyring,
                data=key_data,
                perms='0644'
            )
            teuthology.sudo_write_file(
                remote=remot,
                path=admin_keyring_path,
                data=admin_keyring,
                perms='0644'
            )
            teuthology.sudo_write_file(
                remote=remot,
                path=conf_path,
                data=conf_data,
                perms='0644'
            )
