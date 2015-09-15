"""
Ganesha
"""
import contextlib
import logging
import sys
import time

from teuthology import misc as teuthology
from teuthology.orchestra import run
from teuthology.orchestra.daemon import DaemonGroup

log = logging.getLogger(__name__)


def get_gsh_servers(ctx, roles):
    """
    Scan for roles matching ganesha.(\d+).  Yield the id of the the role
    (ganesha.0, ganesha.1...)  and the associated remote site

    :param ctx: Context
    :param roles: roles for this test (extracted from yaml files)
    """
    for role in roles:
        assert isinstance(role, basestring)
        PREFIX = 'ganesha.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        yield (id_, remote)


@contextlib.contextmanager
def task(ctx, config):
    """
    Start a ganesha.nfsd server with a single, '/' export via the
    CEPH fsal module.

    Unlike the Samba task, this task will source install Ganesha
    and its supporting ntirpc library before configuring and starting
    the server.

    The config is optional and defaults to starting Ganesha on one node.
    If a config is given, it is expected to be a list of ganesha nodes
    to start gansha.nfsd servers on.

    Example that starts smbd on all samba nodes::

        tasks:
        - install:
        - install:
            project: ganesha
            extra_packages: ['cmake']
        - ceph:
        - ganesha:
        - interactive:

    Example that starts smbd on one samba node::

        tasks:
        - ganesha: [ganesha.0]

XXX edit here

    An optional backend can be specified, and requires a path which smbd will
    use as the backend storage location:

        roles:
            - [osd.0, osd.1, osd.2, mon.0, mon.1, mon.2, mds.a]
            - [client.0, ganesha.0]

        tasks:
        - ceph:
        - ceph-fuse: [client.0]
        - samba:
            samba.0:
              cephfuse: "{testdir}/mnt.0"

    This mounts ceph to {testdir}/mnt.0 using fuse, and starts smbd with
    a UNC of //localhost/cephfuse.  Access through that UNC will be on
    the ceph fuse mount point.

    If no arguments are specified in the samba
    role, the default behavior is to enable the ceph UNC //localhost/ceph
    and use the ceph vfs module as the smbd backend.

    :param ctx: Context
    :param config: Configuration
    """
    log.info("Setting up Ganesha with FSAL_CEPH export...")
    assert config is None or isinstance(config, list) or isinstance(config, dict), \
        "task samba got invalid config"

    if config is None:
        config = dict(('ganesha.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'ganesha'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    gsh_servers = list(get_gsh_servers(ctx=ctx, roles=config.keys()))

    testdir = teuthology.get_testdir(ctx)

    if not hasattr(ctx, 'daemons'):
        ctx.daemons = DaemonGroup()

    for id_, remote in gsh_servers:

        rolestr = "ganesha.{id_}".format(id_=id_)

        confextras = """vfs objects = ceph
  ceph:config_file = /etc/ceph/ceph.conf"""

        unc = "ceph"
        backend = "/"

        if config[rolestr] is not None:
            # verify that there's just one parameter in role
            if len(config[rolestr]) != 1:
                log.error("ganesha config for role samba.{id_} must have only one parameter".format(id_=id_))
                raise Exception('invalid config')
            confextras = ""
            (unc, backendstr) = config[rolestr].items()[0]
            backend = backendstr.format(testdir=testdir)


        # XXX the following is wrong for ganesha, we may not need anything interesting,
        # as we will be exporting the cephfs root
        if config[rolestr] is None and id_ == gsh_servers[0][0]:
            remote.run(
                    args=[
                        'mkdir', '-p', '/tmp/cmnt', run.Raw('&&'),
                        'sudo', 'ceph-fuse', '/tmp/cmnt', run.Raw('&&'),
                        'sudo', 'chown', 'ubuntu:ubuntu', '/tmp/cmnt/', run.Raw('&&'),
                        'sudo', 'chmod', '1777', '/tmp/cmnt/', run.Raw('&&'),
                        'sudo', 'umount', '/tmp/cmnt/', run.Raw('&&'),
                        'rm', '-rf', '/tmp/cmnt',
                        ],
                    )
        else:
            remote.run(
                    args=[
                        'sudo', 'chown', 'ubuntu:ubuntu', backend, run.Raw('&&'),
                        'sudo', 'chmod', '1777', backend,
                        ],
                    )

        teuthology.sudo_write_file(remote, "/usr/local/ganesha/etc/ganesha.conf", """
EXPORT
{
	# Export Id (mandatory, each EXPORT must have a unique Export_Id)
	Export_Id = 77;

	# Exported path (mandatory)
	Path = "/";

	# Pseudo Path (required for NFS v4)
	Pseudo = "/";

	# Required for access (default is None)
	# Could use CLIENT blocks instead
	Access_Type = RW;

	SecType = "sys";

	Protocols = 3,4;

	Squash = No_Root_Squash;

	# Exporting FSAL
	FSAL {
		Name = CEPH;
	}
}

CEPH {
     # nothing
}

NFS_Core_Param {
       # number of worker threads
       Nb_Worker = 12;
}

NFSV4 {
      # disable grace period checking
      Graceless = true;
}

LOG {
	Default_Log_Level = DEBUG;
	Components {
		#NFS_WRITE = FULL_DEBUG;
	}
	Facility {
		# log via the daemon-helper (console)
		name = STDOUT;
		enable = active;
	}
}
""".format(extras=confextras, unc=unc, backend=backend))

        run_cmd = [
                'sudo',
                'daemon-helper',
                'term',
                'nostdin',
                '/usr/local/samba/sbin/smbd',
                '-c /usr/local/ganesha/etc/ganesha.conf',
                '-F',
                ]
        ctx.daemons.add_daemon(remote, 'ganehsa.nfsd', id_,
                               args=smbd_cmd,
                               logger=log.getChild("ganesha.nfsd.{id_}".format(id_=id_)),
                               stdin=run.PIPE,
                               wait=False,
                               )

        # startup delay
        seconds_to_sleep = 100
        log.info('Sleeping for %s  seconds...' % seconds_to_sleep)
        time.sleep(seconds_to_sleep)
        log.info('Sleeping stopped...')

    try:
        yield
    finally:
        log.info('Stopping smbd processes...')
        exc_info = (None, None, None)
        for d in ctx.daemons.iter_daemons_of_role('ganesha.nfsd'):
            try:
                d.stop()
            except (run.CommandFailedError,
                    run.CommandCrashedError,
                    run.ConnectionLostError):
                exc_info = sys.exc_info()
                log.exception('Saw exception from %s.%s', d.role, d.id_)
        if exc_info != (None, None, None):
            raise exc_info[0], exc_info[1], exc_info[2]

        for id_, remote in gsh_servers:
            remote.run(
                args=[
                    'sudo',
                    'rm', '-rf',
                    '/usr/local/ganesha/etc/smb.conf',
                    '/usr/local/ganesha/private/*',
                    '/usr/local/ganesha/var/run/',
                    '/usr/local/ganesha/var/locks',
                    '/usr/local/ganesha/var/lock',
                    ],
                )
            # make sure daemons are gone
            try:
                remote.run(
                    args=[
                        'while',
                        'sudo', 'killall', '-9', 'ganesha.nfsd',
                        run.Raw(';'),
                        'do', 'sleep', '1',
                        run.Raw(';'),
                        'done',
                        ],
                    )

                remote.run(
                    args=[
                        'sudo',
                        'lsof',
                        backend,
                        ],
                    check_status=False
                    )
                remote.run(
                    args=[
                        'sudo',
                        'fuser',
                        '-M',
                        backend,
                        ],
                    check_status=False
                    )
            except Exception:
                log.exception("Saw exception")
                pass
