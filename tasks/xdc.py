"""
Task to handle xdc

Assumptions made:
    The ceph-extras xdc package may need to get installed.
    The open-iscsi package needs to get installed.
"""
import logging
import contextlib
import socket

from teuthology import misc as teuthology
from teuthology import contextutil

log = logging.getLogger(__name__)

def _get_remote_ip(remotes):
     """
     Get remote name that is associated with the client specified.
     """
     rem_name = remotes.name
     rem_name = rem_name[rem_name.find('@') + 1:]
     ip = socket.gethostbyname(rem_name)
     return ip

@contextlib.contextmanager
def start_xdc_remotes(ctx, start_xdcd):
    """
    This subtask starts up a xdcd on the clients specified
    """
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    xdcd_list = []
    for rem, roles in remotes.iteritems():
        for _id in roles:
            if _id in start_xdcd:
                if not rem in xdcd_list:
                    xdcd_list.append(rem)
		    localIP = _get_remote_ip(rem)
		    portal = localIP + ":3260"
                    size = ctx.config.get('image_size', 10240)
		    rem.run(
                        args=[
                            'sudo',
                            'service',
                            'xsky-xdc',
                            'restart',
                        ])
                    rem.run(
                        args=[
                            'rbd',
                            'create',
                            'iscsi-image',
                            '--size',
                            str(size),
                        ])
		    rem.run(
                        args=[
                            'sudo',
                            'xdcadm',
                            '--lld',
                            'at',
                            '--mode',
                            'at',
                            '--op',
                            'create',
                            '--atid',
                            '1',
                            '--servernode',
                            'xdchost',
		  	    '--boardid',
                            '0',
                        ])
                    rem.run(
                        args=[
                            'sudo',
                            'xdcadm',
                            '--lld',
                            'at',
                            '--mode',
                            'target',
                            '--op',
                            'create',
                            '--atid',
                            '1',
                            '--iqn',
                            'iqn.2003-01.org',
			    '--type',
                            'iscsi',
                        ])
		    rem.run(
                        args=[
                            'sudo',
                            'xdcadm',
                            '--lld',
                            'at',
                            '--mode',
                            'target',
                            '--op',
                            'add',
                            '--atid',
                            '1',
                            '--iqn',
                            'iqn.2003-01.org',
			    '--port',
                            portal,
                        ])
                    rem.run(
                        args=[
                            'sudo',
                            'xdcadm',
                            '--lld',
                            'at',
                            '--mode',
                            'lun',
                            '--op',
                            'create',
                            '--lunname',
                            'iscsi-image',
                            '--lunsn',
                            '1134321edf',
			    '--lunsize',
                            '10737418240',
			    '--luncfg',
                            'ceph/rbd/iscsi-image',
                        ])
                    rem.run(
                        args=[
                            'sudo',
                            'xdcadm',
                            '--lld',
                            'at',
			    '--mode',
                            'lun',
                            '--op',
                            'add',
                            '--atid',
                            '1',
                            '--lunname',
                            'iscsi-image',
			    '--lunid',
                            '0',
                        ])
    try:
        yield

    finally:
        for rem in xdcd_list:
            rem.run(
		args=[
		    'sudo',
		    'xdcadm',
		    '--lld',
		    'at',
		    '--mode',
		    'lun',
		    '--op',
		    'remove',
		    '--atid',
		    '1',
		    '--lunname',
		    'iscsi-image',
		    '--lunid',
		    '0',
		])
	    rem.run(
		args=[
		    'sudo',
		    'xdcadm',
		    '--lld',
		    'at',
		    '--mode',
		    'lun',
		    '--op',
		    'delete',
		    '--lunname',
		    'iscsi-image',
		])
	    rem.run(
		args=[
		    'sudo',
		    'xdcadm',
		    '--lld',
		    'at',
		    '--mode',
		    'target',
		    '--op',
		    'remove',
		    '--atid',
		    '1',
		    '--iqn',
		    'iqn.2003-01.org',
		    '--port',
		    portal,
		])
	    rem.run(
		args=[
		    'sudo',
		    'xdcadm',
		    '--lld',
		    'at',
		    '--mode',
		    'target',
		    '--op',
		    'delete',
		    '--atid',
		    '1',
		    '--iqn',
		    'iqn.2003-01.org',
		])
	    rem.run(
		args=[
		    'sudo',
		    'xdcadm',
		    '--lld',
		    'at',
		    '--mode',
		    'at',
		    '--op',
		    'delete',
		    '--atid',
		    '1',
		])
            rem.run(
                args=[
                    'rbd',
                    'snap',
                    'purge',
                    'iscsi-image',
                ])
            rem.run(
                args=[
                    'sudo',
                    'rbd',
                    'rm',
                    'iscsi-image',
                ])


@contextlib.contextmanager
def task(ctx, config):
    """
    Start up xdc.

    To start on on all clients::

        tasks:
        - ceph:
        - xdc:

    To start on certain clients::

        tasks:
        - ceph:
        - xdc: [client.0, client.3]

    or

        tasks:
        - ceph:
        - xdc:
            client.0:
            client.3:

    An image blocksize size can also be specified::

        tasks:
        - ceph:
        - xdc:
            image_size = 20480

    The general flow of things here is:
        1. Find clients on which xdc is supposed to run (start_xdcd)
        2. Remotely start up xdc daemon
    On cleanup:
        3. Stop xdc daemon

    The iscsi administration is handled by the iscsi task.
    """
    if config:
        config = {key : val for key, val in config.items()
                if key.startswith('client')}
    # config at this point should only contain keys starting with 'client'
    start_xdcd = []
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    log.info(remotes)
    if not config:
        start_xdcd = ['client.{id}'.format(id=id_)
            for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    else:
        start_xdcd = config
    log.info(start_xdcd)
    with contextutil.nested(
            lambda: start_xdc_remotes(ctx=ctx, start_xdcd=start_xdcd),):
        yield
