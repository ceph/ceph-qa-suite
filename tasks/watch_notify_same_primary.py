
"""
watch_notify_same_primary task
"""
from cStringIO import StringIO
import contextlib
import logging

from teuthology.orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Run watch_notify_same_primary

    The config should be as follows:

    watch_notify_same_primary:
        clients: [client list]

    The client list should contain 1 client

    The test requires 3 osds.

    example:

    tasks:
    - ceph:
    - watch_notify_same_primary:
        clients: [client.0]
    - interactive:
    """
    log.info('Beginning watch_notify_same_primary...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    clients = config.get('clients', ['client.0'])
    assert len(clients) == 1
    role = clients[0]
    assert isinstance(role, basestring)
    PREFIX = 'client.'
    assert role.startswith(PREFIX)
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()

    pool = ctx.manager.create_pool_with_unique_name()
    def obj(n): return "foo-{num}".format(num=n)
    def start_watch(n):
        remote.run(
            args = [
                "rados",
                "-p", pool,
                "put",
                obj(n),
                "/etc/resolv.conf"],
            logger=log.getChild('watch.{id}'.format(id=n)))
        return remote.run(
            args = [
                "rados",
                "-p", pool,
                "watch",
                obj(n)],
            logger=log.getChild('watch.{id}'.format(id=n)),
            stdin=run.PIPE,
            stdout=StringIO(),
            wait=False)
    watches = [start_watch(i) for i in range(20)]

    def notify(n):
        remote.run(
            args = [
                "rados",
                "-p", pool,
                "notify",
                obj(n),
                "message"],
            logger=log.getChild('notify.{id}'.format(id=n)))

    [notify(n) for n in range(len(watches))]

    ctx.manager.raw_cluster_cmd('osd', 'set', 'noout')
    ctx.manager.mark_down_osd(0)

    [notify(n) for n in range(len(watches))]

    for watch in watches:
        watch.stdin.write("\n")
        lines = watch.stdout.split("\n")
        print lines
        assert len(lines) == 3

    try:
        yield
    finally:
        log.info('joining watch_notify_stress')
        ctx.manager.remove_pool(pool)
        [watch.join() for watch in watches]
