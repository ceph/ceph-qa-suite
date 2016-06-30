"""
Osd network blackhole test
"""
import logging
import ceph_manager
import time
from teuthology import misc as teuthology


log = logging.getLogger(__name__)


def rados_start(ctx, remote, cmd):
    """
    Run a remote rados command (currently used to only write data)
    """
    log.info("rados %s" % ' '.join(cmd))
    testdir = teuthology.get_testdir(ctx)
    pre = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'rados',
        ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        wait=False,
        )
    return proc

def task(ctx, config):
    """
    Test network blackhole
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'thrashosds task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    while len(manager.get_osd_status()['up']) < 3:
        manager.sleep(10)
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.wait_for_clean()

    # write some data
    p = rados_start(ctx, mon, ['-p', 'rbd', 'bench', '15', 'write', '-b', '4096',
                          '--no-cleanup'])
    err = p.wait()
    log.info('err is %d' % err)

    # mark osd.0 out to trigger a rebalance/backfill
    manager.network_blackhole_osd(0, 30)

    while manager.osd_is_up(0) == False:
        manager.sleep(1)
    # wait for everything to peer and be happy...
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.wait_till_osd_is_up(0, 60)

    # write some new data
    p = rados_start(ctx, mon, ['-p', 'rbd', 'bench', '700', 'write', '-b', '4096',
                          '--no-cleanup'])

    time.sleep(15)

    # blackhole osd.1
    # this triggers a divergent backfill target
    manager.network_blackhole_osd(1, 400)
    time.sleep(2)

    while manager.osd_is_up(1) == False:
        time.sleep(1)

    manager.mark_in_osd(1)
    manager.wait_till_osd_is_up(1, 500)
    # wait for our writes to complete + succeed
    err = p.wait()
    log.info('err is %d' % err)

    time.sleep(3600)
    # cluster must recover
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.wait_for_recovery()
