"""
Special regression test for tracker #11184

Synopsis: osd/SnapMapper.cc: 282: FAILED assert(check(oid))
"""
import logging
import time
from cStringIO import StringIO

from teuthology import misc as teuthology
from util.rados import rados
import os


log = logging.getLogger(__name__)


def task(ctx, config):
    """
    Test handling of divergent entries during export / import

    config: none

    Requires 3 osds.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'divergent_priors task only accepts a dict for configuration'

    while len(ctx.manager.get_osd_status()['up']) < 3:
        time.sleep(10)
    ctx.manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    ctx.manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    ctx.manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    ctx.manager.raw_cluster_cmd('osd', 'set', 'noout')
    ctx.manager.raw_cluster_cmd('osd', 'set', 'noin')
    ctx.manager.raw_cluster_cmd('osd', 'set', 'nodown')
    ctx.manager.wait_for_clean()

    # something that is always there
    dummyfile = '/etc/fstab'
    dummyfile2 = '/etc/resolv.conf'
    testdir = teuthology.get_testdir(ctx)

    # create 1 pg pool
    log.info('creating foo')
    ctx.manager.raw_cluster_cmd('osd', 'pool', 'create', 'foo', '1')

    osds = [0, 1, 2]
    for i in osds:
        ctx.manager.set_config(i, osd_min_pg_log_entries=1)

    # determine primary
    divergent = ctx.manager.get_pg_primary('foo', 0)
    log.info("primary and soon to be divergent is %d", divergent)
    non_divergent = list(osds)
    non_divergent.remove(divergent)

    log.info('writing initial objects')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    # write 20 objects
    for i in range(20):
        rados(ctx, mon, ['-p', 'foo', 'put', 'existing_%d' % i, dummyfile])

    ctx.manager.wait_for_clean()

    # blackhole non_divergent
    log.info("blackholing osds %s", str(non_divergent))
    for i in non_divergent:
        ctx.manager.set_config(i, filestore_blackhole=1)
    time.sleep(5)

    # remove 1 (divergent) change
    #log.info('remove divergent object existing_0')
    #rados(ctx, mon, ['-p', 'foo', 'rm', 'existing_0'], wait=False)
    # write 1 (divergent) change
    log.info('write divergent object existing_0')
    rados(ctx, mon, ['-p', 'foo', 'put', 'existing_0', dummyfile2], wait=False)
    time.sleep(10)
    mon.run(
        args=['killall', '-9', 'rados'],
        wait=True,
        check_status=False)

    # kill all the osds
    log.info('killing all the osds')
    for i in osds:
        ctx.manager.kill_osd(i)
    for i in osds:
        ctx.manager.mark_down_osd(i)
    for i in osds:
        ctx.manager.mark_out_osd(i)

    # bring up non-divergent
    log.info("bringing up non_divergent %s", str(non_divergent))
    for i in non_divergent:
        ctx.manager.revive_osd(i)
    for i in non_divergent:
        ctx.manager.mark_in_osd(i)

    log.info('making log long to prevent backfill')
    for i in non_divergent:
        ctx.manager.set_config(i, osd_min_pg_log_entries=100000)

    # write 1 non-divergent object (ensure that old divergent one is divergent)
    log.info('writing non-divergent object existing_1')
    rados(ctx, mon, ['-p', 'foo', 'put', 'existing_1', dummyfile2])

    # Split pgs for pool foo
    ctx.manager.raw_cluster_cmd('osd', 'pool', 'set', 'foo', 'pg_num', '2')
    time.sleep(5)
    ctx.manager.wait_for_recovery()

    # Take down osd that has a copy osd.2 as seen in logs (Hack)
    ctx.manager.kill_osd(2)
    ctx.manager.mark_down_osd(2)
    ctx.manager.mark_out_osd(2)
    # Delay recovery on osd.0 (Hack)
    #ctx.manager.set_config(0, osd_recovery_delay_start=100000)

    # Let divergent see the issue before we export the pg
    log.info("revive divergent %d", divergent)
    ctx.manager.revive_osd(divergent)
    #ctx.manager.set_config(divergent, osd_recovery_delay_start=100000)
    ctx.manager.mark_in_osd(divergent)
    time.sleep(5)
    ctx.manager.raw_cluster_cmd('-s')

    # kill divergent osd again
    log.info('killing divergent osds')
    ctx.manager.kill_osd(divergent)
    ctx.manager.mark_down_osd(divergent)
    ctx.manager.mark_out_osd(divergent)

    # Export a pg
    (exp_remote,) = ctx.\
        cluster.only('osd.{o}'.format(o=divergent)).remotes.iterkeys()
    FSPATH = ctx.manager.get_filepath()
    JPATH = os.path.join(FSPATH, "journal")
    prefix = ("sudo adjust-ulimits ceph-objectstore-tool "
              "--data-path {fpath} --journal-path {jpath} "
              "--log-file="
              "/var/log/ceph/objectstore_tool.$$.log ".
              format(fpath=FSPATH, jpath=JPATH))
    expfile = os.path.join(testdir, "exp.$$.out")
    cmd = ((prefix + "--op export --pgid 1.0 --file {file}").format(id=divergent, file=expfile))
    proc = exp_remote.run(args=cmd, wait=True,
                          check_status=False, stdout=StringIO())
    assert proc.exitstatus == 0


    assert False

    # ensure no recovery
    # log.info('delay recovery')
    # for i in non_divergent:
    #    ctx.manager.set_config(i, osd_recovery_delay_start=100000)

    # Kill one of non-divergent OSDs
    log.info('killing osd.%d' % non_divergent[0])
    ctx.manager.kill_osd(non_divergent[0])
    ctx.manager.mark_down_osd(non_divergent[0])
    # ctx.manager.mark_out_osd(non_divergent[0])

    (imp_remote,) = ctx.\
        cluster.only('osd.{o}'.format(o=non_divergent[0])).remotes.iterkeys()

    # See if we need to copy the exp file
    # if exp_remote != imp_remote:
    #

    # Remove the same pg that was exported
    cmd = ((prefix + "--op remove --pgid 1.0").format(id=non_divergent[0]))
    proc = imp_remote.run(args=cmd, wait=True,
                          check_status=False, stdout=StringIO())
    assert proc.exitstatus == 0

    # Import the pg
    cmd = ((prefix + "--op import --file {file}").format(id=non_divergent[0], file=expfile))
    proc = imp_remote.run(args=cmd, wait=True,
                          check_status=False, stdout=StringIO())
    assert proc.exitstatus == 0

    # bring in our divergent friend and other node
    log.info("revive divergent %d", divergent)
    ctx.manager.revive_osd(divergent)
    ctx.manager.mark_in_osd(divergent)
    log.info("revive %d", non_divergent[0])
    ctx.manager.revive_osd(non_divergent[0])
    ctx.manager.mark_in_osd(non_divergent[0])

    while len(ctx.manager.get_osd_status()['up']) < 3:
        time.sleep(10)

    log.info('delay recovery divergent')
    ctx.manager.set_config(divergent, osd_recovery_delay_start=100000)
    log.info('mark divergent in')
    ctx.manager.mark_in_osd(divergent)

    log.info('wait for peering')
    rados(ctx, mon, ['-p', 'foo', 'put', 'foo', dummyfile])

    log.info("killing divergent %d", divergent)
    ctx.manager.kill_osd(divergent)
    log.info("reviving divergent %d", divergent)
    ctx.manager.revive_osd(divergent)

    log.info('allowing recovery')
    # Set osd_recovery_delay_start back to 0 and kick the queue
    for i in non_divergent:
        ctx.manager.raw_cluster_cmd('tell', 'osd.%d' % i, 'debug', 'kick_recovery_wq', ' 0')

    log.info('reading existing_0')
    exit_status = rados(ctx, mon,
                        ['-p', 'foo', 'get', 'existing_0',
                         '-o', '/tmp/existing'])
    assert exit_status is 0

    # Remove expfile on exp_remote
    cmd = "rm -f {file}".format(file=expfile)
    proc = exp_remote.run(args=cmd, wait=True,
                          check_status=False, stdout=StringIO())
    assert proc.exitstatus == 0
    if imp_remote != exp_remote:
        proc = imp_remote.run(args=cmd, wait=True,
                              check_status=False, stdout=StringIO())
    assert proc.exitstatus == 0

    cmd = 'grep "divergent_priors:" /var/log/ceph/ceph-osd.*.log | grep -v "divergent_priors: 0"'
    proc = exp_remote.run(args=cmd, wait=True, check_status=False)
    assert proc.exitstatus == 0

    log.info("success")
