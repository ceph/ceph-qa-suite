import logging
import contextlib
import time

from teuthology import misc
from gevent.greenlet import Greenlet
from gevent.event import Event
from tasks.cephfs.filesystem import Filesystem
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

'''
    parameters:

    Test runs only in one cephfs mount point.

    filesize: Optional Param. Unit in MB, default file size 2MB
              Size of the data file before creating snapshot.
              Not used if workunit task running in parallel.

    runtime: Required Param for using rsync task sequentially. unit in seconds. default 0 sec.
             Don't use this param during parallel execution. if used then finally block will be delayed

    data_dir: Optional Param. Specify the client like client.0/client.1
              make sure cephfs mount and workunit task using this client.
              Default source directory will be source/subdir for other scenarios.

    snapenable: Optional Param. value in boolean (True or False), default False(disabled).
                using this option will enable cephfs snapshot and rsync.

    waittime: Optional Param. units in seconds. default 5 sec.
              During rsync iteration each loop will wait for specified seconds.

    Note: snapenable and data_dir can't co-exist.

    Examples:

    rsync task parallel with another task
    tasks:
      - ceph:
      - ceph-fuse:
      - rsync:
            waittime: 10
            snapenable: True
            filesize: 8
      - cephfs_test_runner:

    rsync task parallel with workunit
    tasks:
      - ceph:
      - ceph-fuse: [client.2]
      - rsync:
            waittime: 10
            data_dir: client.2
      - workunit:
            clients:
            client.2:
                - suites/iozone.sh

    rsync task sequentially based on runtime. default runtime is 0
    tasks:
      - ceph:
      - ceph-fuse:
      - rsync:
            runtime: 120

'''

class RSync(Greenlet):

    def __init__(self, ctx, config, logger):
        super(RSync, self).__init__()

        self.ctx = ctx

        self.stopping = Event()
        self.config = config
        self.logger = logger

        self.my_mnt = None
        self.workUnit = False

        self.runTime = self.config.get('runtime', 0)
        self.fileSize = self.config.get('filesize', 2)
        self.waitTime = self.config.get('waittime', 5)

        self.fs = Filesystem(self.ctx)

        self.snapEnable = bool(self.config.get('snapenable', False))
        self.logger.info('Snapenable param: {}'.format(self.snapEnable))

        if self.config.get('data_dir'):
            self.workUnit = True
            self.dataDir = misc.get_testdir(self.ctx)
            self.sourceDir = self.dataDir + '/workunit.{}/'.format(self.config.get('data_dir'))
        else:
            self.snapShot = None
            self.snapDummy = None
            self.iteration = 0
            self.dataDir = 'source'
            self.sourceDir = self.dataDir + '/subdir'

    def _run(self):
        try:
            self.do_rsync()
        except:
            # Log exceptions here so we get the full backtrace (it's lost
            # by the time someone does a .get() on this greenlet)
            self.logger.exception("Exception in do_rsync:")
            raise

    def log(self, x):
        """Write data to logger assigned to this MDThrasher"""
        self.logger.info(x)

    def stop(self):
        time.sleep(self.runTime)
        self.stopping.set()

    def rsync_io(self):
        try:
            if self.snapEnable:
                # Rsync snapshot data to rsync directory
                self.my_mnt.run_shell(["rsync", "-azvh", "{}".format(self.snapShot), "rsyncdir/dir1/"])
            else:
                # Rsync data from data directory to rsync directory
                self.my_mnt.run_shell(["rsync", "-azvh", "{}".format(self.sourceDir), "rsyncdir/dir1/"])

        except CommandFailedError:
            self.logger.error('rsync command failed asserting rsync')
            raise

    def do_rsync(self):

        # Get mount objects
        mounts = [v for k, v in sorted(self.ctx.mounts.items(), lambda a, b: cmp(a[0], b[0]))]

        # Using first mount point of cephfs for run rsync test.
        if len(mounts):
            self.my_mnt = mounts.pop(0)
        else:
            assert len(mounts) > 0, self.logger.error('No mount available asserting rsync')

        if self.snapEnable:
            # Enable snapshots
            self.fs.mon_manager.raw_cluster_cmd("mds", "set", "allow_new_snaps", "true", "--yes-i-really-mean-it")
            self.snapDummy = self.dataDir + '/.snap/snap'

        if self.workUnit:
            assert self.snapEnable == False, \
                self.logger.error('Workunit task and Snapshot enable cannot co-exist asserting rsync')
        else:
            # Create a data directory, sub directory and rsync directory
            self.my_mnt.run_shell(["mkdir", "{}".format(self.dataDir)])
            self.my_mnt.run_shell(["mkdir", "{}".format(self.sourceDir)])

        # Create destination directory
        self.my_mnt.run_shell(["mkdir", "rsyncdir"])
        self.my_mnt.run_shell(["mkdir", "rsyncdir/dir1"])

        while not self.stopping.is_set():

            if self.snapEnable:
                # Create file and add data to the file
                self.my_mnt.write_test_pattern("{}/file_a".format(self.sourceDir), self.fileSize * 1024 * 1024)
                self.snapShot = self.snapDummy + '{}'.format(self.iteration)

                # Create Snapshot
                self.my_mnt.run_shell(["mkdir", "{}".format(self.snapShot)])
                self.iteration += 1

                self.rsync_io()

                # Delete snapshot
                self.my_mnt.run_shell(["rmdir", "{}".format(self.snapShot)])

                # Delete the file created in data directory
                self.my_mnt.run_shell(["rm", "-f", "{}/file_a".format(self.sourceDir)])

            elif self.workUnit:
                self.rsync_io()

            else:
                # Create file and add data to the file
                self.my_mnt.write_test_pattern("{}/file_a".format(self.sourceDir), self.fileSize * 1024 * 1024)

                self.rsync_io()

                # Delete the file created in data directory
                self.my_mnt.run_shell(["rm", "-f", "{}/file_a".format(self.sourceDir)])

            time.sleep(self.waitTime)

@contextlib.contextmanager
def task(ctx, config):

    log.info('Beginning rsync...')

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "rsync task accepts dict for running configuration"

    log.info("Create object and start the gevent thread")
    start_rsync = RSync(ctx, config, logger=log.getChild('rsync'))
    start_rsync.start()
    start_rsync_thread = start_rsync

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining rsync thread')
        start_rsync_thread.stop()
        start_rsync_thread.get()
        start_rsync_thread.join()
        log.info("Done joining")
