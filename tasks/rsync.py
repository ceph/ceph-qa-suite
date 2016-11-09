import logging
import contextlib
import time

from gevent.greenlet import Greenlet
from gevent.event import Event
from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)

"""
    parameters:

    Test runs only in one client mount point which is client.0 as per roles.

    filesize: Optional Param. Unit in MB, default file size 2MB
              Size of the data file before creating snapshot.

    runtime: Required Param while using rsync sequentially. unit in seconds. default it is 0 sec.
             Don't use this param during parallel execution. if used during execution then finally will be delayed

    Examples:

    Using rsync task parallel with another task
    tasks:
      - ceph:
      - ceph-fuse:
      - rsync:
      - cephfs_test_runner:

    using rsync task sequentially based on runtime. default runtime is 0
    tasks:
      - ceph:
      - ceph-fuse:
      - rsync:
            runtime: 10
"""

class RSync(Greenlet):

    def __init__(self, ctx, config, logger):
        super(RSync, self).__init__()

        self.ctx = ctx

        self.stopping = Event()
        self.config = config
        self.logger = logger

        self.my_mount = None

        self.runTime = self.config.get('runtime', 0)

        self.fileSize = self.config.get('filesize', 2)

        self.fs = Filesystem(self.ctx)

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

    def do_rsync(self):

        # Enable snapshots
        self.fs.mon_manager.raw_cluster_cmd("mds", "set", "allow_new_snaps", "true", "--yes-i-really-mean-it")

        # Get mount points objects
        mounts = [v for k, v in sorted(self.ctx.mounts.items(), lambda a, b: cmp(a[0], b[0]))]

        # Using first client mount point to run rsync test.
        if len(mounts) >= 1:
            self.my_mnt = mounts.pop(0)
        else:
            logging.exception("No mount point available")

        # Create a data directory, sub directory and rsync directory
        self.my_mnt.run_shell(["mkdir", "datadir"])
        self.my_mnt.run_shell(["mkdir", "datadir/subdir"])
        self.my_mnt.run_shell(["mkdir", "rsyncdatadir"])
        self.my_mnt.run_shell(["mkdir", "rsyncdatadir/dir1"])

        while not self.stopping.is_set():

            try:
                # Create file and add data to the file
                self.my_mnt.write_test_pattern("datadir/subdir/file_a", self.fileSize * 1024 * 1024)

                # Create Snapshot
                self.my_mnt.run_shell(["mkdir", "datadir/.snap/snap1"])

                # Rsync snapshot data to rsync directory
                self.my_mnt.run_shell(["rsync", "-azvh", "datadir/.snap/snap1", "rsyncdatadir/dir1/"])

                # Delete snapshot
                self.my_mnt.run_shell(["rmdir", "datadir/.snap/snap1"])

                # Delete the file created in data directory
                self.my_mnt.run_shell(["rm", "-f", "datadir/subdir/file_a"])

            except:
                logging.warning(
                    "Caught Exception while deleting the snapshot because of stale file, Trying to remove it again.")
                time.sleep(20)
                self.my_mnt.run_shell(["rmdir", "datadir/.snap/snap1"])
                self.my_mnt.run_shell(["rm", "-f", "datadir/subdir/file_a"])

            time.sleep(5)


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
    start_rsync_tread = start_rsync

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining rsync thread')
        start_rsync_tread.stop()
        start_rsync_tread.get()
        start_rsync_tread.join()
        log.info("Done joining")