"""
Multiclient I/O
"""
import contextlib
import logging
import os
from hashlib import sha1
from teuthology import misc as teuthology
from random import choice
from string import ascii_letters
from tempfile import mkstemp

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Multiclient I/O tests.

    - multiclient_io:
        size: 10
        remote_dir: /tmp
        file_header: foo
        rados_head: bar

    """
    log.info('Beginning multiclient I/O...')
    size = config.get('size', 10000)
    remote_dir = config.get('remote_dir', '.')
    file_header = config.get('file_header', 'multiclientio')
    rados_head = config.get('rados_head', 'multiclientio')
    remfiles = {}
    for remote in ctx.cluster.remotes:
        if any([x.startswith('client') for x in ctx.cluster.remotes[remote]]):
            rem_file = "%s/%s_%s" % (remote_dir, file_header, remote.shortname)
            txt = ''.join([choice(ascii_letters) for x in range(size)])
            hashv = sha1()
            hashv.update(txt)
            remote.run(args=['python', '-c',
                           'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
                           rem_file,
                           ],
                       stdin=txt)
            rados_file = "%s_%s" % (rados_head, remote.shortname)
            remote.run(args=['rados', '-p', 'rbd', 'put', rados_file,
                             rem_file])
            remfiles[remote] = (rem_file, rados_file, hashv.hexdigest())
    for remote in ctx.cluster.remotes:
        for entry in remfiles:
            if entry == remote:
                continue
            tfd, tmp_path = mkstemp()
            remote.run(args=['rados', '-p', 'rbd', 'get', remfiles[entry][1],
                             tmp_path])
            log.info('checking %s on %s' % (remfiles[entry][1], remote.name))
            local_file = remote.get_file(tmp_path)
            with open(local_file, 'rw') as fdesc:
                hashv = sha1()
                hashv.update(fdesc.read())
                msg_pattern = 'On %s: %s %s value stored by %s'
                if  remfiles[entry][2] != hashv.hexdigest():
                    log.error(msg_pattern % (remote.name, remfiles[entry][1],
                              'does not match', entry.name))
                else:
                    log.info(msg_pattern % (remote.name, remfiles[entry][1],
                              'matches', entry.name))
            remote.run(args=['rm', tmp_path])
            os.close(tfd)
            os.remove(local_file)
            os.remove(tmp_path)
    try:
        yield
    finally:
        for remote in ctx.cluster.remotes:
            remote.run(args=['rados', '-p', 'rbd', 'rm', remfiles[remote][1]])
            remote.run(args=['rm', remfiles[remote][0]])
