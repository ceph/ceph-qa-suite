"""
 Long running fio tests on rbd mapped devices for format/features provided in config
 Many fio parameters can be configured so that this task can be used along with thrash/power-cut tests
 and exercise IO on full disk for all format/features
  - This test should not be run on VM due to heavy use of resource
  
"""
import contextlib
import logging
import StringIO
import re

from teuthology.parallel import parallel
from teuthology import misc as teuthology
from tempfile import NamedTemporaryFile
from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    client.0:
       size: 100gb
       fio-version: 2.2.9
       formats: [1, 2]
       features: [1, 2, 4]
       io-engine: sync
       rw: randrw
    client.1:
       size: 100gb
       fio-version: 2.2.9
       rw: read
       image-size:20480 

or
    all:
       size: 400g
       rw: randrw
       formats: [1,2]
       features: [1,2]
       io-engine: libaio
       
    Create rbd image + device and exercise IO for format/features provided in config file
    Config can be per client or one config can be used for all clients, fio jobs are run in parallel for client provided
    
    """
    if config.get('all'):
        client_config = config['all']  
    clients = ctx.cluster.only(teuthology.is_type('client'))
    for remote,role in clients.remotes.iteritems():
        if 'client_config' in locals():
           with parallel() as p: 
               p.spawn(run_fio, remote, client_config)
        else:    
           for client_config in config:
              if client_config in role:
                 with parallel() as p:
                     p.spawn(run_fio, remote, config[client_config])
            
    yield
    
    
def run_fio(remote, config):
    """
    create fio config file with options based on above config
    get the fio from github, generate binary, and use it to run on
    the generated fio config file
    """
    fio_config=NamedTemporaryFile(prefix='fio_rbd_', dir='/tmp/', delete=False)
    fio_config.write('[global]\n')
    if config.get('io-engine'):
        ioengine=config['io-engine']
        fio_config.write('ioengine={ioe}\n'.format(ioe=ioengine))
    else:
        fio_config.write('ioengine=sync\n')
    if config.get('bs'):
        bs=config['bs']
        fio_config.write('bs={bs}\n'.format(bs=bs))
    else:
        fio_config.write('bs=4k\n')
    fio_config.write('iodepth=2\n')
    if config.get('size'):
        size=config['size']
        fio_config.write('size={size}\n'.format(size=size))
    else:
        fio_config.write('size=10g\n')
    
    fio_config.write('time_based\n')
    if config.get('runtime'):
        runtime=config['runtime']
        fio_config.write('runtime={runtime}\n'.format(runtime=runtime))
    else:
        fio_config.write('runtime=1800\n')
    fio_config.write('allow_file_create=0\n')    
    image_size=10240    
    if config.get('image_size'):
        image_size=config['image_size']
        
    formats=[1,2]
    features=[1,2,4]
    fio_version='2.2.9'
    if config.get('formats'):
        formats=config['formats']
    if config.get('features'):
        features=config['features']
    if config.get('fio-version'):
        fio_version=config['fio-version']
    
    fio_config.write('norandommap\n')
    for frmt in formats:
        for feature in features:
           sn=remote.shortname
           log.info("Creating rbd images on {sn}".format(sn=sn))
           remote.run(args=['sudo', 'rbd', 'create',
                            '--image', 'i{i}f{f}{sn}'.format(i=frmt,f=feature,sn=sn),
                            '--image-features', '{f}'.format(f=feature),
                            '--size', '{size}'.format(size=image_size)])
           out=StringIO.StringIO()
           remote.run(args=['sudo', 'rbd', 'map', 'i{i}f{f}{sn}'.format(i=frmt,f=feature,sn=sn)],stdout=out)
           dev=re.search(r'(/dev/rbd\d+)',out.getvalue())
           rbd_dev=dev.group(1)
           fio_config.write('[{rbd_dev}]\n'.format(rbd_dev=rbd_dev))
           if config.get('rw'):
               rw=config['rw']
               fio_config.write('rw={rw}\n'.format(rw=rw))
           else:
               fio_config.write('rw=randrw\n')
           fio_config.write('filename={rbd_dev}\n'.format(rbd_dev=rbd_dev))
           
    fio_config.close()
    remote.put_file(fio_config.name,fio_config.name)
    try:
        log.info("Running rbd feature - fio test on {sn}".format(sn=sn))
        fio = "https://github.com/axboe/fio/archive/fio-" + fio_version + ".tar.gz"
        remote.run(args=['mkdir', run.Raw('~/rbd-fio-test'),])
        remote.run(args=['cd' , run.Raw('~/rbd-fio-test'),
                         run.Raw(';'), 'wget' , fio , run.Raw(';'), run.Raw('tar -xvf fio*tar.gz'), run.Raw(';'),
                         run.Raw('cd fio-fio*'), 'configure', run.Raw(';') ,'make'])
        remote.run(args=['sudo', 'ceph', '-s'])
        remote.run(args=['sudo', run.Raw('~/rbd-fio-test/fio-fio-{v}/fio {f}'.format(v=fio_version,f=fio_config.name))])
        remote.run(args=['sudo', 'ceph', '-s'])
    finally:
        log.info("Cleaning up fio install")
        remote.run(args=['rm','-rf', run.Raw('~/rbd-fio-test')])
        
    
    
