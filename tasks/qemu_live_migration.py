"""
  This will test qemu live migration using libvirt tools
  It requires Ceph 2 clients which should be configured to have access to same
  rbd pools
"""
import contextlib
import logging

from teuthology import misc as teuthology
import StringIO
import re
import time

from teuthology.orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
     Test the qemu-kvm live migration using libvirt
     Needs 2 ceph clients which should be configured to have access to same
     rbd pools Most of the config items are configurable to include what image
     to be run the vm domain definition and the livirt secret uuid.

     qemu-live-migration:
        libvirtd-conf: http://ceph/qemu-libvirtd.conf
                       (this uses tcp protool for migration)
        libvirtd-args: http://ceph/qemu-libvirtd-args.conf
                       ( should set the listen option)
        qemu-image: http://cloud-images.ubuntu.com/releases/14.04/release/
                        ubuntu-14.04-server-cloudimg-amd64-disk1.img
        secret-file: http://ceph/qemu-secret-file.txt
        vm-file:  http://ceph/qemu-vm-test.xml

    """
    clients = ctx.cluster.only(teuthology.is_type('client'))
    assert len(
        clients.remotes) == 2, "Needs 2 clients to run qemu live migration test"
    config['client0'], config['client1'] = clients.remotes.keys()
    qemu_test_dir = teuthology.get_testdir(ctx) + "/qemu_test"
    config['qemu-test-dir'] = qemu_test_dir
    clients.run(args=['mkdir', '-p', qemu_test_dir])
    config['rbd-name'] = "rbd/ceph-vm.img"
    config['img-name'] = '{dir}/ceph_vm.img'.format(dir=qemu_test_dir)
    config['vm-name'] = '{dir}/vm.xml'.format(dir=qemu_test_dir)
    try:
        get_test_files(config)
        ceph_auth_create(config)
        setup_libvirtd(config)
        libvirt_set_secret(config)
        create_rbd_image(config)
        create_and_define_vm(config)
        migrate_vm(config)
        yield
    finally:
        log.info("running cleanup")
        cleanup(config)


def get_test_files(config):
    """
    Get all the test files and image file on one of the client nodes
    VM file should be xml file that has
    RBD_IMAGE_NAME which is replaced with real rbd image name thats created
    and
    MON_IP that is replaced with real monitor ip
    for example check: http://fpaste.org/336533/57994014/raw/
    """
    image_location = config.get('qemu-image')
    vm_file = config.get('vm-file')
    img_name = config.get('img-name')
    vm_name = config.get('vm-name')
    client = config.get('client0')
    log.info("Get the test files")
    client.run(args=['wget', '-O', run.Raw(img_name),
                     run.Raw(image_location)], check_status=False)
    client.run(args=['wget', '-nv', '-O', run.Raw(vm_name),
                     run.Raw(vm_file)])


def ceph_auth_create(config):
    """
    Set the client.libvirt for use with libvirt config
    """
    client = config.get('client0')
    client.run(
        args=[
            'sudo',
            'ceph',
            'auth',
            'add',
            'client.libvirt',
            run.Raw("mon 'allow *' osd 'allow *'")])
    client.run(args=['sudo', 'ceph', 'auth', 'list'])


def libvirt_set_secret(config):
    """
    get the secret xml and set the secret on both the nodes
    secret.xml can be as seen in qemu ceph document
    but it should also include the same uuid across clients.
    for ex: http://fpaste.org/334196/14189914/raw/
    """
    secret_file = config.get('secret-file')
    qemu_test_dir = config.get('qemu-test-dir')
    out = StringIO.StringIO()
    clients = [config.get('client0'), config.get('client1')]
    for client in clients:
        client.run(args=['wget',
                         '-nv',
                         '-O',
                         run.Raw('{dir}/secret.xml'.format(dir=qemu_test_dir)),
                         run.Raw(secret_file)])
        client.run(
            args=[
                'cd',
                qemu_test_dir,
                run.Raw(';'),
                'ceph',
                'auth',
                'get-key',
                'client.libvirt',
                run.Raw('|'),
                'sudo',
                'tee',
                'client.libvirt'])
        client.run(
            args=[
                'cd',
                qemu_test_dir,
                run.Raw(';'),
                'sudo',
                'virsh',
                'secret-define',
                run.Raw('--file'),
                'secret.xml'],
            stdout=out)
        uuid = re.search(r'(\w{8}-\w{4}-\w{4}-\w{4}-\w{12})', out.getvalue())
        suuid = uuid.group(1)
        client.run(
            args=[
                'cd',
                qemu_test_dir,
                run.Raw(';'),
                'sudo',
                'virsh',
                'secret-set-value',
                run.Raw('--secret'),
                suuid,
                run.Raw('--base64'),
                run.Raw("$(cat client.libvirt)")])
        client.run(args=['sudo', 'virsh', 'secret-list'])


def create_rbd_image(config):
    """
    Convert the image to rbd image using qemu-img
    """
    img_name = config.get('img-name')
    rbd_name = 'rbd:' + config.get('rbd-name')
    client = config.get('client0')
    client.run(
        args=[
            'qemu-img',
            'convert',
            run.Raw('-f'),
            'qcow2',
            run.Raw('-O'),
            'raw',
            img_name,
            rbd_name])


def setup_libvirtd(config):
    """
    Setup the libvirt to use tcp for easy migration
    set the listen option in args for libvirtd
    """
    clients = [config.get('client0'), config.get('client1')]
    libvirtd_conf = config.get('libvirtd-conf')
    libvirtd_args = config.get('libvirtd-args')
    qemu_test_dir = config.get('qemu-test-dir')
    for client in clients:
        log.info("Copy original libvirtd config files")
        client.run(
            args=[
                'sudo',
                'cp',
                '/etc/libvirt/libvirtd.conf',
                qemu_test_dir])
        client.run(
            args=[
                'sudo',
                'cp',
                '/etc/sysconfig/libvirtd',
                qemu_test_dir])
        log.info("Overwrite the config files with the user provided config")
        client.run(
            args=[
                'sudo',
                'wget',
                '-nv',
                '-O',
                run.Raw('/etc/libvirt/libvirtd.conf'),
                libvirtd_conf])
        client.run(
            args=[
                'sudo',
                'wget',
                '-nv',
                '-O',
                run.Raw('/etc/sysconfig/libvirtd'),
                libvirtd_args])
        client.run(args=['sudo', 'systemctl', 'restart', 'libvirtd'])


def create_and_define_vm(config):
    """
    define the vm and set the mon and rbd name to match the
    ceph system in test
    """
    vm_name = config.get('vm-name')
    rbd_name = config.get('rbd-name')
    client = config.get('client0')
    out = StringIO.StringIO()
    log.info("Set the Mon ip and the RBD name for the VM")
    client.run(
        args=[
            'sudo',
            'cat',
            '/etc/ceph/ceph.conf',
            run.Raw('|'),
            run.Raw('grep -oE "([0-9]{1,3}\.){3,3}[0-9]{1,3}:6789"')],
        stdout=out)
    mon_ips = out.getvalue().rstrip()
    ips = mon_ips.split(':')
    mon_ip = ips[0]
    rbd_replace = 's#RBD_IMAGE_NAME#{name}#'.format(name=rbd_name)
    mon_replace = 's#MON_IP#{ip}#'.format(ip=mon_ip)
    client.run(args=['sed', run.Raw('-i'), run.Raw(rbd_replace), vm_name])
    client.run(args=['sed', run.Raw('-i'), run.Raw(mon_replace), vm_name])


def migrate_vm(config):
    """
    Migrate the VM from one ceph client to another
    """
    client0 = config.get('client0')
    client1 = config.get('client1')
    vm_name = config.get('vm-name')
    out = StringIO.StringIO()
    log.info("Start and migrate the VM")
    client0.run(args=['sudo', 'virsh', 'define', vm_name], stdout=out)
    dom_name = re.search(r'Domain\s+(.*)\s+defined', out.getvalue())
    dom_name = dom_name.group(1)
    config['dom-name'] = dom_name
    client0.run(args=['sudo', 'virsh', 'start', dom_name])
    time.sleep(4)
    out = StringIO.StringIO()
    client0.run(args=['sudo', 'virsh', 'list'], stdout=out)
    if not out.getvalue().find(dom_name):
        raise Exception("Unable to start the VM on source host")
    remote_qemu = 'qemu+tcp://{hn}/system'.format(hn=client1.hostname)
    client0.run(args=['sudo', 'virsh', 'migrate', run.Raw('--live'), dom_name,
                      remote_qemu])
    time.sleep(4)
    out = StringIO.StringIO()
    client1.run(args=['sudo', 'virsh', 'list'], stdout=out)
    if not out.getvalue().find(dom_name):
        raise Exception("VM Migration Failed")


def cleanup(config):
    """
    Shutdown the VM and restore config file
    """
    dom_name = config.get('dom-name')
    qemu_test_dir = config.get('qemu-test-dir')
    log.info("Cleanup the test dir and restore config files")
    clients = [config.get('client0'), config.get('client1')]
    client1 = config.get('client1')
    client1.run(args=['sudo', 'virsh', 'shutdown', dom_name])
    client1.run(args=['sudo', 'virsh', 'destroy', dom_name])
    for client in clients:
        client.run(args=['sudo',
                         'cp',
                         '{qd}/libvirtd.conf'.format(qd=qemu_test_dir),
                         '/etc/libvirt/libvirtd.conf'])
        client.run(args=['sudo',
                         'cp',
                         '{qd}/libvirtd'.format(qd=qemu_test_dir),
                         '/etc/sysconfig/libvirtd'])
        client.run(args=['rm', run.Raw('-rf'), qemu_test_dir])
