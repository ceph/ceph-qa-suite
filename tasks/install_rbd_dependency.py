"""
  Install any packages on all distro's that are required to run RBD tests
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology.task import ansible
from teuthology import contextutil
log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
      Installs any dependency packages that are required for RBD tests to run
    """
    hosts = []
    clients = ctx.cluster.only(teuthology.is_type('client'))
    remotes = clients.remotes
    log.info("Installing rbd dependency packages")
    rpm_deps = ['qemu-kvm', 'libvirt']
    deb_deps = ['qemu-system-x86', 'libvirt']
    for client in remotes.keys():
        hosts.append(client.shortname)
    log.info("Clients are %s", hosts)
    ansible_config = {
        'playbook': 'packages.yml',
        'cleanup': 'true',
        'hosts': hosts,
        'vars': {
            'yum_packages': rpm_deps,
            'apt_packages': deb_deps}}
    ansible.CephLab(ctx, config=ansible_config)
    with contextutil.nested(lambda: ansible.CephLab(ctx, config=ansible_config)):
        yield
