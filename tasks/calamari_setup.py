"""
Calamari setup task
"""
import contextlib
import logging
import os
import requests
import shutil
import webbrowser
import json

from cStringIO import StringIO
from teuthology.orchestra import run
from teuthology import contextutil
from teuthology import misc

log = logging.getLogger(__name__)


DEFAULTS = {
    'version': 'v0.80.9',
    'calamari_branch': '1.3',
    'calamari-clients_branch': 'master',
    'diamond_branch': 'master',
    'start_browser': False,
    'email': 'x@y.com',
    'no_epel': True,
    'calamari_user': 'admin',
    'calamari_password': 'admin',
}

UPSTREAM_PROJECTS = (
    'diamond',
    'calamari',
    'calamari-clients',
)

@contextlib.contextmanager
def task(ctx, config):
    """
    Do the setup of a calamari server.

    - calamari_setup:
        version: 'v0.80.9'
        calamari_branch: '1.3'

    Options are (see DEFAULTS above):

    version -- ceph version we are testing against
    calamari_branch -- upstream calamari branch
    calamari-clients_branch -- upstream calamari-clients branch
    diamond_branch -- upstream diamond branch
    start_browser -- If True, start a browser.  To be used by runs that will
                     bring up a browser quickly for human use.  Set to False
                     for overnight suites that are testing for problems in
                     the installation itself
    email -- email address for the user
    no_epel -- indicates if we should remove epel files prior to yum
               installations.
    calamari_user -- user name to log into gui
    calamari_password -- calamari user password
    """
    local_config = DEFAULTS
    local_config.update(config)
    config = local_config
    cal_svr = None
    for remote_, roles in ctx.cluster.remotes.items():
        if 'client.0' in roles:
            cal_svr = remote_
            break
    if not cal_svr:
        raise RuntimeError('client.0 not found in roles')
    with contextutil.nested(
        lambda: adjust_yum_repos(ctx, cal_svr, config),
        lambda: calamari_install(config, cal_svr),
        lambda: ceph_install(ctx, cal_svr),
        # do it again because ceph-deploy installed epel for centos
        lambda: remove_epel(ctx, config['no_epel']),
        lambda: calamari_connect(ctx, cal_svr),
        lambda: browser(config['start_browser'], cal_svr.hostname),
    ):
        yield


@contextlib.contextmanager
def adjust_yum_repos(ctx, cal_svr, config):
    """
    For each remote machine, fix the repos if yum is used.
    """
    _distro = str(cal_svr.os)
    if _distro.startswith('rhel') or _distro.startswith('centos'):
        if config['no_epel']:
            for remote in ctx.cluster.remotes:
                fix_yum_repos(remote, _distro, config)
    try:
        yield
    finally:
        if _distro.startswith('rhel') or _distro.startswith('centos'):
            if config['no_epel']:
                for remote in ctx.cluster.remotes:
                    remove_yum_repos(remote)


def remove_yum_repos(remote):
    """
    Remove the repo file that we created.
    """
    for project in UPSTREAM_PROJECTS:
        if remote.run(args=['sudo', 'rm', '-f', '/etc/yum.repos.d/%s.repo' % project]).exitstatus:
            return False
    return True


def fix_yum_repos(remote, distro, config):
    """
    For yum calamari installations, the repos.d directory should only
    contain a repo file named rhel<version-number>.repo
    """
    platform = distro.split(' ')[0]
    if platform not in ['centos', 'rhel']:
        raise RuntimeError('Unknow platform "%s"' % platform)

    major = distro.split(' ')[1].split('.')[0]
    minor = distro.split(' ')[1].split('.')[1]

    # Query shaman for the diamond and calamari repo urls and create the repo files
    url = {}
    for project in UPSTREAM_PROJECTS:
        _shaman_url = 'https://shaman.ceph.com/api/search/?project=%s&distros=centos/%s&ref=%s&sha1=latest' % (project, major, config[project + '_branch'])
        _response = requests.get(_shaman_url)
        if not _response.ok:
            raise RuntimeError('Failed to query shaman (%s) for %s' % (_shaman_url, project))
        _data = json.loads(_response.text)
        if len(_data) != 1:
            raise RuntimeError('Shaman did not return a single item for %s' % project)
        _data = _data[0]
        if not _data.has_key('url')
            raise RuntimeError('Shaman response did not contain url for %s' % project)
        _url = _data['url']
        _contents = '\n'.join([
            '[%s]' % project,
            'name=%s $releasever - $basearch' % project,
            'baseurl=' + _url,
            'gpgcheck=0',
            'enabled=1',
        ])
        misc.sudo_write_file(remote, '/etc/yum.repos.d/%s.repo' % project, _contents)

    cmds = [
        'sudo yum clean all'.split(),
        'sudo yum makecache'.split(),
    ]
    for cmd in cmds:
        if remote.run(args=cmd).exitstatus:
            return False
    return True


@contextlib.contextmanager
def calamari_install(config, cal_svr):
    """
    Install calamari

    The steps here are:
        -- Install calamari and calamari-clients from the chacra repos
        -- Run calamari-ctl initialize.
    """
    client_id = str(cal_svr)
    at_loc = client_id.find('@')
    if at_loc > 0:
        client_id = client_id[at_loc + 1:]

    # install calamari packages
    args = {
        'deb': 'sudo apt-get install calamari calamari-clients', # TODO: always say yes
        'rpm': 'sudo yum -y install calamari calamari-clients'
    }.get(cal_svr.system_type, None)
    if not args:
        raise RuntimeError('{0}: unknown system type'.format(cal_svr))
    ret = cal_svr.run(args=args)
    if ret.exitstatus:
        raise RuntimeError('calamari package install failed')

    # Run calamari-ctl initialize.
    _data = '%s\n%s\n%s\n%s\n' % (
        config['calamari_user'],
        config['email'],
        config['calamari_password'],
        config['calamari_password'],
    )
    _in = StringIO(_data)
    _out = StringIO()
    ret = cal_svr.run(args=['sudo', 'calamari-ctl', 'initialize'],
                      stdin=_in, stdout=_out)
    log.debug(_out.getvalue())
    if ret.exitstatus:
        raise RuntimeError('calamari-ctl initialize failed')
    try:
        yield
    finally:
        log.info('Cleaning up after Calamari installation')
        # Remove calamari and calamari-clients
        args = {
            'deb': 'sudo apt-get remove calamari calamari-clients', # TODO: always say yes
            'rpm': 'sudo yum -y remove calamari calamari-clients'
        }.get(cal_svr.system_type, None)
        cal_svr.run(args=args)


@contextlib.contextmanager
def ceph_install(ctx, cal_svr):
    """
    Install ceph if ceph was not previously installed by teuthology.  This
    code tests the case where calamari is installed on a brand new system.
    """
    loc_inst = False
    if 'install' not in [x.keys()[0] for x in ctx.config['tasks']]:
        loc_inst = True
        ret = deploy_ceph(ctx, cal_svr)
        if ret:
            raise RuntimeError('ceph installs failed')
    try:
        yield
    finally:
        if loc_inst:
            if not undeploy_ceph(ctx, cal_svr):
                log.error('Cleanup of Ceph installed by Calamari-setup failed')


def deploy_ceph(ctx, cal_svr):
    """
    Perform the ceph-deploy actions needed to bring up a Ceph cluster.  This
    test is needed to check the ceph-deploy that comes with the calamari
    package.
    """
    osd_to_name = {}
    all_machines = set()
    all_mons = set()
    all_osds = set()

    # collect which remotes are osds and which are mons
    for remote in ctx.cluster.remotes:
        all_machines.add(remote.shortname)
        roles = ctx.cluster.remotes[remote]
        for role in roles:
            daemon_type, number = role.split('.')
            if daemon_type == 'osd':
                all_osds.add(remote.shortname)
                osd_to_name[number] = remote.shortname
            if daemon_type == 'mon':
                all_mons.add(remote.shortname)

    # figure out whether we're in "1.3+" mode: prior to 1.3, there was
    # only one Ceph repo, and it was all installed on every Ceph host.
    # with 1.3, we've split that into MON and OSD repos (in order to
    # be able to separately track subscriptions per-node).  This
    # requires new switches to ceph-deploy to select which locally-served
    # repo is connected to which cluster host.
    #
    # (TODO: A further issue is that the installation/setup may not have
    # created local repos at all, but that is the subject of a future
    # change.)

    r = cal_svr.run(args='/usr/bin/test -d /mnt/MON', check_status=False)
    use_install_repo = (r.returncode == 0)

    # pre-1.3:
    # ceph-deploy new <all_mons>
    # ceph-deploy install <all_machines>
    # ceph-deploy mon create-initial
    #
    # 1.3 and later:
    # ceph-deploy new <all_mons>
    # ceph-deploy install --repo --release=ceph-mon <all_mons>
    # ceph-deploy install <all_mons>
    # ceph-deploy install --repo --release=ceph-osd <all_osds>
    # ceph-deploy install <all_osds>
    # ceph-deploy mon create-initial
    #
    # one might think the install <all_mons> and install <all_osds>
    # commands would need --mon and --osd, but #12147 has not yet
    # made it into RHCS 1.3.0; since the package split also hasn't
    # landed, we can avoid using the flag and avoid the bug.

    cmds = ['ceph-deploy new ' + ' '.join(all_mons)]

    if use_install_repo:
        cmds.append('ceph-deploy repo ceph-mon ' +
                    ' '.join(all_mons))
        cmds.append('ceph-deploy install --no-adjust-repos --mon ' +
                    ' '.join(all_mons))
        cmds.append('ceph-deploy repo ceph-osd ' +
                    ' '.join(all_osds))
        cmds.append('ceph-deploy install --no-adjust-repos --osd ' +
                    ' '.join(all_osds))
        # We tell users to use `hostname` in our docs. Do the same here.
        cmds.append('ceph-deploy install --no-adjust-repos --cli `hostname`')
    else:
        cmds.append('ceph-deploy install ' + ' '.join(all_machines))

    cmds.append('ceph-deploy mon create-initial')

    for cmd in cmds:
        cal_svr.run(args=cmd).exitstatus

    disk_labels = '_dcba'
    # NEEDS WORK assumes disks start with vd (need to check this somewhere)
    for cmd_pts in [['disk', 'zap'], ['osd', 'prepare'], ['osd', 'activate']]:
        mach_osd_cnt = {}
        for osdn in osd_to_name:
            osd_mac = osd_to_name[osdn]
            mach_osd_cnt[osd_mac] = mach_osd_cnt.get(osd_mac, 0) + 1
            arg_list = ['ceph-deploy']
            arg_list.extend(cmd_pts)
            disk_id = '%s:vd%s' % (osd_to_name[osdn],
                                   disk_labels[mach_osd_cnt[osd_mac]])
            if 'activate' in cmd_pts:
                disk_id += '1'
            arg_list.append(disk_id)
            cal_svr.run(args=arg_list).exitstatus


def undeploy_ceph(ctx, cal_svr):
    """
    Cleanup deployment of ceph.
    """
    all_machines = []
    ret = True
    for remote in ctx.cluster.remotes:
        roles = ctx.cluster.remotes[remote]
        if (
            not any('osd' in role for role in roles) and
            not any('mon' in role for role in roles)
        ):
            continue
        ret &= remote.run(
            args=['sudo', 'stop', 'ceph-all', run.Raw('||'),
                  'sudo', 'service', 'ceph', 'stop']
        ).exitstatus
        all_machines.append(remote.shortname)
    all_machines = set(all_machines)
    cmd1 = ['ceph-deploy', 'uninstall']
    cmd1.extend(all_machines)
    ret &= cal_svr.run(args=cmd1).exitstatus
    cmd2 = ['ceph-deploy', 'purge']
    cmd2.extend(all_machines)
    ret &= cal_svr.run(args=cmd2).exitstatus
    for remote in ctx.cluster.remotes:
        ret &= remote.run(args=['sudo', 'rm', '-rf',
                                '.ssh/known_hosts']).exitstatus
    return ret


@contextlib.contextmanager
def calamari_connect(ctx, cal_svr):
    """
    Connect calamari to the ceph nodes.
    """
    connects = ['ceph-deploy', 'calamari', 'connect']
    for machine_info in ctx.cluster.remotes:
        if 'client.0' not in ctx.cluster.remotes[machine_info]:
            connects.append(machine_info.shortname)
    ret = cal_svr.run(args=connects)
    if ret.exitstatus:
        raise RuntimeError('calamari connect failed')
    try:
        yield
    finally:
        log.info('Calamari test terminating')


@contextlib.contextmanager
def browser(start_browser, web_page):
    """
    Bring up a browser, if wanted.
    """
    if start_browser:
        webbrowser.open('http://%s' % web_page)
    try:
        yield
    finally:
        if start_browser:
            log.info('Web browser support terminating')
