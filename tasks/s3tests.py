from cStringIO import StringIO
from configobj import ConfigObj
import base64
import contextlib
import logging
import os
import random
import string

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user

log = logging.getLogger(__name__)

@contextlib.contextmanager
def download(ctx, config):
    assert isinstance(config, dict)
    log.info('Downloading s3-tests...')
    testdir = teuthology.get_testdir(ctx)
    for (client, cconf) in config.items():
        branch = cconf.get('force-branch', None)
        if not branch:
            ceph_branch = ctx.config.get('branch')
            suite_branch = ctx.config.get('suite_branch', ceph_branch)
            branch = cconf.get('branch', suite_branch)
        if not branch:
            raise ValueError(
                "Could not determine what branch to use for s3tests!")
        else:
            log.info("Using branch '%s' for s3tests", branch)
        sha1 = cconf.get('sha1')
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', branch,
#                'https://github.com/ceph/s3-tests.git',
                'git://ceph.com/git/s3-tests.git',
                '{tdir}/s3-tests'.format(tdir=testdir),
                ],
            )
        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/s3-tests'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                    ],
                )
    try:
        yield
    finally:
        log.info('Removing s3-tests...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    ],
                )


def _config_user(s3tests_conf, section, user):
    s3tests_conf[section].setdefault('user_id', user)
    s3tests_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    s3tests_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
    s3tests_conf[section].setdefault('access_key', ''.join(random.choice(string.uppercase) for i in xrange(20)))
    s3tests_conf[section].setdefault('secret_key', base64.b64encode(os.urandom(40)))


@contextlib.contextmanager
def create_users(ctx, config):
    assert isinstance(config, dict)
    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'s3 main': 'foo', 's3 alt': 'bar'}
    for client in config['clients']:
        s3tests_conf = config['s3tests_conf'][client]
        s3tests_conf.setdefault('fixtures', {})
        s3tests_conf['fixtures'].setdefault('bucket prefix', 'test-' + client + '-{random}-')
        for section, user in users.iteritems():
            _config_user(s3tests_conf, section, '{user}.{client}'.format(user=user, client=client))
            ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client,
                    'user', 'create',
                    '--uid', s3tests_conf[section]['user_id'],
                    '--display-name', s3tests_conf[section]['display_name'],
                    '--access-key', s3tests_conf[section]['access_key'],
                    '--secret', s3tests_conf[section]['secret_key'],
                    '--email', s3tests_conf[section]['email'],
                ],
            )
    try:
        yield
    finally:
        for client in config['clients']:
            for user in users.itervalues():
                uid = '{user}.{client}'.format(user=user, client=client)
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client,
                        'user', 'rm',
                        '--uid', uid,
                        '--purge-data',
                        ],
                    )


@contextlib.contextmanager
def configure(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring s3-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].iteritems():
        s3tests_conf = config['s3tests_conf'][client]
        if properties is not None and 'rgw_server' in properties:
            host = None
            for target, roles in zip(ctx.config['targets'].iterkeys(), ctx.config['roles']):
                log.info('roles: ' + str(roles))
                log.info('target: ' + str(target))
                if properties['rgw_server'] in roles:
                    _, host = split_user(target)
            assert host is not None, "Invalid client specified as the rgw_server"
            s3tests_conf['DEFAULT']['host'] = host
        else:
            s3tests_conf['DEFAULT']['host'] = 'localhost'

        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'cd',
                '{tdir}/s3-tests'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap',
                ],
            )
        conf_fp = StringIO()
        s3tests_conf.write(conf_fp)
        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

    log.info('Configuring boto...')
    boto_src = os.path.join(os.path.dirname(__file__), 'boto.cfg.template')
    for client, properties in config['clients'].iteritems():
        with file(boto_src, 'rb') as f:
            (remote,) = ctx.cluster.only(client).remotes.keys()
            conf = f.read().format(
                idle_timeout=config.get('idle_timeout', 30)
                )
            teuthology.write_file(
                remote=remote,
                path='{tdir}/boto.cfg'.format(tdir=testdir),
                data=conf,
                )

    try:
        yield

    finally:
        log.info('Cleaning up boto...')
        for client, properties in config['clients'].iteritems():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm',
                    '{tdir}/boto.cfg'.format(tdir=testdir),
                    ],
                )

@contextlib.contextmanager
def run_tests(ctx, config):
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.iteritems():
        args = [
            'S3TEST_CONF={tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
            'BOTO_CONFIG={tdir}/boto.cfg'.format(tdir=testdir),
            '{tdir}/s3-tests/virtualenv/bin/nosetests'.format(tdir=testdir),
            '-w',
            '{tdir}/s3-tests'.format(tdir=testdir),
            '-v',
            '-a', '!fails_on_rgw',
            ]
        if client_config is not None and 'extra_args' in client_config:
            args.extend(client_config['extra_args'])

        ctx.cluster.only(client).run(
            args=args,
            label="s3 tests against rgw"
            )
    yield

@contextlib.contextmanager
def task(ctx, config):
    """
    Run the s3-tests suite against rgw.

    To run all tests on all clients::

        tasks:
        - ceph:
        - rgw:
        - s3tests:

    To restrict testing to particular clients::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests: [client.0]

    To run against a server on client.1 and increase the boto timeout to 10m::

        tasks:
        - ceph:
        - rgw: [client.1]
        - s3tests:
            client.0:
              rgw_server: client.1
              idle_timeout: 600

    To pass extra arguments to nose (e.g. to run a certain test)::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests:
            client.0:
              extra_args: ['test_s3:test_object_acl_grand_public_read']
            client.1:
              extra_args: ['--exclude', 'test_100_continue']
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task s3tests only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    overrides = ctx.config.get('overrides', {})
    # merge each client section, not the top level.
    for client in config.iterkeys():
        if not config[client]:
            config[client] = {}
        teuthology.deep_merge(config[client], overrides.get('s3tests', {}))

    log.debug('config is %s', config)

    s3tests_conf = {}
    for client in clients:
        s3tests_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'DEFAULT':
                    {
                    'port'      : 7280,
                    'is_secure' : 'no',
                    },
                'fixtures' : {},
                's3 main'  : {},
                's3 alt'   : {},
                }
            )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                s3tests_conf=s3tests_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                s3tests_conf=s3tests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield
