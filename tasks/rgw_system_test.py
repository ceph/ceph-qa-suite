import yaml
import os
import shutil
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run
log = logging.getLogger(__name__)


def test_Mbuckets(config, clients):
    """
        make sure that rgw is installed in client.0

        tasks:
        - rgw-system-test:
            test-name: test_m_bucket
            config:
                user_count: 3
                bucket_count: 5

        """

    log.info('test: test_Mbuckets')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            objects_count=0,

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_Mbuckets.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_Mbuckets.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' % clients[
        0].hostname
    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_Mbuckets.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_Mbuckets.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_Mbuckets_with_Nobjects(config, clients):
    """
        make sure that rgw is installed in client.0

        tasks:
        - rgw-system-test:
            test-name: test_m_bucket_n_objects
            config:
                user_count: 3
                bucket_count: 5
                object_count: 5
                min_file_size: 5
                max_file_size: 10

        """

    log.info('test: test_Mbuckets_with_Nobjects')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_Mbuckets_with_Nobjects.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_Mbuckets_with_Nobjects.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_Mbuckets_with_Nobjects.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_Mbuckets_with_Nobjects.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_bucket_with_delete(config, clients):
    """
            make sure that rgw is installed in client.0

            tasks:
            - rgw-system-test:
                test-name: test_bucket_with_delete
                config:
                    user_count: 3
                    bucket_count: 5
                    object_count: 5
                    min_file_size: 5
                    max_file_size: 10

            """

    log.info('test: test_bucket_with_delete')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_bucket_with_delete.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_bucket_with_delete.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_bucket_with_delete.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_bucket_with_delete.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_multipart_upload(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_multipart_upload
           config:
               user_count: 3
               bucket_count: 5
               min_file_size: 5
               max_file_size: 10

    """

    log.info('test: test_multipart_upload')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_multipart_upload.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_multipart_upload.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_multipart_upload.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_multipart_upload.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_multipart_upload_download(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_multipart_upload_download
           config:
               user_count: 3
               bucket_count: 5
               min_file_size: 5
               max_file_size: 10

    """

    log.info('test: test_multipart_upload_download')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_multipart_upload_download.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_multipart_upload_download.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_multipart_upload_download.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_multipart_upload_download.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_multpart_upload_cancel(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_multipart_upload_download
           config:
               user_count: 3
               bucket_count: 5
               break_at_part_no: 90
               min_file_size: 1500
               max_file_size: 1000

    """

    log.info('test: test_multipart_upload_cancel')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            break_at_part_no=config['break_at_part_no'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_multipart_upload_cancel.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_multipart_upload_cancel.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_multpart_upload_cancel.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_multipart_upload_cancel.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_basic_versioning(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_basic_versioning
           config:
               user_count: 3
               bucket_count: 5
               objects_count: 90
               version_count: 5
               min_file_size: 1000
               max_file_size: 1500

    """

    log.info('test: test_basic_versioning')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            version_count=config['version_count'],
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_basic_versioning.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_basic_versioning.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_basic_versioning.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_basic_versioning.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_delete_key_versions(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_delete_key_versions
           config:
               ;

    """

    log.info('test: test_delete_key_versions')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            version_count=config['version_count'],
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_delete_key_versions.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_delete_key_versions.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_delete_key_versions.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_delete_key_versions.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_suspend_versioning(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_suspend_versioning
           config:
               user_count: 3
               bucket_count: 5
               objects_count: 90
               version_count: 5
               min_file_size: 1000
               max_file_size: 1500

    """

    log.info('test: test_suspend_versioning')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            version_count=config['version_count'],
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_suspend_versioning.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_suspend_versioning.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_suspend_versioning.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_suspend_versioning.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_version_with_revert(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_version_with_revert
           config:
               user_count: 3
               bucket_count: 5
               objects_count: 90
               version_count: 5
               min_file_size: 1000
               max_file_size: 1500

    """

    log.info('test: test_version_with_revert')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['user_count'], int), "supports only integers"
    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            user_count=config['user_count'],
            bucket_count=config['bucket_count'],
            version_count=config['version_count'],
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_version_with_revert.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_version_with_revert.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_version_with_revert.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_version_with_revert.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_basic_acls(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_acls
           config:
               bucket_count: 5
               objects_count: 90
               min_file_size: 1000
               max_file_size: 1500

    """

    log.info('test: test_acls')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            bucket_count=config['bucket_count'],
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_acls.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_acls.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_acls.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_acls.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_acls_all_usrs(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_acls_all_usrs
           config:
               bucket_count: 5
               user_count: 3
               objects_count: 90
               min_file_size: 1000
               max_file_size: 1500

    """

    log.info('test: test_acls_all_usrs')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    assert isinstance(config['bucket_count'], int), "supports only integers"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            bucket_count=config['bucket_count'],
            user_count=config['user_count'],
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_acls_all_usrs.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_acls.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_acls_all_usrs.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_acls_all_usrs.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_acls_copy_obj(config, clients):
    """
       make sure that rgw is installed in client.0

       tasks:
       - rgw-system-test:
           test-name: test_acls_copy_obj
           config:
               objects_count: 90
               min_file_size: 1000
               max_file_size: 1500

    """

    log.info('test: test_acls_copy_obj')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_acls_copy_obj.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_acls.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_acls_copy_obj.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_acls_copy_obj.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


def test_acls_reset(config, clients):
    """
           make sure that rgw is installed in client.0

           tasks:
           - rgw-system-test:
               test-name: test_acls_reset
               config:
                   bucket_count: 5
                   user_count: 3
                   objects_count: 90
                   min_file_size: 1000
                   max_file_size: 1500

    """

    log.info('test: test_acls_reset')

    if config is None:
        assert isinstance(config, dict), "configuration not given"

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')

    if not os.path.exists('/tmp/rgwtmp'):
        os.makedirs('/tmp/rgwtmp')

    log.info('created tmp dir in local machine')

    data = dict(
        config=dict(
            objects_count=config['objects_count'],
            objects_size_range=dict(
                min=config['min_file_size'],
                max=config['max_file_size']
            )

        )
    )

    log.info('creating yaml from the config: %s' % data)

    with open('/tmp/rgwtmp/test_acls_reset.yaml', 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    # local_file = '/tmp/rgwtmp/test_Mbuckets.yaml'
    # destination_location = '/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/'

    # clients[0].put_file(local_file,  destination_location)

    scp_cmd = 'scp /tmp/rgwtmp/test_acls.yaml %s:~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/' \
              % clients[0].hostname

    os.system(scp_cmd)

    clients[0].run(
        args=[
            run.Raw(
                'source venv/bin/activate; python ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/test_acls_reset.py '
                '-c ~/rgw-tests/ceph-qe-scripts/rgw/tests/s3/yamls/test_acls_reset.yaml -p 8080')])

    if os.path.exists('/tmp/rgwtmp'):
        shutil.rmtree('/tmp/rgwtmp')


@contextlib.contextmanager
def task(ctx, config):

    log.info('starting the task')

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    remotes = ctx.cluster.only(teuthology.is_type('client'))
    clients = [
        remote for remote,
        roles_for_host in remotes.remotes.iteritems()]

    log.info('cloning the repo to client.0 machines')

    clients[0].run(args=['sudo', 'rm', '-rf', 'rgw-tests'], check_status=False)
    clients[0].run(
        args=[
            'sudo',
            'rm',
            '-rf',
            run.Raw('/tmp/apilog*')],
        check_status=False)
    clients[0].run(args=['mkdir', 'rgw-tests'])
    clients[0].run(
        args=[
            'cd',
            'rgw-tests',
            run.Raw(';'),
            'git',
            'clone',
            'http://gitlab.osas.lab.eng.rdu2.redhat.com/ceph/ceph-qe-scripts.git',
            '-b',
            'wip_rgw'])

    clients[0].run(args=['virtualenv', 'venv'])
    clients[0].run(
        args=[
            'source',
            'venv/bin/activate',
            run.Raw(';'),
            run.Raw('pip install boto names PyYaml'),
            run.Raw(';'),
            'deactivate'])

    # basic Upload

    if config['test-name'] == 'test_m_bucket':
        test_Mbuckets(config['config'], clients)

    if config['test-name'] == 'test_m_bucket_n_objects':
        test_Mbuckets_with_Nobjects(config['config'], clients)

    if config['test-name'] == 'test_bucket_with_delete':
        test_bucket_with_delete(config['config'], clients)

    # multipart

    if config['test-name'] == 'test_multipart_upload':
        test_multipart_upload(config['config'], clients)

    if config['test-name'] == 'test_multipart_upload_download':
        test_multipart_upload_download(config['config'], clients)

    if config['test-name'] == 'test_multipart_upload_cancel':
        test_multpart_upload_cancel(config['config'], clients)

    # Versioning

    if config['test-name'] == 'test_basic_versioning':
        test_basic_versioning(config['config'], clients)

    if config['test-name'] == 'test_delete_key_versions':
        test_delete_key_versions(config['config'], clients)

    if config['test-name'] == ' test_suspend_versioning]':
        test_suspend_versioning(config['config'], clients)

    if config['test-name'] == ' test_version_with_revert':
        test_version_with_revert(config['config'], clients)

    # ACLs

    if config['test-name'] == 'test_acls':
        test_basic_acls(config['config'], clients)

    if config['test-name'] == 'test_acls_all_usrs':
        test_acls_all_usrs(config['config'], clients)

    if config['test-name'] == 'test_acls_copy_obj':
        test_acls_copy_obj(config['config'], clients)

    if config['test-name'] == 'test_acls_reset':
        test_acls_reset(config['config'], clients)

    try:
        yield
    finally:

        clients[0].run(
            args=[
                'source',
                'venv/bin/activate',
                run.Raw(';'),
                run.Raw('pip uninstall boto names PyYaml -y'),
                run.Raw(';'),
                'deactivate'])

        log.info('test completed')

        log.info("Deleting repos")

        clients[0].run(args=[run.Raw(
            'sudo rm -rf venv rgw-tests *.json Download.* Download *.mpFile  x* key.*  Mp.* *.key.*')])
