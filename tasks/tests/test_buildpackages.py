# py.test -v -s tests/test_buildpackages.py

from mock import Mock

from .. import buildpackages

def test_lookup_configs():
    expected_system_type = 'deb'
    def make_remote():
        remote = Mock()
        remote.arch = 'x86_64'
        remote.os = Mock()
        remote.os.name = 'ubuntu'
        remote.os.version = '14.04'
        remote.os.codename = 'trusty'
        remote.system_type = expected_system_type
        return remote
    ctx = Mock()
    class cluster:
        remote1 = make_remote()
        remote2 = make_remote()
        remotes = {
            remote1: ['client.0'],
            remote2: ['mon.a','osd.0'],
        }
        def only(self, role):
            result = Mock()
            if role in ('client.0',):
                result.remotes = { cluster.remote1: None }
            elif role in ('osd.0', 'mon.a'):
                result.remotes = { cluster.remote2: None }
            else:
                result.remotes = None
            return result
    ctx.cluster = cluster()
    ctx.config = {
        'roles': [ ['client.0'], ['mon.a','osd.0'] ],
    }

    # nothing -> nothing
    assert buildpackages.lookup_configs(ctx, {}) == []
    assert buildpackages.lookup_configs(ctx, {1:[1,2,3]}) == []
    assert buildpackages.lookup_configs(ctx, [[1,2,3]]) == []
    assert buildpackages.lookup_configs(ctx, None) == []

    #
    # the overrides applies to install and to install.upgrade
    # that have no tag, branch or sha1
    #
    config = {
        'overrides': {
            'install': {
                'ceph': {
                    'sha1': 'overridesha1',
                    'tag': 'overridetag',
                    'branch': 'overridebranch',
                },
            },
        },
        'tasks': [
            {
                'install': {
                    'sha1': 'installsha1',
                },
            },
            {
                'install.upgrade': {
                    'osd.0': {
                    },
                    'client.0': {
                        'sha1': 'client0sha1',
                    },
                },
            }
        ],
    }
    ctx.config = config
    expected_configs = [{'branch': 'overridebranch', 'sha1': 'overridesha1', 'tag': 'overridetag'},
                        {'project': 'ceph', 'branch': 'overridebranch', 'sha1': 'overridesha1', 'tag': 'overridetag'},
                        {'project': 'ceph', 'sha1': 'client0sha1'}]

    assert buildpackages.lookup_configs(ctx, config) == expected_configs
