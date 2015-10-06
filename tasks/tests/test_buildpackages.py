# py.test -v -s tests/test_buildpackages.py

import pytest
from mock import patch, Mock, DEFAULT

from .. import buildpackages
from teuthology import packaging

def test_get_tag_branch_sha1():
    gitbuilder = packaging.GitbuilderProject(
        'ceph',
        {
            'os_type': 'centos',
            'os_version': '7.0',
        })
    (tag, branch, sha1) = buildpackages.get_tag_branch_sha1(gitbuilder)
    assert tag == None
    assert branch == None
    assert sha1 is not None

    gitbuilder = packaging.GitbuilderProject(
        'ceph',
        {
            'os_type': 'centos',
            'os_version': '7.0',
            'sha1': 'asha1',
        })
    (tag, branch, sha1) = buildpackages.get_tag_branch_sha1(gitbuilder)
    assert tag == None
    assert branch == None
    assert sha1 == 'asha1'

    remote = Mock
    remote.arch = 'x86_64'
    remote.os = Mock
    remote.os.name = 'ubuntu'
    remote.os.version = '14.04'
    remote.os.codename = 'trusty'
    remote.system_type = 'deb'
    ctx = Mock
    ctx.cluster = Mock
    ctx.cluster.remotes = {remote: ['client.0']}

    expected_tag = 'v0.94.1'
    expected_sha1 = 'expectedsha1'
    def check_output(cmd, shell):
        assert shell == True
        return expected_sha1 + " refs/tags/" + expected_tag
    with patch.multiple(
            buildpackages,
            check_output=check_output,
    ):
        gitbuilder = packaging.GitbuilderProject(
            'ceph',
            {
                'os_type': 'centos',
                'os_version': '7.0',
                'sha1': 'asha1',
                'all': {
                    'tag': tag,
                },
            },
            ctx = ctx,
            remote = remote)
        (tag, branch, sha1) = buildpackages.get_tag_branch_sha1(gitbuilder)
        assert tag == expected_tag
        assert branch == None
        assert sha1 == expected_sha1

    expected_branch = 'hammer'
    expected_sha1 = 'otherexpectedsha1'
    def check_output(cmd, shell):
        assert shell == True
        return expected_sha1 + " refs/heads/" + expected_branch
    with patch.multiple(
            buildpackages,
            check_output=check_output,
    ):
        gitbuilder = packaging.GitbuilderProject(
            'ceph',
            {
                'os_type': 'centos',
                'os_version': '7.0',
                'sha1': 'asha1',
                'all': {
                    'branch': branch,
                },
            },
            ctx = ctx,
            remote = remote)
        (tag, branch, sha1) = buildpackages.get_tag_branch_sha1(gitbuilder)
        assert tag == None
        assert branch == expected_branch
        assert sha1 == expected_sha1
