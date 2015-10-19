import logging
import os
import re
from teuthology.config import config as teuth_config
from subprocess import check_output

log = logging.getLogger(__name__)


def get_tag_branch_sha1(gitbuilder):
    """The install config may have contradicting tag/branch and sha1.
    When suite.py prepares the jobs, it always overrides the sha1 with
    whatever default is provided on the command line with --distro
    and what is found in the gitbuilder. If it turns out that the
    tag or the branch in the install config task is about another sha1,
    it will override anyway. For instance:

    install:
       tag: v0.94.1

    will be changed into

    install:
       tag: v0.94.1
       sha1: 12345

    even though v0.94.1 is not sha1 12345. This is does not cause
    problem with the install task because
    GitbuilderProject._get_uri_reference is used to figure out what to
    install from the gitbuilder and this function gives priority to
    the tag, if not found the branch, if not found the sha1.

    It is however confusing and this function returns a sha1 that is
    consistent with the tag or the branch being used.

    """

    uri_reference = gitbuilder.uri_reference
    url = gitbuilder.base_url
    assert '/' + uri_reference in url, \
        (url + ' (from template ' + teuth_config.baseurl_template +
         ') does not contain /' + uri_reference)
    log.info('uri_reference ' + uri_reference)
    if uri_reference.startswith('ref/'):
        ref = re.sub('^ref/', '', uri_reference) # do not use basename because the ref may contain a /
        ceph_git_url = teuth_config.get_ceph_git_url()
        cmd = "git ls-remote " + ceph_git_url + " " + ref
        output = check_output(cmd, shell=True)
        if not output:
            raise Exception(cmd + " returns nothing")
        lines = output.splitlines()
        if len(lines) != 1:
            raise Exception(
                cmd + " returns " + output +
                " which contains " + str(len(lines)) +
                " lines instead of exactly one")
        log.info(cmd + " returns " + lines[0])
        (sha1, ref) = lines[0].split()
        if ref.startswith('refs/heads/'):
            tag = None
            branch = re.sub('^refs/heads/', '', ref)
        elif ref.startswith('refs/tags/'):
            tag = re.sub('^refs/tags/', '', ref)
            branch = None
    else:
        sha1 = os.path.basename(uri_reference)
        tag = None
        branch = None
    return (tag, branch, sha1)

