# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import unicode_literals
# from __future__ import absolute_import

import calendar
from collections import Mapping
import copy
import json
import os
import re
import time

import messageparams
from loghandler import LogHandler
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap
from pyLibrary.env.pulse import Consumer
from pyLibrary.meta import use_settings
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date


class PulseBuildbotTranslator(object):

    @use_settings
    def __init__(
        self,
        durable=False,
        logdir='logs',
        message=None,
        display_only=False,
        no_output=False,
        consumer_cfg=None,
        publisher_cfg=None,
        settings=None
    ):
        self.settings = settings
        self.loghandler = LogHandler(self.settings.destination)

    def start(self):
        if self.settings.message:
            # handle a test message
            json_data = open(self.settings.message)
            data = json.load(json_data)
            self.on_pulse_message(data)
            return

        with Consumer(target=self.on_pulse_message, settings=self.settings.source):
            Thread.wait_for_shutdown_signal()

    def process_unittest(self, data):
        data.insertion_time = calendar.timegm(time.gmtime())
        if not data.get('logurl'):
            Log.error("No log URL in {{key|quote}}", data)
        if data.platform not in messageparams.platforms:
            Log.error("Bad platform in {{key|quote}}", data)
        elif data.os not in messageparams.platforms[data.platform]:
            Log.error("Bad OS {{os}} in {{key|quote}}", data)

        if not self.settings.destination:
            return
        if not isinstance(self.settings.destination, Mapping):
            Log.note("Test properties:\n{{data|indent}}", data=data)
            return

        self.loghandler.handle_message(data)

    def process_build(self, data):
        if data.platform not in messageparams.platforms:
            Log.error("Bad platform {{platform|quote}} in {{key|quote}}", data)
        for tag in data.tags:
            if tag not in messageparams.tags:
                Log.error("Bad tag {{tag|quote}} in {{key|quote}}", data, tag=tag)
        # Repacks do not have a buildurl included. We can remove this
        # workaround once bug 857971 has been fixed
        if not data.buildurl and not data.repack:
            Log.warning("No build URL in {{key|quote}}", data)

        if not self.settings.destination:
            return
        if not isinstance(self.settings.destination, Mapping):
            Log.note("Build properties:\n{{data}}\n", data=data)
            return

        self.loghandler.handle_message(data)

    def on_pulse_message(self, data):
        data = wrap(data)
        if data._meta.routing_key == "heartbeat":
            Log.note("heartbeat")
            return

        stage_platform = None
        key = data._meta.routing_key

        # Create a dict that holds build properties that apply to both
        # unittests and builds.
        builddata = Dict(key=key)

        # scan the payload for properties applicable to both tests and
        # builds
        for k, v, source in data.payload.build.properties:

            # look for the job number
            if k == 'buildnumber':
                builddata.job_number = v

            # look for revision
            if k == 'revision':
                builddata.revision = v

            # look for product
            elif k == 'product':
                # Bug 1010120:
                # Ensure to lowercase to prevent issues with capitalization
                builddata.product = v.lower()

            # look for version
            elif k == 'version':
                builddata.version = v

            # look for tree
            elif k == 'branch':
                builddata.tree = v
                # For builds, this property is sometimes a relative path,
                # ('releases/mozilla-beta') and not just a name.  For
                # consistency, we'll strip the path components.
                if isinstance(builddata.tree, basestring):
                    builddata.tree = os.path.basename(builddata.tree)

            # look for buildid
            elif k == 'buildid':
                builddata.buildid = v
                builddata.builddate = Date(v)

            # look for the build number which comes with candidate builds
            elif k == 'build_number':
                builddata.build_number = v

            # look for the previous buildid
            elif k == 'previous_buildid':
                builddata.previous_buildid = v

            # look for platform
            elif k == 'platform':
                builddata.platform = v
                if (builddata.platform and
                    '-debug' in builddata.platform):
                    # strip '-debug' from the platform string if it's
                    # present
                    builddata.platform = builddata.platform[
                        0:builddata.platform.find('-debug')]

            # look for the locale
            elif k == 'locale':
                builddata.locale = v

            # look for the locale
            elif k == 'locales':
                builddata.locales = v

            # look for build url
            elif k in ['packageUrl', 'build_url', 'fileURL']:
                builddata.buildurl = v

            # look for log url
            elif k == 'log_url':
                builddata.logurl = v

            # look for release name
            elif k in ['en_revision', 'script_repo_revision']:
                builddata.release = v

            # look for tests url
            elif k == 'testsUrl':
                builddata.testsurl = v

            # look for buildername
            elif k == 'buildername':
                builddata.buildername = v

            # look for slave builder
            elif k == 'slavename':
                builddata.slave = v

            # look for blobber files
            elif k == 'blobber_files':
                try:
                    builddata.blobber_files = json.loads(v)
                except ValueError:
                    Log.error("Malformed `blobber_files` buildbot property: {{json}}", json=v)

            # look for stage_platform
            elif k == 'stage_platform':
                # For some messages, the platform we really care about
                # is in the 'stage_platform' property, not the 'platform'
                # property.
                stage_platform = v
                for buildtype in messageparams.buildtypes:
                    if buildtype in stage_platform:
                        stage_platform = stage_platform[0:stage_platform.find(buildtype) - 1]

            elif k == 'completeMarUrl':
                builddata.completemarurl = v

            elif k == 'completeMarHash':
                builddata.completemarhash = v

        if not builddata.tree:
            Log.error("{{key|quote}} no 'branch' property", key=key)

        # If no locale is given fallback to en-US
        if not builddata.locale:
            builddata.locale = 'en-US'

        # status of the build or test notification
        # see http://hg.mozilla.org/build/buildbot/file/08b7c51d2962/master/buildbot/status/builder.py#l25
        builddata.status = data.payload.build.results

        if 'debug' in key:
            builddata.buildtype = 'debug'
        elif 'pgo' in key:
            builddata.buildtype = 'pgo'
        else:
            builddata.buildtype = 'opt'

        # see if this message is for a unittest
        unittestRe = re.compile(r'build\.((%s)[-|_](.*?)(-debug|-o-debug|-pgo|_pgo|_test)?[-|_](test|unittest|pgo)-(.*?))\.(\d+)\.(log_uploaded|finished)' %
                                builddata.tree)
        match = unittestRe.match(key)
        if match:
            # for unittests, generate some metadata by parsing the key

            if match.groups()[7] == 'finished':
                # Ignore this message, we only care about 'log_uploaded'
                # messages for unittests.
                return

            # The 'short_builder' string is quite arbitrary, and so this
            # code is expected to be fragile, and will likely need
            # frequent maintenance to deal with future changes to this
            # string.  Unfortunately, these items are not available
            # in a more straightforward fashion at present.
            short_builder = match.groups()[0]

            builddata.os = match.groups()[2]
            if builddata.os in messageparams.os_conversions:
                builddata.os = messageparams.os_conversions[
                    builddata.os](builddata)

            builddata.test = match.groups()[5]

            # yuck!!
            if builddata.test.endswith('_2'):
                short_builder = "%s.2" % short_builder[0:-2]
            elif builddata.test.endswith('_2-pgo'):
                short_builder = "%s.2-pgo" % short_builder[0:-6]

            builddata.talos = 'talos' in builddata.buildername

            if stage_platform:
                builddata.platform = stage_platform

            self.process_unittest(builddata)
        elif 'source' in key:
            # what is this?
            # ex: build.release-mozilla-esr10-firefox_source.0.finished
            pass
        elif [x for x in ['schedulers', 'tag', 'submitter', 'final_verification', 'fuzzer'] if x in key]:
            # internal buildbot stuff we don't care about
            # ex: build.release-mozilla-beta-firefox_reset_schedulers.12.finished
            # ex: build.release-mozilla-beta-fennec_tag.40.finished
            # ex: build.release-mozilla-beta-bouncer_submitter.46.finished
            pass
        elif 'jetpack' in key:
            # These are very awkwardly formed; i.e.
            # build.jetpack-mozilla-central-win7-debug.18.finished,
            # and the tree appears nowhere except this string.  In order
            # to support these we'd have to keep a tree map of all
            # possible trees.
            pass
        else:
            if not builddata.platform:
                if stage_platform:
                    builddata.platform = stage_platform
                else:
                    # Some messages don't contain the platform
                    # in any place other than the routing key, so we'll
                    # have to guess it based on that.
                    builddata.platform = messageparams.guess_platform(key)
                    if not builddata.platform:
                        Log.error("{{key|quote}} no \"platform\" property", key=key)

            otherRe = re.compile(r'build\.((release-|jetpack-|b2g_)?(%s)[-|_](xulrunner[-|_])?(%s)([-|_]?)(.*?))\.(\d+)\.(log_uploaded|finished)' %
                                 (builddata.tree, builddata.platform))
            match = otherRe.match(key)
            if match:
                if 'finished' in match.group(9):
                    # Ignore this message, we only care about 'log_uploaded'
                    # messages for builds
                    return

                builddata.tags = match.group(7).replace('_', '-').split('-')

                # There are some tags we don't care about as tags,
                # usually because they are redundant with other properties,
                # so remove them.
                notags = ['debug', 'pgo', 'opt', 'repack']
                builddata.tags = [x for x in builddata.tags if x not in notags]

                # Sometimes a tag will just be a digit, i.e.,
                # build.mozilla-central-android-l10n_5.12.finished;
                # strip these.
                builddata.tags = [x for x in builddata.tags if not x.isdigit()]

                if isinstance(match.group(2), basestring):
                    if 'release' in match.group(2):
                        builddata.tags.append('release')
                    if 'jetpack' in match.group(2):
                        builddata.tags.append('jetpack')

                if match.group(4) or 'xulrunner' in builddata.tags:
                    builddata.product = 'xulrunner'

                # Sadly, the build url for emulator builds isn't published
                # to the pulse stream, so we have to guess it.  See bug
                # 1071642.
                if ('emulator' in builddata.get('platform', '') and
                        'try' not in key and builddata.get('buildid')):
                    builddata.buildurl = (
                        'https://pvtbuilds.mozilla.org/pub/mozilla.org/b2g/tinderbox-builds' +
                        '/%s-%s/%s/emulator.tar.gz' %
                        (builddata.tree, builddata.platform,
                         builddata.buildid))

                # In case of repacks we have to send multiple notifications,
                # each for every locale included. We can remove this
                # workaround once bug 857971 has been fixed.
                if 'repack' in key:
                    builddata.repack = True

                    if not builddata["locales"]:
                        Log.error("Repack with no locales in {{key|quote}}", key=key)

                    for locale in builddata["locales"].split(','):
                        if not locale:
                            Log.error("{{key|quote}} bad locals {{locales}}", builddata, key=key)

                        data = copy.deepcopy(builddata)
                        data.locale = locale
                        self.process_build(data)

                else:
                    self.process_build(builddata)
            else:
                Log.error("Unknown message type: {{key|quote}}", key=key)

