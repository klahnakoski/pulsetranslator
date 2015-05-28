# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import calendar
import httplib
import time
from urlparse import urlparse

from mozillapulse.publishers import NormalizedBuildPublisher
import requests

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap
from translatorqueues import publish_message


DEBUG = False


class LogHandler(object):

    def __init__(self, publisher_cfg):
        self.publisher_cfg = publisher_cfg

    def _get_url_info(self, url):
        """Return a (code, content_length) tuple from making an
           HTTP HEAD request for the given url.
        """

        try:
            res = requests.head(url)
            code = res.status

            if code == 200:
                content_length = wrap(res.headers)["content-length"]
            else:
                content_length = -1

            return code, content_length
        except Exception, e:
            Log.error("Problem verifying {{url}}", e)


    def _process_data(self, data, publish_method):
        """
        Publish the message when the data is ready.

        ``publish_method`` The method to publish the type of message that
            this data is for.  Usually ``publish_unittest_message`` or
            ``publish_build_message``.
        """

        if not data.get('logurl'):
            Log.note("no logurl for {{key|quote}} ", key=data.key)
            return

        retrying = False

        while True:
            now = calendar.timegm(time.gmtime())

            code, content_length = self._get_url_info(str(data['logurl']))
            if DEBUG:
                if retrying:
                    print '...reprocessing logfile', code, data.get('logurl')
                    print '...', data.get('key')
                    print '...', now - data.get('insertion_time', 0), 'seconds since insertion_time'
                else:
                    print 'processing logfile', code, data.get('logurl')
            if code == 200:
                publish_method(data)
                break
            else:
                if now - data.get('insertion_time', 0) > 600:
                    Log.error("TIMEOUT, can not read url {{key|quote}}\n{{url|indent}}", key=data.key, url=data.logurl)
                else:
                    retrying = True
                    if DEBUG:
                        Log.note('sleeping 15 seconds before retrying')
                    time.sleep(15)

    def _publish_unittest_message(self, data):
        # The original routing key has the format build.foo.bar.finished;
        # we only use 'foo' in the new routing key.
        original_key = data['key'].split('.')[1]
        tree = data['tree']
        pltfrm = data['platform']
        buildtype = data['buildtype']
        os = data['os']
        test = data['test']
        product = data['product'] if data['product'] else 'unknown'
        key_parts = ['talos' if data['talos'] else 'unittest',
                     tree,
                     pltfrm,
                     os,
                     buildtype,
                     test,
                     product,
                     original_key]

        publish_message(
            NormalizedBuildPublisher,
            data,
            '.'.join(key_parts),
            self.publisher_cfg
        )

    def _publish_build_message(self, data):
        # The original routing key has the format build.foo.bar.finished;
        # we only use 'foo' in the new routing key.
        original_key = data['key'].split('.')[1]
        tree = data['tree']
        pltfrm = data['platform']
        buildtype = data['buildtype']
        key_parts = ['build', tree, pltfrm, buildtype]
        for tag in data['tags']:
            if tag:
                key_parts.append(tag)
        key_parts.append(original_key)

        publish_message(
            NormalizedBuildPublisher,
            data,
            '.'.join(key_parts),
            self.publisher_cfg
        )

    def handle_message(self, data):
        try:
            # publish the right kind of message based on the data.
            # if it's not a unittest, presume it's a build.
            if data.get("test"):
                publish_method = self._publish_unittest_message
            else:
                publish_method = self._publish_build_message
            self._process_data(data, publish_method=publish_method)
        except Exception, e:
            obj_to_log = data
            if data.payload.build.properties:
                obj_to_log = data.payload.build.properties
            Log.error("Problem with {{data}}", data=obj_to_log, cause=e)
