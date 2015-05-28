# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals
from __future__ import absolute_import
from collections import Mapping

import datetime
import time

from mozillapulse.messages.base import GenericMessage
from pyLibrary.env import pulse
from pyLibrary.parsers import Log


publisher = None


def publish_message(_, data, routing_key, pulse_cfg):
    global publisher
    assert(isinstance(data, Mapping))

    msg = GenericMessage()
    msg.routing_parts = routing_key.split('.')
    for key, value in data.iteritems():
        msg.set_data(key, value)

    failures = []
    while True:
        try:
            if not publisher:
                publisher = pulse.Publisher(settings=pulse_cfg)

            publisher.send(routing_key, msg)
            break
        except Exception, e:
            now = datetime.datetime.now()
            Log.exception('Failure when publishing {{key|quote}}', key=routing_key, cause=e)

            failures = [x for x in failures
                        if now - x < datetime.timedelta(seconds=60)]
            failures.append(now)

            if len(failures) >= 5:
                Log.warning('{{num}} publish failures within one minute.', num=len(failures), cause=e)
                failures = []
                sleep_time = 5 * 60
            else:
                sleep_time = 5

            Log.warning('Sleeping for {{num}} seconds.', num=sleep_time)
            time.sleep(sleep_time)
            Log.warning('Retrying...')
