# encoding: utf-8
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import unicode_literals

import os

from daemon import createDaemon
from pulsetranslator import PulseBuildbotTranslator
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log


def main():
    try:
        options = startup.read_settings()
        Log.start(options.debug)
        constants.set(options.constants)

        if options.daemon:
            if os.access(options.logfile, os.F_OK):
                os.remove(options.logfile)
            createDaemon(options.pidfile, options.logfile)

            f = open(options.pidfile, 'w')
            f.write("%d\n" % os.getpid())
            f.close()

        service = PulseBuildbotTranslator(settings=options)
        service.start()
    except Exception, e:
        Log.error("Problem with service", e)
    finally:
        Log.stop()



if __name__ == "__main__":
    main()
