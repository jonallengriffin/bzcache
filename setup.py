# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
from setuptools import setup, find_packages

version = '0.1.1'

deps = ['mozautoeslib']

if sys.version < '2.6' or sys.version >= '3.0':
    print >>sys.stderr, '%s requires Python >= 2.6 and < 3.0' % _PACKAGE_NAME
    sys.exit(1)

setup(name='bzcache',
      version=version,
      description="Pulse-based bugzilla cache",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Jonathan Griffin',
      author_email='jgriffin@mozilla.com',
      url='http://hg.mozilla.org/users/jgriffin_mozilla.com/bzcache/',
      license='MPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=deps,
      )
