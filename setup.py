from distutils.core import setup

setup(
    name='data-collector',
    version='0.1',
    packages=['bitex.bitex', 'bitex.bitex.api', 'bitex.bitex.api.WSS', 'bitex.bitex.api.REST', 'bitex.bitex.formatters',
              'bitex.bitex.interfaces', 'bitex.build.lib.bitex', 'bitex.build.lib.bitex.api',
              'bitex.build.lib.bitex.api.WSS', 'bitex.build.lib.bitex.api.REST', 'bitex.build.lib.bitex.formatters',
              'bitex.build.lib.bitex.interfaces'],
    url='https://github/com/jamesprinc3/data-collector',
    license='MIT',
    author='James Prince',
    author_email='jamesprinc3@me.com',
    description='Collects data from a number of cryptocurrency exchanges'
)
