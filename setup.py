#!/usr/bin/env python

try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import Command, setup

version = "0.1.1"
description = 'PostgreSQL database wrapper - provides wrapper over psycopg2 supporting a Python API for common sql ' \
              'functions, transaction and pooling'
long_description = file("README").read()


class GenerateReadme(Command):
    description = "Generates README file"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import pypgwrap,textwrap
        long_description = textwrap.dedent(pypgwrap.__doc__)
        open("README", "w").write(long_description)

setup(name='pypgwrap',
      version = version,
      description = description,
      long_description = long_description,
      platforms=["any"],
      author = 'Erick Almeida',
      author_email = 'ephillipe@gmail.com',
      maintainer="Erick Almeida",
      maintainer_email="ephillipe@gmail.com",
      url = 'https://github.com/ephillipe/pypgwrap',
      download_url = 'https://pypi.python.org/packages/source/p/pypgwrap/pypgwrap-0.1.zip#md5=2a9eaf73706d1242a84d63f8dfd1ed46',
      cmdclass = { 'readme' : GenerateReadme },
      packages = ['pypgwrap'],
      install_requires = ['psycopg2'],
      license = 'BSD',
      classifiers = [ "Topic :: Database" ]
     )