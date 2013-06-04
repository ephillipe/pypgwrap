#!/usr/bin/env python

try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import Command, setup

version = "0.1"
description = 'PostgreSQL database wrapper - provides wrapper over psycopg2 supporting a Python API for common sql ' \
              'functions'
long_description = file("README.md").read()


class GenerateReadme(Command):
    description = "Generates README file"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import psycopgwrap,textwrap
        long_description = textwrap.dedent(psycopgwrap.__doc__)
        open("README.md","w").write(long_description)

setup(name='psycopgwrap',
      version = version,
      description = description,
      long_description = long_description,
      author = 'Paul Chakravarti',
      author_email = 'paul.chakravarti@gmail.com',
      url = 'https://github.com/paulchakravarti/pgwrap',
      cmdclass = { 'readme' : GenerateReadme },
      packages = ['psycopgwrap'],
      install_requires = ['psycopg2'],
      license = 'BSD',
      classifiers = [ "Topic :: Database" ]
     )