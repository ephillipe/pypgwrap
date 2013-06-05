#!/usr/bin/env python

try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import Command, setup

version = "0.1"
description = 'PostgreSQL database wrapper - provides wrapper over psycopg2 supporting a Python API for common sql ' \
              'functions, transaction and pooling'
long_description = file("README.md").read()


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
        open("README.md","w").write(long_description)

setup(name='pypgwrap',
      version = version,
      description = description,
      long_description = long_description,
      author = 'Erick Almeida',
      author_email = 'ephillipe@gmail.com',
      url = 'https://github.com/ephillipe/pypgwrap',
      cmdclass = { 'readme' : GenerateReadme },
      packages = ['pypgwrap'],
      install_requires = ['psycopg2'],
      license = 'BSD',
      classifiers = [ "Topic :: Database" ]
     )