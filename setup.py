#!/usr/bin/env python

try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import Command, setup

version = "0.1.4"
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
        import pypgwrap
        import textwrap

        long_description = textwrap.dedent(pypgwrap.__doc__)
        open("README", "w").write(long_description)


setup(name='pypgwrap',
      version=version,
      description=description,
      long_description=long_description,
      platforms=["any"],
      author='Erick Almeida',
      author_email='ephillipe@gmail.com',
      maintainer="Erick Almeida",
      maintainer_email="ephillipe@gmail.com",
      url='https://github.com/ephillipe/pypgwrap',
      download_url='https://pypi.python.org/packages/source/p/pypgwrap/pypgwrap-0.1.4.zip#md5=0cd8c0af50e4f8b9fd64bac68b00e2d6',
      cmdclass={'readme': GenerateReadme},
      packages=['pypgwrap'],
      install_requires=['psycopg2'],
      license='BSD',
      classifiers=["Topic :: Database"]
)