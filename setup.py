"""Setup.py script for packaging project."""

from setuptools import setup, find_packages

import json
import os


def read_pipenv_dependencies(fname):
    """Get default dependencies from Pipfile.lock."""
    filepath = os.path.join(os.path.dirname(__file__), fname)
    with open(filepath) as lockfile:
        lockjson = json.load(lockfile)
        return [dependency for dependency in lockjson.get('default')]


if __name__ == '__main__':
    setup(
        name='metadata-driven',
        author='Stefan Schenk',
        author_email='stefan_schenk@hotmail.com',
        version=os.getenv('PACKAGE_VERSION', '0.0.dev0'),
        package_dir={'': 'src'},
        packages=find_packages('src', include=[
            'metadata_driven*'
        ]),
        description='A metadata driven data processing package.',
        install_requires=[
            *read_pipenv_dependencies('Pipfile.lock'),
        ]
    )
