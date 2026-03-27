from setuptools import find_packages, setup

setup(
    name='dagster_sync',
    packages=find_packages(),
    install_requires=['dagster', 'dagster-postgres'],
)
