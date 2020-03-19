from setuptools import setup, find_packages

setup(
    name='gobconfig',
    version='0.1',
    description='GOB Config',
    url='https://github.com/Amsterdam/GOB-Config',
    author='Datapunt',
    author_email='',
    license='MPL-2.0',
    install_requires=[],
    packages=find_packages(exclude=['tests*']),
    dependency_links=[],
)
