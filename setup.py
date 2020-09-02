#!/usr/bin/env python

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()


setup(
    name='pyarrowfs-adlgen2',
    version='0.1.0',
    description='Use arrow with Azure Data Lake gen2',
    url='https://github.com/kaaveland/pyarrowfs-adlgen2',
    author='Robin Kåveland',
    author_email='kaaveland@gmail.com',
    license='MIT',
    long_description=readme,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=find_packages(include=['pyarrowfs_adlgen2'], exclude=['test']),
    python_requires='>=3.6',
    install_requires=[
        'pyarrow>=1.0.1',
        'azure-identity',
        'azure-storage-file-datalake'
    ],
    extras_require={
        'dev': ['pandas', 'pytest']
    },
    keywords='azure filesystem arrow parquet'
)