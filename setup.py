# coding=utf-8
from setuptools import setup
setup(
    name="chipster-job-manager",
    version='3.1.6',
    description='Manages long running jobs on Chipster platform',
    license='MIT',
    author='Harri Hämäläinen',
    author_email='harri.hamalainen@csc.fi',
    packages=['jobmanager'],
    install_requires=[
        'flask',
        'stompest',
        'PyYAML',
        'stompest.async',
        'Twisted',
        'sqlalchemy',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ])
