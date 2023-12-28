# encoding: UTF-8

from distutils.core import setup

setup(
    name='kite',
    version='0.1.0',
    author='Eric Lam',
    author_email='ericlam@vitessedata.com',
    license='Apache License 2.0',
    url='https://github.com/vderic/kite-client-sdk',
    description='Kite Client for Python',
    packages=['kite', 'kite.xrg', 'kite.client'],
    py_modules=['kite'],
)
