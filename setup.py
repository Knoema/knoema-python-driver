from setuptools import setup

setup(
  name = 'knoema',
  packages = ['knoema'],
  version = '0.1.4.dev1',
  description = "Official Python package for Knoema's API",
  author = 'Knoema',
  author_email = 'info@knoema.com',
  url = 'https://github.com/Knoema/knoema-python-driver',
  keywords = ['API', 'knoema'],
  classifiers = ['Development Status :: 3 - Alpha', 'Programming Language :: Python :: 3 :: Only'],
  install_requires=['pandas']
)
