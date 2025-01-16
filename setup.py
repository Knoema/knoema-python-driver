from setuptools import setup

def readme():
    with open('README.rst', encoding="utf8") as readme_file:
        return readme_file.read()

setup(
  name = 'knoema',
  packages = ['knoema'],
  version = '1.3.1',
  description = "Official Python package for Knoema's API",
  long_description=readme(),
  author = 'Knoema',
  author_email = 'info@knoema.com',
  license='MIT',
  url = 'https://github.com/Knoema/knoema-python-driver',
  keywords = ['API', 'knoema'],
  classifiers = ['Development Status :: 5 - Production/Stable', 'Programming Language :: Python :: 3 :: Only'],
  install_requires=['pandas>=2.0.0', 'pytest-shutil==1.7.0']
)