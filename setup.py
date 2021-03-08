from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    'google-cloud-bigquery == 1.27.2',
    'pybigquery == 0.4.15',
    'SQLAlchemy == 1.3.19',
    'pandas == 0.24.2',
    'shapely == 1.7.1',
    'enum34 == 1.1.10;python_version < "3.4"',
]

setup(
    name='bigorm',
    packages=find_packages(exclude=['tests', 'docs']),
    description='BigQuery ORM using sqlalchemy',
    version='0.0.5',
    url='',
    author='Anthony Perez',
    author_email='anthonyp@alumni.stanford.edu',
    keywords=['Bigquery', 'sqlalchemy', 'ORM', 'Big data'],
    install_requires=REQUIRED_PACKAGES,
)
