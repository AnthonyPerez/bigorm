from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    'google-cloud-bigquery == 1.9.0',
    'pybigquery == 0.4.9',
    'SQLAlchemy == 1.2.17',
    'pandas == 0.24.1',
    'sqlalchemy-utils == 0.33.11',
    'shapely == 1.6.4.post2',
]

setup(
    name='bigorm',
    packages=find_packages(exclude=['tests', 'docs']),
    description='BigQuery ORM using sqlalchemy',
    version='0.0.2',
    url='',
    author='Anthony Perez',
    author_email='anthonyp@alumni.stanford.edu',
    keywords=['Bigquery', 'sqlalchemy', 'ORM', 'Big data'],
    install_requires=REQUIRED_PACKAGES,
)
