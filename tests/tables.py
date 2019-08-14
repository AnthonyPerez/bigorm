"""
python -m tests.tables
"""
import datetime

from sqlalchemy import Integer, String
import sqlalchemy
import pandas as pd

from bigorm.database import BigQueryDatabaseContext as DatabaseContext
from bigorm.abstract_models import BigQueryModel, BigQueryColumn as Column
from bigorm.bigquery_types import GeographyWKT, GeographyGeoJson

from tests import UNIT_TEST_PROJECT


class TestModelCluster1(BigQueryModel):

    __tablename__ = 'unittest.table_test_cluster1'

    __table_args__ = {
        'info': {
            'bigquery_cluster_by': ['string', 'boolean']
        }
    }

    intr = Column(Integer)
    double = Column(sqlalchemy.Float)
    boolean = Column(sqlalchemy.Boolean, nullable=False)
    string = Column(String(10))  # Give maximum length
    created_date = Column(sqlalchemy.DateTime, nullable=False, default=datetime.datetime.utcnow)
    wkt = Column(GeographyWKT, nullable=True)
    geojson = Column(GeographyGeoJson, nullable=True)


class TestModelCluster2(BigQueryModel):

    __tablename__ = 'unittest.table_test_cluster2'

    __table_args__ = {
        'info': {
            'bigquery_cluster_by': ['string']
        }
    }

    intr = Column(Integer)
    double = Column(sqlalchemy.Float)
    boolean = Column(sqlalchemy.Boolean, nullable=False)
    string = Column(String(10))  # Give maximum length
    created_date = Column(sqlalchemy.DateTime, nullable=False, default=datetime.datetime.utcnow)
    wkt = Column(GeographyWKT, nullable=True)
    geojson = Column(GeographyGeoJson, nullable=True)


def test_cluster():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        TestModelCluster1.table_create()
        TestModelCluster2.table_create()

        def _test(klass):
            klass.create([
                klass(
                    intr=4, double=1./3., boolean=True, string='mystr',
                    wkt='POLYGON((0 0,1 0,1 1,0 1,0 0))',
                    geojson={"type": "Point","coordinates": [5, 7]},
                ),
                klass(
                    intr=5, double=1./3., boolean=True, string='mystr2',
                    wkt='POLYGON((0 0,1 0,1 1,0 1,0 0))',
                    geojson={"type": "Point","coordinates": [5, 7]},
                ),
                klass(
                    intr=4, boolean=False,
                ),
                klass(
                    intr=3, boolean=False,
                ),
            ])

            count = 0
            for example_model in klass.query():
                print(example_model)
                count += 1

            assert count == 4

            assert sorted(klass.query_empty(klass.intr).all_as_list()) == [(3,), (4,), (4,), (5,)]

            dataframe = klass.query().all_as_pandas()
            print(dataframe)
            df_columns = list(dataframe.columns.values)
            print(df_columns)

            for column_name in ['intr', 'double', 'boolean', 'string', 'created_date', 'wkt', 'geojson']:
                assert column_name in df_columns

            df = pd.DataFrame({
                'intr': [-1, -2, -3],
                'boolean': [True, False, False],
                'geo': [{"type": "Point","coordinates": [1, 1]}, None, None],
            })
            klass.create_from_pandas(df, relabel={
                'geo': 'geojson'
            })

            all_results = klass.query().all_as_list()
            print(all_results)
            assert len(all_results) == 7

        _test(TestModelCluster1)
        _test(TestModelCluster2)

        def _test_load_job(klass):
            instances = [
                klass(intr=i, string='load_str1', boolean=False)
                for i in range(200)
            ]
            instances += [
                klass(intr=i, string='load_str1', boolean=True)
                for i in range(200)
            ]
            instances += [
                klass(intr=i, string='load_str2', boolean=False)
                for i in range(200)
            ]
            instances += [
                klass(intr=i, string='load_str2', boolean=True)
                for i in range(200)
            ]
            klass.create_load_job(instances)

            query_result = klass.query_empty(klass.boolean).filter(
                klass.string.in_(['load_str1', 'load_str2'])
            ).all_as_list()
            assert len(query_result) == 800

        _test_load_job(TestModelCluster1)
        _test_load_job(TestModelCluster2)

        TestModelCluster1.table_delete()
        TestModelCluster2.table_delete()


class TestModelCluster3(BigQueryModel):

    __tablename__ = 'unittest.table_test_cluster3'

    __table_args__ = {
        'info': {
            'bigquery_partition_by': ['partition_column'],
            'bigquery_cluster_by': ['string', 'boolean'],
        }
    }

    intr = Column(Integer)
    boolean = Column(sqlalchemy.Boolean, nullable=False)
    string = Column(String(10))  # Give maximum length
    partition_column = Column(sqlalchemy.TIMESTAMP)


def test_partition():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        TestModelCluster3.table_create()

        now = datetime.datetime.utcnow()
        one_day_old = now - datetime.timedelta(days=1)
        two_days_old = now - datetime.timedelta(days=2)
        three_days_old = now - datetime.timedelta(days=3)

        instances = [
            TestModelCluster3(intr=i, string='load_str1', boolean=False, partition_column=now)
            for i in range(200)
        ]
        instances += [
            TestModelCluster3(intr=i, string='load_str1', boolean=True, partition_column=one_day_old)
            for i in range(200)
        ]
        instances += [
            TestModelCluster3(intr=i, string='load_str2', boolean=False, partition_column=two_days_old)
            for i in range(200)
        ]
        instances += [
            TestModelCluster3(intr=i, string='load_str2', boolean=True, partition_column=three_days_old)
            for i in range(200)
        ]

        TestModelCluster3.create_load_job(instances)

        query_result = TestModelCluster3.query_empty(
            TestModelCluster3.intr
        ).filter_by(
            string='load_str1', boolean=False
        ).all_as_list()
        assert len(query_result) == 200

        TestModelCluster3.table_delete()


if __name__ == '__main__':
    pass
