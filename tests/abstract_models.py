"""
This is not exactly a unit test, it lacks both a unittesting framework and is not mocking the BigQuery connection.

python -m tests.abstract_models
"""
import datetime
import json

from sqlalchemy import Integer, String
import sqlalchemy
import pandas as pd

from bigorm.database import BigQueryDatabaseContext as DatabaseContext
from bigorm.abstract_models import BigQueryModel, BigQueryColumn as Column
from bigorm.bigquery_types import GeographyWKT, GeographyGeoJson

from tests import UNIT_TEST_PROJECT


class TestGeoModel(BigQueryModel):

    __tablename__ = 'unittest.test_geo'

    id = Column(Integer)
    geometry1 = Column(GeographyWKT, nullable=True)
    geometry2 = Column(GeographyGeoJson, nullable=True)

    def __repr__(self):
        return 'TestModel(id={}, geo1={}, geo2={})'.format(
            self.id, self.geometry1, self.geometry2
        )

    def __eq__(self, other):
        return (
            self.id == other.id
        )


def test_geo():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        TestGeoModel.table_create()

        TestGeoModel.create([
            TestGeoModel(
                id=1, geometry1='POLYGON((0 0,1 0,1 1,0 1,0 0))',
                geometry2={"type": "Point","coordinates": [5, 7]},
            ),
            TestGeoModel(
                id=2, geometry1='POLYGON((1 1,2 1,2 2,1 2,1 1))',
                geometry2={"type": "Point","coordinates": [5, 7]},
            ),
        ])

        id1 = TestGeoModel.query().filter(
            sqlalchemy.func.ST_Contains(
                TestGeoModel.geometry1, sqlalchemy.func.ST_GeogFromText('POINT(0.5 0.5)')
            )
        ).one()
        print(id1)
        assert id1.id == 1

        print(list(TestGeoModel.query().all()))

        assert id1.geometry2['type'] == 'Point'
        assert id1.geometry2['coordinates'] == [5, 7]

        TestGeoModel.table_delete()


class TestModel(BigQueryModel):

    __tablename__ = 'unittest.test'

    id = Column(Integer)
    column1 = Column(Integer, nullable=False, default=1)
    column2 = Column(Integer, nullable=False, default=2)

    def __repr__(self):
        return 'TestModel(id={}, column1={}, column2={})'.format(
            self.id, self.column1, self.column2
        )

    def __eq__(self, other):
        return (
            self.id == other.id
            and self.column1 == other.column1
            and self.column2 == other.column2
        )


def test1():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        # Leaving this comment for illustrative purposes, but it's not what we want to do for this test.
        # Base.metadata.create_all(DatabaseContext.get_engine())
        TestModel.table_create()

        TestModel.create_load_job([
            TestModel(id=1),
            TestModel(id=2),
            TestModel(id=2),
            TestModel(id=2, column1=2),
            TestModel(id=3),
            TestModel(id=4),
            TestModel(id=4, column1=2, column2=5),
        ])

        id1 = TestModel.query().filter_by(id=1).one()  # Get one or raise an error
        column1_is_1 = list(TestModel.query().filter_by(column1=1).all())  # Get all as iterable

        print(id1)
        print(column1_is_1)

        assert id1 == TestModel(id=1, column1=1, column2=2)
        assert len(column1_is_1) == 5

        update_count = (
            TestModel.query()
            .filter(TestModel.id >= 2)
            .filter(TestModel.id <= 3)
            .update({
                'column1': TestModel.column1 + 3
            })
        )
        print(update_count)
        assert update_count == 4

        column1_is_4 = list(TestModel.query().filter_by(column1=4).all())
        print(column1_is_4)
        assert len(column1_is_4) == 3

        delete_count = (
            TestModel.query()
            .filter(TestModel.column1 >= 3)
            .delete()
        )

        print(delete_count)
        assert delete_count == 4

        TestModel.table_delete()


class TestModel2(BigQueryModel):

    __tablename__ = 'unittest.test2'

    id = Column(Integer)
    c1 = Column(Integer)
    c2 = Column(Integer, nullable=False, default=2)


def test2():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        TestModel2.table_create()

        TestModel2.create([
            TestModel2(id=1),
            TestModel2(id=2, c2=4),
            TestModel2(id=3, c1=None, c2=3),
            TestModel2(id=4, c1=1),
        ])

        TestModel2.create([
            TestModel2(id=None),
            TestModel2(id=None, c2=5),
            TestModel2(id=None, c1=None, c2=6),
            TestModel2(id=None, c1=1),
        ])

        TestModel2.table_delete()


class TestModel3(BigQueryModel):

    __tablename__ = 'test3'

    id = Column(Integer)
    geo = Column(GeographyWKT, nullable=True)
    geo2 = Column(GeographyGeoJson, nullable=True)
    c1 = Column(String(10), nullable=True)
    default_int = Column(Integer, nullable=False, default=1)

    def __repr__(self):
        return 'TestModel3(id={})'.format(self.id)


def test3():
    with DatabaseContext(project=UNIT_TEST_PROJECT, default_dataset='unittest'):
        TestModel3.table_create()

        TestModel3.create([
            TestModel3(id=i, geo='POLYGON((0 0,1 0,1 1,0 0))',) for i in range(2000)
        ])

        TestModel3.create([
            TestModel3(id=i, geo='POLYGON((0 0,1 0,1 1,0 0))',) for i in range(2001, 2001+100)
        ], batch_size=20)

        query_results = list(TestModel3.query().all())

        assert len(query_results) == 2100

        TestModel3.table_delete()


class Test4Model1(BigQueryModel):

    __tablename__ = 'unittest.test4_1'

    id = Column(Integer)
    c1 = Column(Integer, nullable=True)
    c2 = Column(Integer, nullable=True)

    def __repr__(self):
        return 'Test4Model1(id={})'.format(
            self.id
        )


class Test4Model2(BigQueryModel):

    __tablename__ = 'unittest.test4_2'

    id = Column(Integer)
    c2 = Column(Integer, nullable=True)
    c3 = Column(Integer, nullable=True)

    def __repr__(self):
        return 'Test4Model2(id={})'.format(
            self.id
        )


def test4():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        Test4Model1.table_create()
        Test4Model2.table_create()

        Test4Model1.create([
            Test4Model1(id=i, c1=i+1, c2=i*2) for i in range (5)
        ] + [
            Test4Model1(id=-i, c1=i+1, c2=i*2) for i in range (1, 3)
        ])
        Test4Model2.create([
            Test4Model2(id=i, c2=i+1, c3=-i) for i in range (5)
        ] + [
            Test4Model2(id=i + 100, c2=i+1, c3=-i) for i in range (2)
        ])

        results = Test4Model1.query(
            Test4Model2.id.label('2nd_id'), Test4Model2.c2.label('2nd_c2'),
            Test4Model2.c3.label('2nd_c3'),
        ).join(
            Test4Model2, Test4Model1.id == Test4Model2.id, full=True  # full outer join
        ).all()
        results = list(results)
        print(results)
        assert len(results) == 9

        Test4Model1.table_delete()
        Test4Model2.table_delete()


class TestModel5(BigQueryModel):

    __tablename__ = 'unittest.test5'
    
    intr = Column(Integer)
    double = Column(sqlalchemy.Float)
    boolean = Column(sqlalchemy.Boolean, nullable=False)
    string = Column(String(10))  # Give maximum length
    created_date = Column(sqlalchemy.DateTime, nullable=False, default=datetime.datetime.utcnow)
    wkt = Column(GeographyWKT, nullable=True)
    geojson = Column(GeographyGeoJson, nullable=True)


def test5():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        TestModel5.table_create()

        TestModel5.create([
            TestModel5(
                intr=4, double=1./3., boolean=True, string='mystr',
                wkt='POLYGON((0 0,1 0,1 1,0 1,0 0))',
                geojson={"type": "Point","coordinates": [5, 7]},
            ),
            TestModel5(
                intr=5, double=1./3., boolean=True, string='mystr2',
                wkt='POLYGON((0 0,1 0,1 1,0 1,0 0))',
                geojson={"type": "Point","coordinates": [5, 7]},
            ),
            TestModel5(
                intr=4, boolean=False,
            ),
            TestModel5(
                intr=3, boolean=False,
            ),
        ])

        count = 0
        for example_model in TestModel5.query():
            print(example_model)
            count += 1

        assert count == 4

        assert sorted(TestModel5.query_empty(TestModel5.intr).all_as_list()) == [(3,), (4,), (4,), (5,)]

        dataframe = TestModel5.query().all_as_pandas()
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
        TestModel5.create_from_pandas(df, relabel={
            'geo': 'geojson'
        })

        all_results = TestModel5.query().all_as_list()
        print(all_results)
        assert len(all_results) == 7

        TestModel5.table_delete()


class TestModel6(BigQueryModel):

    __tablename__ = 'unittest.test6'
    
    intr = Column('intr_label', Integer)
    intr_def_lab = Column('intr_def_label', Integer, nullable=False, default=5)
    double = Column(sqlalchemy.Float)
    boolean = Column(sqlalchemy.Boolean, nullable=False)
    string = Column(String(10))
    created_date = Column(sqlalchemy.DateTime, nullable=False, default=datetime.datetime.utcnow)
    wkt = Column(GeographyWKT, nullable=True)
    geojson = Column(GeographyGeoJson, nullable=True)

def test6():
    with DatabaseContext(project=UNIT_TEST_PROJECT):

        hole_comes_second = TestModel6(boolean=True, geojson={
            "type": "Polygon",
            "coordinates": [
                [
                    [
                    -120,
                    60
                    ],
                    [
                    120,
                    60
                    ],
                    [
                    120,
                    -60
                    ],
                    [
                    -120,
                    -60
                    ],
                    [
                    -120,
                    60
                    ]
                ],
                [
                    [
                    -60,
                    30
                    ],
                    [
                    60,
                    30
                    ],
                    [
                    60,
                    -30
                    ],
                    [
                    -60,
                    -30
                    ],
                    [
                    -60,
                    30
                    ]
                ],
            ]
        })

        hole_comes_first = TestModel6(boolean=True, geojson={
            "type": "Polygon",
            "coordinates": [
                [
                    [
                    -60,
                    30
                    ],
                    [
                    60,
                    30
                    ],
                    [
                    60,
                    -30
                    ],
                    [
                    -60,
                    -30
                    ],
                    [
                    -60,
                    30
                    ]
                ],
                [
                    [
                    -120,
                    60
                    ],
                    [
                    120,
                    60
                    ],
                    [
                    120,
                    -60
                    ],
                    [
                    -120,
                    -60
                    ],
                    [
                    -120,
                    60
                    ]
                ],
            ]
        })

        instances = [
            TestModel6(
                intr=4, double=1./3., boolean=True, string='str',
                wkt='POLYGON((0 0,1 0,1 1,0 1,0 0))',
                geojson={"type": "Point","coordinates": [5, 7]},
            ),
            TestModel6(
                intr=4, boolean=False,
            ),
            TestModel6(
                intr=3, boolean=False,
            ),
        ]

        print(instances)
        assert instances[0].geojson['coordinates'] == [5, 7]

        json_repr = {
            'intr_label': 4,
            'intr_def_label': 5,
            'double': 1./3.,
            'boolean': True,
            'string': 'str',
            'created_date': None,
            'wkt': str({
                "type": "Polygon","coordinates": [[
                    [0., 0.],
                    [1., 0.],
                    [1., 1.],
                    [0., 1.],
                    [0., 0.],
                ]],
            }).replace("'", '"'),
            'geojson': str({"type": "Point","coordinates": [5., 7.]}).replace("'", '"'),
        }

        as_json = instances[0].serialize_as_dict()
        print(as_json)
        as_json['created_date'] = None
        assert as_json == json_repr

        json_repr = {
            'intr_label': 4,
            'intr_def_label': 5,
            'double': None,
            'boolean': False,
            'string': None,
            'created_date': None,
            'wkt': None,
            'geojson': None,
        }

        as_json = instances[1].serialize_as_dict()
        print(as_json)
        as_json['created_date'] = None
        assert as_json == json_repr

        TestModel6.table_create()

        TestModel6.create_load_job(instances)

        query_result = TestModel6.query().all_as_list()
        print(query_result)
        assert len(query_result) == 3

        TestModel6.table_delete()


def test6_2():
    with DatabaseContext(project=UNIT_TEST_PROJECT):

        exterior = [
            [
            -120,
            60
            ],
            [
            120,
            60
            ],
            [
            120,
            -60
            ],
            [
            -120,
            -60
            ],
            [
            -120,
            60
            ]
        ]

        interior = [
            [
            -60,
            30
            ],
            [
            60,
            30
            ],
            [
            60,
            -30
            ],
            [
            -60,
            -30
            ],
            [
            -60,
            30
            ]
        ]

        hole_comes_second_list = [
            TestModel6(boolean=True, geojson={
                "type": "Polygon",
                "coordinates": [list(exterior), list(interior),]
            }),
            TestModel6(boolean=True, geojson={
                "type": "Polygon",
                "coordinates": [list(reversed(exterior)), list(interior),]
            }),
            TestModel6(boolean=True, geojson={
                "type": "Polygon",
                "coordinates": [list(exterior), list(reversed(interior)),]
            }),
            TestModel6(boolean=True, geojson={
                "type": "Polygon",
                "coordinates": [list(reversed(exterior)), list(reversed(interior)),]
            }),
        ]

        hole_comes_first = TestModel6(boolean=True, geojson={
            "type": "Polygon",
            "coordinates": [interior, exterior,]
        })

        TestModel6.table_create()

        hole_2nd_dicts = [m.serialize_as_dict() for m in hole_comes_second_list]
        for serialized_form in hole_2nd_dicts:
            # created_date will be different for each object
            serialized_form.pop('created_date', None)
        for serialized_form in hole_2nd_dicts:
            assert serialized_form == hole_2nd_dicts[0]
            print(serialized_form)

        bad_input_fails = False
        try:
            hole_comes_first.serialize_as_dict()
        except ValueError:
            print('hole first fails')
            bad_input_fails = True

        if not bad_input_fails:
            raise RuntimeError('Polygon with hole first successfully serialized when it shouldn\'t be.')
        
        TestModel6.create_load_job(hole_comes_second_list)

        query_result = TestModel6.query().all_as_list()
        print(query_result)
        assert len(query_result) == 4

        TestModel6.table_delete()

"""
Test 7:
Test if tables from different datasets can be joined.
"""

class TestModel7_1(BigQueryModel):

    __tablename__ = 'unittest.test7'
    
    id = Column(Integer)

    def __eq__(self, other):
        # typically __eq__ will also check that the classes match
        return other.id == self.id

class TestModel7_2(BigQueryModel):

    __tablename__ = 'unittest2.test7'
    
    id = Column(Integer)

    def __eq__(self, other):
        # typically __eq__ will also check that the classes match
        return other.id == self.id

def test7():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        TestModel7_1.table_create()
        TestModel7_2.table_create()

        TestModel7_1.create([
            TestModel7_1(id=1),
            TestModel7_1(id=2),
            TestModel7_1(id=3),
        ])
        TestModel7_1.create_load_job([
            TestModel7_1(id=4),
            TestModel7_1(id=5),
            TestModel7_1(id=6),
        ])

        TestModel7_2.create([
            TestModel7_2(id=1),
            TestModel7_2(id=2),
            TestModel7_2(id=3),
        ])
        TestModel7_2.create_load_job([
            TestModel7_2(id=4),
            TestModel7_2(id=5),
            TestModel7_2(id=6),
        ])

        m71s = TestModel7_1.query().all_as_list()
        m72s = TestModel7_2.query().all_as_list()

        m71s = [m for _, m in sorted((m.id, m) for m in m71s)]
        m72s = [m for _, m in sorted((m.id, m) for m in m72s)]

        print(m71s)
        print(m72s)
        assert m71s == m72s

        joined_result = TestModel7_1.query(
            TestModel7_2.id.label('id2')
        ).join(
            TestModel7_2, TestModel7_2.id == TestModel7_1.id
        ).all_as_list()

        print(joined_result)
        assert len(joined_result) == 6

        TestModel7_1.table_delete()
        TestModel7_2.table_delete()


def test_table_methods():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        for klass in [TestModel7_1, TestModel7_2]:
            assert not klass.table_exists()
            klass.table_create()
            assert klass.table_exists()
            klass.table_delete()
            assert not klass.table_exists()


"""
Test 8:
There was a bug where create_from_pandas was failing if you specify
the dataset name in the table and the DatabaseContext constructor.
"""


class TestModel8(BigQueryModel):

    __tablename__ = 'unittest.test8'
    
    intr = Column(Integer)
    created_date = Column(sqlalchemy.DateTime, nullable=False, default=datetime.datetime.utcnow)


def test8():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        TestModel8.table_create()

        df = pd.DataFrame({
            'intr': [1, 2, 3],
            'created_date': [datetime.datetime.utcnow()] * 3
        })
        TestModel8.create_from_pandas(df)

        table_results = TestModel8.query().all_as_list()
        print(table_results)
        assert len(table_results) == 3

        TestModel8.table_delete()



"""
Test 9:
test parse functions and create_from_geojson
"""

class TestModel9(BigQueryModel):

    __tablename__ = 'unittest.test9'

    intr = Column(Integer)
    double = Column(sqlalchemy.Float)
    created_date = Column(sqlalchemy.DateTime, nullable=False, default=datetime.datetime.utcnow)
    geojson = Column(GeographyGeoJson, nullable=True)

    def __eq__(self, other):
        return (
            isinstance(other, TestModel9) and
            self.intr == other.intr and
            self.double == other.double and
            self.created_date == other.created_date and
            self.geojson == other.geojson
        )

def test9():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        TestModel9.table_create()

        date = datetime.datetime.utcnow()
        point = {'type': 'Point', 'coordinates': [5, 7]}

        df = pd.DataFrame({
            'intr': [1, 2, 3],
            'created_date': [date] * 3,
            'geojson': [point] * 3,
        })
        instances = TestModel9.parse_from_pandas(df)

        assert instances == [
            TestModel9(intr=1, created_date=date, geojson=point),
            TestModel9(intr=2, created_date=date, geojson=point),
            TestModel9(intr=3, created_date=date, geojson=point),
        ]

        geojson = {
            'features': [
                {
                    'properties': {
                        'intr': 1,
                    },
                    'geometry': point,
                },
                {
                    'properties': {
                        'intr': 2,
                    },
                    'geometry': point,
                },
                {
                    'properties': {
                        'intr': 3,
                    },
                    'geometry': point,
                },
            ]
        }

        assert instances == TestModel9.parse_from_geojson(
            geojson,
            geometry_property_name='geojson',
            defaults={
                'created_date': date
            }
        )

        TestModel9.table_delete()


def test_geojson_serialize():
    """
    test:
        TestModel9.query(...).all_as_dicts()
        TestModel9.query(...).all_as_geojson()
        TestModel9.serialize_as_geojson()

        serialize_as_geojson and parse_from_geojson should be inverses.
    """
    with DatabaseContext(project=UNIT_TEST_PROJECT):

        date = datetime.datetime.utcnow()
        date_as_str = date.strftime('%Y-%m-%d %H:%M:%S.%f')
        instances = [
            TestModel9(
                intr=i,
                created_date=date_as_str,
                geojson={'type': 'Point', 'coordinates': [5.0, i]}
            )
            for i in range(3)
        ]
        expected_geojson = {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'geometry': {'type': 'Point', 'coordinates': [5.0, float(i)]},
                    'properties': {
                        'intr': i,
                        'double': None,
                        'created_date': date_as_str
                    }
                }
                for i in range(3)
            ]
        }
        expected_geojson_str = json.dumps(expected_geojson, sort_keys=True)
        instances_as_dicts = [
            {
                'intr': instance.intr,
                'created_date': instance.created_date,
                'geojson': instance.geojson,
                'double': instance.double,
            }
            for instance in instances
        ]

        # test parse_from_geojson
        parsed_from_geojson = TestModel9.parse_from_geojson(
            expected_geojson, geometry_property_name='geojson'
        )
        assert parsed_from_geojson == instances

        # test parse_from_geojson
        parsed_from_geojson = TestModel9.parse_from_geojson(
            json.loads(expected_geojson_str), geometry_property_name='geojson'
        )
        assert parsed_from_geojson == instances

        # test serialize_as_geojson
        serialized_as_geojson = TestModel9.serialize_as_geojson(
            instances, geometry_column='geojson',
            excluded_keys=None)
        assert serialized_as_geojson == expected_geojson_str

        # test parse_from_geojson and serialize_as_geojson consistency
        assert (
            TestModel9.parse_from_geojson(
                json.loads(TestModel9.serialize_as_geojson(
                    instances, geometry_column='geojson',
                    excluded_keys=None
                )),
                geometry_property_name='geojson'
            ) == instances
        )

        # Goes from unittest to intregration test below this line.

        TestModel9.table_create()
        TestModel9.create(instances)

        all_as_dicts = TestModel9.query().order_by(
            TestModel9.intr
        ).all_as_dicts()
        all_as_dicts = [
            dict(d, **{'created_date': d['created_date'].strftime('%Y-%m-%d %H:%M:%S.%f')})
            for d in all_as_dicts
        ]
        assert all_as_dicts == instances_as_dicts

        all_as_geojson = TestModel9.query().order_by(
            TestModel9.intr
        ).all_as_geojson(geometry_column='geojson')
        assert json.loads(all_as_geojson) == json.loads(expected_geojson_str)

        all_as_geojson_empty_query = TestModel9.query_empty(
            TestModel9.intr
        ).order_by(
            TestModel9.intr
        ).all_as_geojson(geometry_column=None)
        assert (
            json.loads(all_as_geojson_empty_query) == {
                'type': 'FeatureCollection',
                'features': [
                    {
                        'type': 'Feature',
                        'geometry': None,
                        'properties': {
                            'intr': i
                        }
                    }
                    for i in range(3)
                ]
            }
        )

        TestModel9.table_delete()


if __name__ == '__main__':
    pass
