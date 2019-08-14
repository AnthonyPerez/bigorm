# Usage Tutorial

## Create Schema

The example below does nothing yet, but will define the schema of a table named 'example'.

```python
import datetime
import sqlalchemy
from sqlalchemy import String, Float, Integer, DateTime
from bigorm.bigquery_types import GeographyGeoJson
from bigorm.abstract_models import BigQueryModel as Model, BigQueryColumn as Column

class Example(Model):

    __tablename__ = 'example_dataset.example'  # The table will be named 'example' in the dataset 'example_dataset'
    example_version = Column(Integer)  # Creates a column in the table named 'example_version'
    example_float_property = Column(Float, nullable=False)  # Create a property that is not allowed to be empty with nullable=False

    # Create a column that automatically gets populated with the current datetime (when the model is created)
    example_date_created = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)

    # Create a string property.  Types have parameters, this parameter will determine the maximum length of the string
    # Bigquery will not enforce this parameter but other databases will.
    # doc can be used to add documentation.
    example_string_property = Column(String(100), doc='My string Property')
```

You can use mixins to have a common set of properties appear in multiple tables.

```python
class MyMixin(object):

    mymixin_column_in_multiple_tables = Column(Integer)

class ExampleWithMixin1(MyMixin, Model):
    __tablename__ = 'example_dataset.example1'
    # Will have column_in_multiple_tables from MyMixin
    example1_column = Column(Float)

class ExampleWithMixin2(MyMixin, Model):
    __tablename__ = 'example_dataset.example2'
    # Will have column_in_multiple_tables from MyMixin
    example2_column = Column(Float)
```

## Create Table

The example below creates a table with `Example`'s schema in the project named `example_project` under the dataset named `example_dataset`.

```python
from bigorm.database import BigQueryDatabaseContext as DatabaseContext

with DatabaseContext(project='example_project'):
    if not Example.table_exists():
        Example.table_create()
```

## Upload Data

You have several options.

```python
import pandas as pd

with DatabaseContext(project='example_project'):

    example1 = Example(
        example_version=5,
        example_float_property=1.3,
        example_date_created=datetime.datetime(2019, 2, 19),
        example_string_property='str'
    )

    assert example1.example_float_property == 1.3

    # the example_date_created will be automatically populated when we ask the database to
    # create example2
    example2 = Example(
        example_version=5,
        example_float_property=1.3,
        example_string_property='str'
    )

    # Only the float property is required and without a default.  So this is valid.
    example3 = Example(
        example_float_property=1.3,
    )

    # This example will cause an error if we upload it
    # because example_float_property is required but not given.
    # We can still create it.
    example4 = Example()

    assert example2.example_date_created is None

    instances = [example1, example2, example3]

    dataframe = pd.DataFrame({
        'example_float_property': [1.1, 2.2, 5.2],
        'example_date_created': [datetime.datetime.utcnow()] * 3,
        'example_version_labeled_differently': [None, 3, None],
    })

    # The create methods below can be found under datasets.abstract_models.BigQueryCRUDMixin

    Example.create(instances)  # Create with streaming API
    Example.create_load_job(instances)  # Create with load job API
    # Return instances from pandas.DataFrame
    # example_string_property will be None for all rows
    examples = Example.parse_from_pandas(
        df=dataframe,
        relabel={
            'example_version_labeled_differently': 'example_version'
        }
    )
    # Create from pandas.DataFrame with load_job
    # example_string_property will be None for all rows
    Example.create_from_pandas(
        df=dataframe,
        relabel={
            'example_version_labeled_differently': 'example_version'
        },
        create_method='load_job'
    )
    examples = Example.parse_from_geojson(...)
    Example.create_from_geojson(...)
```

See https://github.com/AtlasAIPBC/datasets/blob/anthony_qa_pipeline/datasets/abstract_models.py

## Query Data

Querying data has three steps.
See https://docs.sqlalchemy.org/en/latest/orm/query.html for more info.

Step 1: Select the columns you want.

```python
with DatabaseContext(project='example_project'):

    query1 = Example.query()  # Selects all columns
    query2 = Example.query_empty(Example.example_version, Example.example_float_property)  # Select some columns

    # Select all columns of Example and some columns of ExampleWithMixin1
    # We will need to join later.
    query3 = Example.query(ExampleWithMixin1.example1_column)
```

Step 2: Filter as necessary

```python
    query1 = query1.filter(Example.example_version > 3)
    query1 = query1.filter_by(example_float_property=1.3)

    query2 = query2.filter(
        sqlalchemy.or_(
            Example.example_version <= 5,
            Example.example_string_property == 'test',
        )
    ).filter(  # This is an And but you can also use sqlalchemy.and_
        Example.example_float_property > 0
    )
    
    # There are many join arguments for complex joins
    query3 = query3.join(
        ExampleWithMixin1, ExampleWithMixin1.example1_column == Example.example_version
    ).filter(
        ExampleWithMixin1.mymixin_column_in_multiple_tables < 5
    )
```

Step 3: Get the data

The queries created in steps 1 and 2 have not yet been executed (no data has been pulled).  The query is executed by functions listed here, i.e. all(), all_as_list(), all_as_pandas().

```python
    for row in query1:
        assert isinstance(row, Example)

    iterator = query2.all()
    for row in iterator:
        assert isinstance(row, tuple)
        example_version, example_float_property = row

    q3_result = query3.all_as_list()
    example_obj, example1_column = q3_result[1]
    assert isinstance(example_obj, Example)
    # example1_column is the type of ExampleWithMixin1.example1_column

    df = query1.all_as_pandas()
    assert isinstance(df, pd.DataFrame)

    list_of_dicts = query1.all_as_dicts()
    assert isinstance(list_of_dicts, list)
    assert isinstance(list_of_dicts[0], dict)

    geojson = query1.all_as_geojson(
        geometry_column=None, excluded_keys=None, as_str=False
    )
    assert isinstance(geojson, dict)

    query1.first()  # gets the first element and returns it
    query1.one() # raises an error if there isn't exactly one element in the return result
    query1.one_or_none()  # error if there isn't exactly 0 or 1 element in the return result
    query2.scalar()  # Calls one_or_none, if the query returns a tuple, returns the first element.
```

All together:

```python
with DatabaseContext(project='example_project'):
    examples = Example.query().filter_by(example_float_property=1.3).all_as_list()
```

## Delete Data

To update data, create a query and call .delete

```python
with DatabaseContext(project='example_project'):
    number_deleted_ = Example.query().filter_by(
        example_float_property=1.3
    ).delete()
```

## Update Data

To update data, create a query and call .update

```python
with DatabaseContext(project='example_project'):
    number_updated = Example.query().filter_by(
        example_float_property=1.3
    ).update({
        'example_float_property': Example.example_float_property + 3
    }
```

If you need to update individual rows more carefully, you must
* Download them via a query
* Delete them
* Upload them again (after updating them locally)

## Delete Table

```python
with DatabaseContext(project='example_project'):
    Example.table_delete()
```

# Considerations

* Mind the quotas and limits for creating via streaming and load job APIs  See the function docstrings for more info.
* Inserting with the streaming API puts data into a "streaming buffer" which can still be queried.  As a result, the table metadata is unreliable.
* * https://cloud.google.com/blog/products/gcp/life-of-a-bigquery-streaming-insert
* Joining may not be possible across projects.
* Bigquery is not made to frequently update individual rows.
* Note that when using BigQuery, rollbacks do not work and all results are immediately commited.
* Bigquery is not SQL, it just supports SQL queries.
* Table names can be prefixed with their dataset e.g. `__tablename__ = dataset_name.table_name`.  If they are then the `DatabaseContext`'s `default_dataset` argument will be ignored.
* Changing the table schema is possible, but is not supported directly by this API.
* Adding models directly to the session may break sqlalchemy because Bigquery allows duplicate rows.
* If an error is raised during the create operation, some records from the operation may still have been added to the table.
* * Particularly with the streaming API and if batch_size is not None.
* Additionally, changes in table metadata (including whether the table exists) are eventually consistent.  This means you'll have to wait for a few minutes after creating a table before you can insert elements.
* * This is especially bad after deleting and then recreating a table.  You may have to wait for up to 10 minutes.

# Conventions

* Properties that you intend to have shared across multiple tables and intent to mean the same thing should go into a Mixin class.
* All Models and Mixins should be uniquely prefixed.  This prefix should not contain the `_` character.  Rather the `_` character is a delimiter showing where the prefix ends.  This will make joins much simpler.
* Make an effort to look at the other models to avoid duplicating a prefix and also consider future tables.
* * Prefixes should be mutually unique across regular models and mixins.
* * After the prefix, use snake_case.
* * Numbers are allowed in the prefix.

* Table names can be prefixed with their dataset e.g. `__tablename__ = dataset_name.table_name`.  If they are then the database's dataset argument will be ignored.
* * **Always include the dataset name in the table.**

* Avoid making class property names different from column names.
* Avoid making raw sql queries if possible.
* Avoid directly using the session or engine objects if possible.
* Avoid creating instances outside of the methods of the API unless you know what you're doing.  ('creating' in that sentence means populating the database)
* `BigQueryQueryMixin.query_raw` will let you run queries directly in raw sql.  Avoid this if possible.
* Make an attempt to use Mixins to standardized columns that other services will use to search over.
* Use relations judiciously.
* Avoid composite types, rather use a python property getter and setter (@property and @property_name.setter)

# Advanced Querying

```python
with DatabaseContext(project='example_project'):
    # Filter in list
    Example.query().filter(Example.example_version.in_([1, 2, 3])).all()
    # Distinct will remove duplicates from a query.
    # The next line will return all unique versions
    Example.query_empty(Example.example_version).distinct().all()
    # The next line will return all unique pairs of version and float_property.
    Example.query_empty(Example.example_version, Example.example_float_property).distinct().all()
```

# Other API Functions

### Serialize

Note that all functions in the serialize API will
serialize data into a Bigquery ingestable form which
is not necessarily what you might expect.
In particular geographies and dates will be converted to strings.

```python
with DatabaseContext(project='example_project'):
    # serialize_as_dict returns a dict from property
    # to value, gets default values if they are set
    # and will do processing sqlalchemy would do
    # locally before sending to the server
    # e.g. Geographies are converted to strings.
    Example(...).serialize_as_dict()
    # serialize_as_json first calls serialize_as_dict
    # and then converts the dict to a json string, while
    # converting datetime objects to a Bigquery friendly format.
    Example(...).serialize_as_json()
    # get_property_names_to_columns returns a mapping
    # from the ORM class' attribute names to a list of columns
    # the value is a list because composite properties may map to multiple columns.
    Example.get_property_names_to_columns()
    # Returns instances of Example as a geojson
    Example.serialize_as_geojson(
        instances=[
            Example(...),
            Example(...),
        ],
        geometry_column=None,
        excluded_keys=None
    )
```

### Types

```python
import sqlalchemy
import enum
from bigorm.bigquery_types import GeographyGeoJson, Enum
from bigorm.abstract_models import BigQueryModel as Model, BigQueryColumn as Column

class MyEnum(enum.Enum):
    RED = 'RED'
    BLUE = 'BLUE'

class Example(Model):

    __tablename__ = 'example_dataset.example'

    example_enum = Column(Enum(MyEnum))
    example_geojson = Column(GeographyGeoJson)
    
longitude = 1.0
latitude = 0.0
Example(
    example_enum=MyEnum.RED,
    example_geojson={'type': 'Point', 'coordinates': [longitude, latitude]}
)
```

### Paritioning and Clustering

https://cloud.google.com/bigquery/docs/creating-clustered-tables
https://medium.com/google-cloud/bigquery-optimized-cluster-your-tables-65e2f684594b

```python
class Example(Model):

    __tablename__ = 'example_dataset.example'  # The table will be named 'example' in the dataset 'example_dataset'
    __table_args__ = {
        'info': {
            'bigquery_partition_by': [None],
            'bigquery_cluster_by': ['example_version', 'example_string_property']
        }
    }

    # bigquery_partition_by can be used to create a partitioned table
    # This must be a list of length 1.  Use None to specify the default partition column _PARTITIONTIME
    # Otherwise, you can specify a DATE or a TIMESTAMP Column.

    # bigquery_cluster_by can be used to create a clustered table
    # This must be a list of length between 1 and 4 inclusive.
    # Note that since as of 2019/02/25 only parition tables can be clustered, if
    # you specify bigquery_cluster_by and not bigquery_partition_by then bigquery_partition_by will be set to [None]
    # The order of columns names in bigquery_cluster_by is the order they will be clustered by.

    # ... your columns here
```

# Setup / Deployment

If you're using it locally you'll need to setup default credentials:

```
gcloud auth application-default login
```
