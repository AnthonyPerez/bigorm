# bigorm

This project was created before the official [SQLAlchemy dialect for BigQuery](https://github.com/googleapis/python-bigquery-sqlalchemy) was released. Now that the official dialect is released, this project is deprecated.


This project provides an abstract base class mimicing sqlalchemy's [declarative base class](https://docs.sqlalchemy.org/en/20/orm/mapping_api.html#sqlalchemy.orm.DeclarativeBase) which can be used to create a [declarative mapping](https://docs.sqlalchemy.org/en/20/orm/mapping_styles.html#declarative-mapping) to BigQueryTables.
This base class wraps a number of the expected sqlalchemy functionality to support interacting with BigQuery as well as providing new BigQuery specific functionality.
Declaring models and querying tables should be familiar to users of sqlalchemy with most sqlalchemy types supported.
This is no concept of a [session](https://docs.sqlalchemy.org/en/20/orm/session_basics.html#what-does-the-session-do) when interacting with BigQuery through the wrapped ORM.
At the time of writing, BigQuery did not support [primary keys](https://cloud.google.com/bigquery/docs/information-schema-table-constraints).

```python
import datetime
import pandas as pd
from sqlalchemy import Float, Integer, DateTime
from bigorm.abstract_models import BigQueryModel as Model, BigQueryColumn as Column
from bigorm.database import BigQueryDatabaseContext as DatabaseContext


class Example(Model):

    __tablename__ = 'example_dataset.example'  # The table will be named 'example' in the dataset 'example_dataset'
    example_integer = Column(Integer)  # Creates a column in the table named 'example_integer'
    example_float_property = Column(Float, nullable=False)  # Create a property that is not allowed to be empty with nullable=False

    # Create a column that automatically gets populated with the current datetime (when the object instance is created)
    example_date_created = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)

with DatabaseContext(project='example_project'):
    if not Example.table_exists():
        Example.table_create()

    dataframe = pd.DataFrame({
        'example_float_property': [1.1, 2.2, 5.2],
        'example_date_created': [datetime.datetime.utcnow()] * 3,
        'example_integer_labeled_differently': [None, 3, None],
    })
    Example.create_from_pandas(
        df=dataframe,
        relabel={
            'example_integer_labeled_differently': 'example_integer'
        },
        create_method='load_job'
    )

    query_df = Example.query().all_as_pandas()
```

See `Examples.md` in docs folder.
