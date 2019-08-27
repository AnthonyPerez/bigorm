import functools
import six
import json

import pandas as pd
from bigorm.database import Base
import sqlalchemy as sa

import google.api_core.exceptions
from google.cloud.bigquery import job as bigquery_job
from google.cloud.bigquery.dbapi.cursor import job, exceptions

from bigorm.database import DatabaseContext
from bigorm.serialization import JsonSerializableOrmMixin, _dicts_to_geojson
from bigorm.tables import BigQueryTableCRUDMixin
from bigorm.utils import _get_table_ref


class BigQueryOrmError(RuntimeError):
    pass


class BigQueryCRUDMixin(object):
    """
        Updates and Deletion can actually be done through the query object
        See BigQueryQueryMixin.delete() and BigQueryQueryMixin.update()
        Example:
            TestModel.query().filter_by(column1=1).delete()
    """
    __table_args__ = {'extend_existing': True}

    @classmethod
    def _create_streaming(cls, instances):
        client = DatabaseContext.get_session().connection().connection._client
        table_ref = _get_table_ref(cls.__table__.name, client)
        table = client.get_table(table_ref)

        # https://cloud.google.com/bigquery/quotas#streaming_inserts
        empty_row = {field.name: None for field in table.schema}
        seq_of_parameters = [inst.serialize_as_dict() for inst in instances]
        seq_of_parameters = [dict(empty_row, **params) for params in seq_of_parameters]
        errors = client.insert_rows(table, seq_of_parameters)
        if len(errors) > 0:
            raise exceptions.DatabaseError(errors)

    @classmethod
    def create(cls, instances, batch_size=None):
        """
        Load instances through the streaming inserts API.
        https://cloud.google.com/bigquery/quotas#streaming_inserts
        Maximum row size: 1MB
        Maximum HTTP request size: 10MB
        Maximum rate: 100,000 rows/s per project and 100MB/s per table.

        Args:
            instances (List[BigQueryCRUDMixin]):  Instances of cls.
                These will be appended to the database, duplicates will be added.
                Table metadata is eventually consistent.  This means that if you've
                recently create this table or changed the schema, this method may
                incorrectly report no errors.
            batch_size (Optional[int]):  The batch size to use when uploading data.
            As of 2/13/19 Big query has a 10MB upload size limit.  Batching
            will allow larger requests to go through.
            See https://cloud.google.com/bigquery/quotas#streaming_inserts
            If None, will send all data at once.
            Defaults to None.
        """
        if not all([type(inst) == cls for inst in instances]):
            raise BigQueryOrmError('Got invalid class in {}\'s create method'.format(cls))

        if batch_size is None:
            cls._create_streaming(instances)
            return

        if batch_size < 1:
            raise ValueError('batch_size was {}'.format(batch_size))

        for i in range(0, len(instances), batch_size):
            instances_slice = instances[i:i+batch_size]
            cls._create_streaming(instances_slice)

    @classmethod
    def create_load_job(cls, instances):
        """
        Load instances through a load job.
        The job is asynchronous but this function will wait for the job to complete.
        https://cloud.google.com/bigquery/quotas#load_jobs
        Limited to 1000 per table per day.
        Maximum row size limit is 100MB.
        https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json

        Args:
            instances (List[BigQueryCRUDMixin]):  Instances of cls.
                These will be appended to the database, duplicates will be added.
                Table metadata is eventually consistent.  This means that if you've
                recently create this table or changed the schema, this method may
                incorrectly report no errors.
        """
        if not all([type(inst) == cls for inst in instances]):
            raise BigQueryOrmError('Got invalid class in {}\'s create method'.format(cls))

        instances_json_str = '\n'.join([
            instance.serialize_as_json() for instance in instances
        ])
        json_bytes_file = six.BytesIO(instances_json_str.encode('utf-8'))

        client = DatabaseContext.get_session().connection().connection._client
        table_ref = _get_table_ref(cls.__table__.name, client)

        job_config = bigquery_job.LoadJobConfig(
            autodetect=False,
            create_disposition=bigquery_job.CreateDisposition.CREATE_NEVER,
            ignore_unknown_values=False,
            source_format=bigquery_job.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery_job.WriteDisposition.WRITE_APPEND
        )
        load_job = client.load_table_from_file(
            file_obj=json_bytes_file,
            destination=table_ref,
            job_config=job_config
        )

        try:
            load_job.result()
        except Exception as e:
            raise exceptions.DatabaseError('{}\n{}\n{}\n{}'.format(
                load_job.errors,
                '{}({})'.format(type(e), e),
                load_job.error_result,
                'This error may have occured because a column'
                ' default value could not be created locally.  Only'
                ' scalar defaults or python callables are supported.',
            ))

        if ((load_job.error_result and len(load_job.error_result) > 0)
            or (load_job.errors and len(load_job.errors) > 0)):
            raise exceptions.DatabaseError('{}\n{}'.format(load_job.errors, load_job.error_result))

    @classmethod
    def _create_helper(cls, create_method, instances, **kwargs):
        """
        Helper method for passing arguments to the other create methods

        Args:
            create_method (str):  One of 'streaming' or 'load_job'.
                If 'streaming', will use the streaming API.
                If 'load_job', will use the load job API.
                Defaults to 'streaming'.
            instances (List[BigQueryCRUDMixin]):  Instances of cls.
                These will be appended to the database, duplicates will be added.
                Table metadata is eventually consistent.  This means that if you've
                recently create this table or changed the schema, this method may
                incorrectly report no errors.
            **kwargs (Dict[str, Any]): Arguments are passed to appropriate
                create function.
        """
        if create_method == 'streaming':
            cls.create(instances, batch_size=kwargs.get('batch_size', None))
        elif create_method == 'load_job':
            cls.create_load_job(instances)
        else:
            raise ValueError('Unrecognized create_method: {}'.format(create_method))

    @classmethod
    def create_from_query(cls, query, flatten_results=True):
        """
        Load instances through a query job.
        The job is asynchronous but this function will wait for the job to complete.
        See https://cloud.google.com/bigquery/docs/writing-results
        Note that this method must compile the sql query to a string.
        It does so using sqlalchemy_query.statement.compile(compile_kwargs={"literal_binds": True}).
        This will fail for certain queries and should not be used for queries which depend on untrusted input.
        See https://docs.sqlalchemy.org/en/13/faq/sqlexpressions.html for more information.
        Args:
            query (BigQueryQuery):  A query object whose results are
                to be appended to the table.
            flatten_results (Optional[bool]): If True, will flatten the query results.
                Defaults to True.
        """
        client = DatabaseContext.get_session().connection().connection._client
        table_ref = _get_table_ref(cls.__table__.name, client)

        job_config = bigquery_job.QueryJobConfig(
            destination=table_ref,
            create_disposition=bigquery_job.CreateDisposition.CREATE_NEVER,
            write_disposition=bigquery_job.WriteDisposition.WRITE_APPEND,
            flatten_results=flatten_results,
            allow_large_results=not flatten_results,
        )

        dialect = DatabaseContext.get_engine().dialect
        compiled_sql = query.sqlalchemy_query.statement.compile(
            dialect=dialect,
            compile_kwargs={
                'literal_binds': True,
            }
        )
        raw_sql = str(compiled_sql)

        query_job = client.query(
            raw_sql,
            job_config=job_config
        )

        try:
            query_job.result()
        except Exception as e:
            raise exceptions.DatabaseError('{}\n{}\n{}'.format(
                query_job.errors,
                '{}({})'.format(type(e), e),
                query_job.error_result,
            ))

        if ((query_job.error_result and len(query_job.error_result) > 0)
            or (query_job.errors and len(query_job.errors) > 0)):
            raise exceptions.DatabaseError('{}\n{}'.format(query_job.errors, query_job.error_result))

    @classmethod
    def parse_from_pandas(cls, df, relabel=None,
                          if_exists='append'):
        """
        Create instances from a pandas DataFrame.  If the table does not
        exist it will be created (see if_exists).

        Args:
            df (pandas.DataFrame):  The data frame to be converted
                to class instances.  Column names must match the
                column names in the ORM class.  Will fail if the column
                names do not match the names of the properties that
                represent them in the ORM class.
            relabel (Optional[Mapping[str, str]]):
                A dictionary that maps pandas column names to names
                of properties in the ORM.  This is required if
                the pandas column names differ from the property
                names representing columns in this class.
            if_exists (str):  One of {'fail', 'replace', 'append'}, default 'append'.
                How to behave if the table already exists.
                fail: Raise a ValueError.
                replace: Drop the table before inserting new values.
                append: Insert new values to the existing table.
        Returns:
            (List[BigQueryCRUDMixin]):  Returns a list
                of class instances.
        """
        if relabel is None:
            relabel = {}
        instances = []

        def method(sqlalchemy_table, conn, keys, data_iter):
            """
            Args:
                sqlalchemy_table (): Ignored
                conn (): Ignored
                keys (Tuple): The column names.
                data_iter (Iterable[Tuple]):  An iterable
                    iterating over column values.  Matches keys.
            """
            keys = tuple(relabel.get(key, key) for key in keys)
            for params in data_iter:
                instances.append(
                    cls(**dict(zip(keys, params)))
                )

        table_name = cls.__tablename__
        df.to_sql(
            name=table_name, con=DatabaseContext.get_engine(),
            if_exists=if_exists, index=False,
            method=method,
        )
        return instances


    @classmethod
    def create_from_pandas(cls, df, relabel=None,
                           if_exists='append', batch_size=None,
                           create_method='streaming'):
        """
        Uploads from a pandas DataFrame.  If the table does not
        exist it will be created (see if_exists).

        Args:
            See arguments of parse_from_pandas
            batch_size (Optional[int]):  The batch size to use when uploading data.
                As of 2/13/19 Big query has a 10MB upload size limit.  Batching
                will allow larger requests to go through.
                See https://cloud.google.com/bigquery/quotas#streaming_inserts
                If None, will send all data at once.
                Defaults to None.
                Only applies to the streaming API.
            create_method (str):  One of 'streaming' or 'load_job'.
                If 'streaming', will use the streaming API.
                If 'load_job', will use the load job API.
                Defaults to 'streaming'.
        """
        instances = cls.parse_from_pandas(
            df=df,
            relabel=relabel,
            if_exists=if_exists,
        )
        cls._create_helper(create_method, instances, batch_size=batch_size)

    @classmethod
    def parse_from_geojson(cls, geojson, geometry_property_name,
                           relabel=None, ignore=None, defaults=None,
                           allow_null_geometry=False):
        """
        Args:
            geojson (Mapping[str, Any]): A geojson with only one
            geometry per feature.  Expected format is:
            {
                ... (ignores other properties)
                features: [
                    ... (other features)
                    {
                        ... (ignores other properties)
                        geometry: {
                            ... (other valid geojson geometry properties)
                            type: TYPE,
                            coordinates: [COORDINATES...]
                        },
                        properties: {
                            ... (all properties here are
                            expected to match property names in
                            the ORM class)
                        },
                    }
                ]
            }
            geometry_property_name (Optional[str]):
                The name of the property in the ORM
                class for which to set the geometry from the
                geojson.  If None, will not set geometry.
                The value passed to this property
                will be exactly what is in the geojson.
            relabel (Optional[Mapping[str, str]]):
                A mapping from properties in the geojson
                to the ORM property name.  Applies the
                renaming before ORM mapping.  Use this
                when the geojson properties do not match
                the ORM properties.
            ignore (Optional[Container[str]]):  Any property
                in ignore will not be used to populate new
                rows in the Table.
                ignore applies before relabel.
            defaults (Optional[Dict[str, Any]]):
                Default values for any properties.
                Will be overridden by the actual values
                of the feature.
            allow_null_geometry (bool):  If False,
                will raise a ValueError when a feature
                without a 'geometry' property is encountered.
                Note that a 'geometries' property is valid
                in geojson but will be ignored.
                Defaults to False.
        Returns:
            (List[BigQueryCRUDMixin]):  Returns a list
                of class instances.
        """
        if relabel is None:
            relabel = {}
        if ignore is None:
            ignore = []
        if defaults is None:
            defaults = {}

        def feature_to_instance(feature):
            geometry = feature.get('geometry', None)
            if geometry is None and (not allow_null_geometry):
                raise ValueError('Geojson contained feature without geometry')

            properties = feature['properties']
            properties = {
                relabel.get(k, k): v
                for k, v in properties.items()
                if k not in ignore
            }
            if geometry_property_name:
                if geometry_property_name in properties:
                    raise ValueError(
                        'geometry_property_name {} was found in properties {}'.format(
                            geometry_property_name, properties
                        )
                    )

                properties[geometry_property_name] = geometry

            kwargs = dict(defaults)
            kwargs.update(properties)
            return cls(**kwargs)

        instances = [feature_to_instance(f) for f in geojson['features']]
        return instances

    @classmethod
    def create_from_geojson(cls, geojson, geometry_property_name,
                            relabel=None, ignore=None, defaults=None,
                            allow_null_geometry=False,
                            batch_size=None, create_method='streaming'):
        """
        Args:
            See arguments of parse_from_geojson
            batch_size (Optional[int]):  The batch size to use when uploading data.
                As of 2/13/19 Big query has a 10MB upload size limit.  Batching
                will allow larger requests to go through.
                See https://cloud.google.com/bigquery/quotas#streaming_inserts
                If None, will send all data at once.
                Defaults to None.
                Only applies to the streaming API.
            create_method (str):  One of 'streaming' or 'load_job'.
                If 'streaming', will use the streaming API.
                If 'load_job', will use the load job API.
                Defaults to 'streaming'.
        """
        instances = cls.parse_from_geojson(
            geojson=geojson, geometry_property_name=geometry_property_name,
            relabel=relabel, ignore=ignore, defaults=defaults,
            allow_null_geometry=allow_null_geometry
        )
        cls._create_helper(create_method, instances, batch_size=batch_size)


class BigQueryQuery(object):
    """
    Wraps the Query object with the following object.
    1) Every query function that returns a new query will return
    a new BigQueryQuery instead. (wrapped below)
    2) Every query function that executes a query that is safe
        for sqlalchemy to execute against big query
        will execute through sqlalchemy.
    3) Every query function that executes a query that is not safe
        for sqlalchemy to execute against Bigquery will instead
        use the big query client directly and perform an equivalent function.
    """

    SAFE_SQLALCHEMY_QUERIES_EXCUTIONS = [
        'as_scalar', 'column_descriptions', 'count', 'delete',
        'cte', 'exists', 'get_execution_options', 'label',
        'selectable', 'statement', 'subquery',
    ]

    NOT_SUPPORTED = [
        'from_statement', 'get', 'merge_result',
        'populate_existing', 'values', 'value',
        'with_parent', 'with_session',
    ]
    # TODO: values and value seem like they are easily supported.
    # TODO: Does Bigquery place nicely with sqlalchemy relationships?  If so then with_parent may be allowable

    @staticmethod
    def sqlalchemy_query_fn_wrapper(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return BigQueryQuery(f(*args, **kwargs))
        return wrapped

    def query_raw(self, sql_statement):
        """
        Execute a sql statement, attempt to convert the query
        results to the appropriate ORM objects (passed to query constructor).
        Args
            sql_statement (str):  A sql statement to execute
        Returns:
            (Iterable[Any]):  An iterable of the query results.
        """
        result_proxy = DatabaseContext.get_engine().execute(sql_statement)
        return self.instances(result_proxy)

    def __init__(self, sqlalchemy_query):
        self.sqlalchemy_query = sqlalchemy_query

    def __getattr__(self, name):
        try:
            return super(BigQueryQuery, self).__getattr__(name)
        except AttributeError as _:
            sqlalchemy_fn = getattr(self.sqlalchemy_query, name)
            if name in self.SAFE_SQLALCHEMY_QUERIES_EXCUTIONS:
                return sqlalchemy_fn
            elif name in self.NOT_SUPPORTED:
                raise BigQueryOrmError('Not supported')
            else:
                return BigQueryQuery.sqlalchemy_query_fn_wrapper(sqlalchemy_fn)

    def all(self):
        """
        Returns:
            (Iterable[Any]):  The result of the query as an interable,
                with returned columns converted to the appropriate representation
                in the ORM or in their raw values.
        """
        # Note: Regular sqlalchemy's Query.all() will
        # remove what it thinks are duplicates based on the primary
        # key.  This will not.
        return self.query_raw(self.sqlalchemy_query.statement)

    def all_as_list(self):
        """
        Returns:
            (List[Any]):  The result of the query as a list,
                with returned columns converted to the appropriate representation
                in the ORM or in their raw values.
        """
        return list(self.all())

    def all_as_pandas(self):
        """
        Returns:
            (pandas.DataFrame):  The result of the query as a pandas DataFrame.
        """
        return pd.read_sql(
            self.sqlalchemy_query.statement,
            DatabaseContext.get_engine()
        )

    def all_as_dicts(self):
        """
        Returns:
            (List[Dict[str, Any]]):  The result of the query as a list of dicts.
        """
        return self.all_as_pandas().to_dict('records')

    def all_as_geojson(self, geometry_column,
                       excluded_keys=None, as_str=True):
        """
        Args:
            geometry_column (Optional[str]):  The name of the property of the model
                meant to represent the geometry.  Must have type GeographyGeoJson or GeographyWKT.
                May be set to None to return features with null geometries.
            excluded_keys (Iterable[str]):  A list of properties to exclude.
            as_str (bool):  If true, will serialize the geojson to a string.
                Otherwise will return the geojson as a dict.
                Defaults to True.
        Returns:
            (Union[Dict[str, Any], str]):  A feature collection in dictionary form
                where feature properties match model properties.
                If as_str is true, this will be serialized to a string.
        """
        return _dicts_to_geojson(
            dicts=self.all_as_dicts(),
            geometry_column=geometry_column,
            excluded_keys=excluded_keys,
            as_str=as_str,
        )

    def first(self):
        """
        Returns:
            (Optional[Any]):  The first result of the query, or None if it retuns nothing.
        """
        query = self.limit(1)
        for result in query.all():
            return result
        return None

    def one_or_none(self):
        """
        Returns:
            (Optional[Any]):  The first result of the query, or None if it retuns nothing.
        Raises:
            (sa.orm.exc.MultipleResultsFound): If the query returned more than 1 result.
        """
        query = self.limit(2)
        results = list(query.all())
        if len(results) == 0:
            return None
        if len(results) > 1:
            raise sa.orm.exc.MultipleResultsFound
        return results[0]

    def one(self):
        """
        Returns:
            (Optional[Any]):  The first result of the query.
        Raises:
            (sa.orm.exc.MultipleResultsFound): If the query returned more than 1 result.
            (sa.orm.exc.NoResultFound):  If the query had no results.
        """
        one = self.one_or_none()
        if one is None:
            raise sa.orm.exc.NoResultFound
        return one

    def scalar(self):
        """
        Returns:
            (Optional[Any]):  The first column of the first result of the query, or None
                if the query is empty.
        Raises:
            (sa.orm.exc.MultipleResultsFound): If the query returned more than 1 result.
        """
        ret = self.one_or_none()
        if ret:
            if not isinstance(ret, tuple):
                return ret
            return ret[0]
        return ret

    def update(self, values, update_args=None):
        """
        Args:
            See sqlalchemy.orm.Query.
        Returns:
            (int):  The number of updated rows.
        """
        return self.sqlalchemy_query.update(
            values,
            synchronize_session=False,
            update_args=update_args,
        )

    def instances(self, cursor):
        query = self.sqlalchemy_query
        context = sa.orm.query.QueryContext(query)

        context.runid = sa.orm.loading._new_runid()
        context.post_load_paths = {}

        single_entity = (
            not query._only_return_tuples
            and len(query._entities) == 1
            and query._entities[0].supports_single_entity
        )

        try:
            (process, labels) = list(
                zip(
                    *[
                        query_entity.row_processor(query, context, cursor)
                        for query_entity in query._entities
                    ]
                )
            )

            if not single_entity:
                keyed_tuple = sa.util._collections.lightweight_named_tuple("result", labels)

            while True:
                context.partials = {}

                if query._yield_per:
                    fetch = cursor.fetchmany(query._yield_per)
                    if not fetch:
                        break
                else:
                    fetch = cursor.fetchall()

                if single_entity:
                    proc = process[0]
                    rows = [proc(row) for row in fetch]
                else:
                    rows = [
                        keyed_tuple([proc(row) for proc in process])
                        for row in fetch
                    ]

                for path, post_load in context.post_load_paths.items():
                    post_load.invoke(context, path)

                for row in rows:
                    yield row

                if not query._yield_per:
                    break
        finally:
            cursor.close()

    def __iter__(self):
        return self.all()

    def __str__(self):
        return self.sqlalchemy_query.__str__()

    def __getitem__(self, *args, **kwargs):
        raise BigQueryOrmError('Not Supported')


class BigQueryQueryMixin(object):

    @classmethod
    def query(cls, *args, **kwargs):
        """
        https://docs.sqlalchemy.org/en/latest/orm/query.html#sqlalchemy.orm.query.Query

        Selects all columns of the class and any additional
        ORM objects requested through arguments.

        Args:
            *args (Union[Column, BigQueryModel]):
                Columns or classes matching what the sql statement is expected to return
                (e.g. what is selects).
            **kwargs (Any):  Passed to sqlalchemy.orm.query
        Returns
            (BigQueryQuery):  A query object that wraps sqlalchemy.orm.Query.
        """
        return BigQueryQuery(
            DatabaseContext.get_session().query(cls, *args, **kwargs)
        )

    @classmethod
    def query_empty(cls, *args, **kwargs):
        """
        https://docs.sqlalchemy.org/en/latest/orm/query.html#sqlalchemy.orm.query.Query

        Selects no columns by default.

        Args:
            *args (Union[Column, BigQueryModel]):
                Columns or classes matching what the sql statement is expected to return
                (e.g. what is selects).
            **kwargs (Any):  Passed to sqlalchemy.orm.query
        Returns
            (BigQueryQuery):  A query object that wraps sqlalchemy.orm.Query.
        """
        return BigQueryQuery(
            DatabaseContext.get_session().query(*args, **kwargs)
        )

    @classmethod
    def query_raw(cls, sql_statement, *args, **kwargs):
        """
        Execute a raw sql statement against the BigQuery API
        and then attempt to return the result in ORM classes.
        ORM objects match those passed through *args.
        If you don't want to use the ORM, this method is not appropriate.

        Args:
            sql_statement (str):  Execute this raw sql_statement.
            *args (Union[Column, BigQueryModel]):
                Columns or classes matching what the sql statement is expected to return
                (e.g. what is selects).
            **kwargs (Any):  Passed to sqlalchemy.orm.query
        Returns:
            (Iterable[Any]):  The result of the query,
                with returned columns converted to the appropriate representation
                in the ORM or in their raw values (matching *args).
        """
        return cls.query_empty(*args, **kwargs)._query_raw(sql_statement)


class BigQueryColumn(sa.Column):

    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        kwargs['nullable'] = kwargs.get('nullable', True)
        if kwargs.get('default', None) and kwargs['nullable']:
            raise BigQueryOrmError('default values are only implemented for REQUIRED columns.')
        if kwargs.get('server_default', None) is not None:
            raise BigQueryOrmError('Bigquery does not support server defaults.')
        super(BigQueryColumn, self).__init__(*args, **kwargs)
        if self.name == '_PARTITIONTIME':
            raise ValueError('_PARTITIONTIME is a reserved column name in BigQuery.')


class BigQueryModel(BigQueryCRUDMixin, BigQueryQueryMixin, JsonSerializableOrmMixin, BigQueryTableCRUDMixin, Base):
    """Base model class that includes CRUD convenience methods."""
    __abstract__ = True
