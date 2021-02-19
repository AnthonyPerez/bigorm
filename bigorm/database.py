import threading
import functools

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class DatabaseContextError(RuntimeError):
    pass


"""
Once an engine is created is is not destroyed until the program itself exits.
Engines are used to produce a new session when a context is entered.
When a context is exited, the session for that context is destroyed.
"""
global_database_context = threading.local()


class DatabaseContext(object):
    """
    This is fairly complicated.  Follow these rules:
    1) Do not create threads in a DatabaseConext.  If you
        do you will lose the context.
    2) With async/await asychronous programming,
        enter contexts in atmotic blocks (do not await in a context).

    Usage:
        with DatabaseContext():
    """
    @classmethod
    def __get_engines(_):
        if not hasattr(global_database_context, 'engines'):
            global_database_context.engines = {}
        return global_database_context.engines

    @classmethod
    def __get_sessions(_):
        if not hasattr(global_database_context, 'sessions'):
            global_database_context.sessions = []
        return global_database_context.sessions

    @classmethod
    def get_session(_):
        sessions = DatabaseContext.__get_sessions()
        if len(sessions) == 0:
            raise DatabaseContextError('Session not established, did you create a DatabaseContext?')

        _, session = sessions[-1]
        return session

    @classmethod
    def get_engine(_):
        sessions = DatabaseContext.__get_sessions()
        if len(sessions) == 0:
            raise DatabaseContextError('Session not established, did you create a DatabaseContext?')

        engine, _ = sessions[-1]
        return engine

    @classmethod
    def is_in_context(_):
        sessions = DatabaseContext.__get_sessions()
        return len(sessions) > 0

    def __init__(self, *args, **kwargs):
        """
        All arguments are forwarded to create_engine
        """
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        key = (tuple(self.args), tuple(sorted(list(self.kwargs.items()))))
        engine, Session = DatabaseContext.__get_engines().get(key, (None, None))
        if engine is None:
            engine = sqlalchemy.create_engine(
                *self.args,
                **self.kwargs
            )
            Session = sqlalchemy.orm.sessionmaker(bind=engine)
            DatabaseContext.__get_engines()[key] = (engine, Session)

        new_session = Session()
        DatabaseContext.__get_sessions().append(
            (engine, new_session)
        )

    def __exit__(self, exception_type, exception_value, traceback):
        _, session = DatabaseContext.__get_sessions().pop()
        try:
            if exception_type is not None:
                # There was an exception, roll back.
                session.rollback()
        finally:
            session.close()


class BigQueryDatabaseContext(DatabaseContext):

    def __init__(self, project='', default_dataset='', **kwargs):
        """
        Args:
            project (Optional[str]):  The project name, defaults to
                your credential's default project.
            default_dataset (Optional[str]): The default dataset.
                This is used in the case where the table has no
                dataset referenced in it's __tablename__
            **kwargs (kwargs):  Keyword arguments are passed to create_engine.
                Example:
                'bigquery://some-project/some-dataset' '?'
                'credentials_path=/some/path/to.json' '&'
                'location=some-location' '&'
                'arraysize=1000' '&'
                'clustering_fields=a,b,c' '&'
                'create_disposition=CREATE_IF_NEEDED' '&'
                'destination=different-project.different-dataset.table' '&'
                'destination_encryption_configuration=some-configuration' '&'
                'dry_run=true' '&'
                'labels=a:b,c:d' '&'
                'maximum_bytes_billed=1000' '&'
                'priority=INTERACTIVE' '&'
                'schema_update_options=ALLOW_FIELD_ADDITION,ALLOW_FIELD_RELAXATION' '&'
                'use_query_cache=true' '&'
                'write_disposition=WRITE_APPEND'

                These keyword arguments match those in the job configuration:
                https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.QueryJobConfig.html#google.cloud.bigquery.job.QueryJobConfig
        """
        connection_str = 'bigquery://{}/{}'.format(project, default_dataset)

        if len(kwargs) > 0:
            connection_str += '?'
            for k, v in kwargs.items():
                connection_str += '{}={}&'.format(k, v)
            connection_str = connection_str[:-1]

        super(BigQueryDatabaseContext, self).__init__(
            connection_str
        )


def requires_database_context(f):
    """
    Dectorator that causes the function
    to throw a DatabaseContextError if the function is called
    but a DatabaseContext has not been entered.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not DatabaseContext.is_in_context():
            raise DatabaseContextError('Session not established, did you create a DatabaseContext?')
        return f(*args, **kwargs)

    return wrapper
