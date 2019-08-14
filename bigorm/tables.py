import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
from sqlalchemy.ext.compiler import compiles
import google.api_core.exceptions

from bigorm.database import DatabaseContext
from bigorm.utils import _get_table_ref


def _read_create_table_info(element):
    partition_by = element.element.info.get('bigquery_partition_by', None)
    cluster_by = element.element.info.get('bigquery_cluster_by', None)

    if isinstance(cluster_by, list):
        if not isinstance(partition_by, list):
            partition_by = [None]

    if isinstance(partition_by, list):
        if len(partition_by) != 1:
            raise ValueError('partition_by must be a list of length 1.')

    if isinstance(cluster_by, list):
        if len(cluster_by) == 0 or len(cluster_by) > 4:
            raise ValueError('cluster_by must be a list of length in [1, 4].')

    return (partition_by, cluster_by)


def get_column_by_name(element, column_name):
    table = element.element
    for column in table.columns:
        if column_name == column.name:
            return column
    return None


@compiles(CreateTable, "bigquery")
def _add_suffixes(element, compiler, **kw):
    text = compiler.visit_create_table(element, **kw)

    partition_by, cluster_by = _read_create_table_info(element)

    if isinstance(partition_by, list):
        partition_by = partition_by[0]
        if partition_by is None:
            partition_clause = 'PARTITION BY DATE(_PARTITIONTIME)'
        else:
            column = get_column_by_name(element, partition_by)
            if column is None:
                raise ValueError('Column {} not found for table partition.'.format(partition_by))
            elif isinstance(column.type, sa.types.TIMESTAMP):
                partition_clause = 'PARTITION BY DATE(`{}`)'.format(partition_by)
            elif isinstance(column.type, sa.types.DATE):
                partition_clause = 'PARTITION BY `{}`'.format(partition_by)

        partition_clause = '\n{}\n'.format(partition_clause)
        text += partition_clause

    if isinstance(cluster_by, list):
        cluster_clause = 'CLUSTER BY {}'.format(
            ', '.join(['\t`{}`'.format(field_name) for field_name in cluster_by])
        )
        cluster_clause = '\n{}\n'.format(cluster_clause)
        text += cluster_clause

    return text 


class BigQueryTableCRUDMixin(object):

    @classmethod
    def table_get(cls):
        """
        Returns:
            (google.cloud.bigquery.table.Table):  The table this class maps to.
        Raises:
            (google.api_core.exceptions.NotFound): If the table does not exist.
        """
        client = DatabaseContext.get_session().connection().connection._client
        table_ref = _get_table_ref(cls.__table__.name, client)
        table = client.get_table(table_ref)
        return table

    @classmethod
    def table_exists(cls):
        """
        Returns:
            (bool): True if the table exists, false otherwise.
        """
        try:
            cls.table_get()
        except google.api_core.exceptions.NotFound:
            return False
        return True

    @classmethod
    def table_create(cls):
        """
        Creates the table corresponding to this class
        """
        engine = DatabaseContext.get_engine()
        cls.__table__.create(engine)

    @classmethod
    def table_delete(cls):
        """
        Deletes the table corresponding to this class
        """
        engine = DatabaseContext.get_engine()
        cls.__table__.drop(engine)
