from google.cloud.bigquery.dbapi.cursor import exceptions


def _get_table_ref(table_name, client):
    table_name_split = table_name.split('.')

    if len(table_name_split) == 3:
        project_name, dataset_name, table_name = table_name_split
        raise NotImplementedError('You must supply the project directly to the client.')
    elif len(table_name_split) == 2:
        dataset_name, table_name = table_name_split
        table_ref = client.dataset(dataset_name).table(table_name)
    elif len(table_name_split) == 1:
        default_dataset = client._default_query_job_config.default_dataset
        if default_dataset:
            table_ref = default_dataset.table(table_name)
        else:
            raise exceptions.ProgrammingError('Table name missing dataset and default dataset not provided: {}'.format(table_name))
    else:
        raise exceptions.ProgrammingError('Unrecognized table name: {}'.format(table_name))

    return table_ref