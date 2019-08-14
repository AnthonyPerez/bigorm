import datetime
import json
import itertools
from enum import Enum as PythonEnum

import sqlalchemy as sa

from bigorm.database import DatabaseContext, DatabaseContextError


class JsonSerializableOrmKeyError(KeyError):
    pass


class JsonSerializableOrmRuntimeError(RuntimeError):
    pass


def _dicts_to_geojson(dicts, geometry_column,
                      excluded_keys=None, as_str=False):
    """
    Returns the dicts as geojson FeatureCollection.

    Args:
        dicts (Dict[str, Any]):  An iterable of dicts.
        geometry_column (Optional[str]):  The name of the property of the model
            meant to represent the geometry.  Must have type GeographyGeoJson or GeographyWKT.
            May be set to None to return features with null geometries.
            Will attempt to parse strings into geojson dicts.
        excluded_keys (Iterable[str]):  A list of properties to exclude.
        as_str (bool):  If true will serialize the dict to a string.
            Defaults to False.
    Returns:
        (Union[Dict[str, Any], str]):  A feature collection in dictionary form
            where feature properties match model properties.
            If as_str is true, this will be serialized to a string.
    Example:
        {
            'type': 'FeatureCollection',
            'features': [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [5, 7]
                    },
                    "properties": {
                        'prop1': 'prop1_value'
                    }
                }
            ]
        }
    """
    if excluded_keys is None:
        excluded_keys = set()

    def dict_to_feature(_dict):
        properties = {k: v for k, v in _dict.items() if k not in excluded_keys}
        geometry = None
        if geometry_column:
            try:
                geometry = properties.pop(geometry_column)
            except KeyError as e:
                raise JsonSerializableOrmKeyError(str(e))

            if isinstance(geometry, str):
                try:
                    geometry = json.loads(geometry)
                except Exception as e:
                    raise JsonSerializableOrmRuntimeError(str(e))

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": properties,
        }

        return feature

    geojson = {
        'type': 'FeatureCollection',
        'features': [dict_to_feature(_dict) for _dict in dicts],
    }

    if as_str:
        geojson = _json_dump(geojson)

    return geojson


class JsonSerializableOrmMixin(object):
    """
    Implementes as_json, __json__, and __repr__ generically for sql alchemy models.
    https://github.com/kolypto/py-flask-jsontools/blob/master/flask_jsontools/formatting.py
    """

    @staticmethod
    def get_entity_property_names_to_columns(entity):
        """ 
        Get entity properties corresponding to columns

        Args:
            entity (sqlalchemy.ext.declarative.api.DeclarativeMeta)
        Returns:
            (Dict[str, List[sqlalchemy.schema.Column]]): Mapping from entity property names to columns
        """
        ins = entity if isinstance(entity, sa.orm.state.InstanceState) else sa.inspect(entity)
        column_map = {k: ins.mapper.column_attrs[k].columns for k in ins.mapper.column_attrs.keys()}
        return column_map

    @staticmethod
    def get_entity_loaded_property_names_to_columns(entity):
        """ 
        Get entity property names that are loaded (e.g. won't produce new queries) and their mappings to columns.

        Args:
            entity (sqlalchemy.ext.declarative.api.DeclarativeMeta)
        Returns:
            (Dict[str, List[sqlalchemy.schema.Column]]): Mapping from entity property names to columns
        """
        ins = sa.inspect(entity)
        property_names_to_columns = JsonSerializableOrmMixin.get_entity_property_names_to_columns(ins)

        # If the entity is not transient -- exclude unloaded keys
        # Transient entities won't load these anyway, so it's safe to include all columns and get defaults
        # If the entity is expired -- reload expired attributes as well
        # Expired attributes are usually unloaded as well!
        if not ins.transient:
            unloaded_keys = set(ins.unloaded)
            if ins.expired:
                unloaded_keys = unloaded_keys - ins.expired_attributes
            for key in unloaded_keys:
                property_names_to_columns.pop(key, None)

        return property_names_to_columns

    @classmethod
    def get_property_names_to_columns(cls):
        """ 
        Get ORM class property names to columns mapping.
        This will not return relations.

        Args:
            cls (sqlalchemy.ext.declarative.api.DeclarativeMeta)
        Returns:
            (Dict[str, List[sqlalchemy.schema.Column]]): Mapping from entity property names to columns
        """
        ins = sa.inspect(cls)
        column_map = {k: ins.mapper.column_attrs[k].columns for k in ins.mapper.column_attrs.keys()}
        return column_map

    def serialize_as_dict(self, excluded_keys=None):
        """
        Returns this object as a dictionary.
        Will get default values as possible and will call bind_processor
        on any value.
        (e.g. bind_processor will turn geometries into geojson strings)

        Args:
            excluded_keys (Iterable[str]):  A list of keys to exclude.
        Returns:
            (Dict[str, Any]):  Returns the dict representation of this object
                with values populated by their defaults if available.
        """
        if excluded_keys is None:
            excluded_keys = set()
        else:
            excluded_keys = set(excluded_keys)
        attr_names = JsonSerializableOrmMixin.get_entity_loaded_property_names_to_columns(self)

        json_out = {}
        for property_name, columns in attr_names.items():
            if property_name in excluded_keys:
                continue

            if len(columns) > 1:
                raise ValueError('serialize_as_json does not support composite types.')
            column = columns[0]

            key = column.key
            value = getattr(self, property_name)

            if value is None:
                if column.default is not None:
                    default_arg = column.default.arg
                    if column.default.is_callable:
                        value = default_arg(None)
                    elif column.default.is_scalar:
                        value = default_arg

            if value is not None:
                bind_processor = column.type.bind_processor(
                    dialect=DatabaseContext.get_session().bind.dialect
                )
                if bind_processor is not None:
                    value = bind_processor(value)

            json_out[key] = value

        return json_out

    def serialize_as_json(self, excluded_keys=None):
        """
        Returns this object as a JSON string.
        Will get default values as possible and will call bind_processor
        on any value.
        (e.g. bind_processor will turn geometries into geojson strings)
        Will convert types as necessary to conform to Bigquery's
        canonical formats.

        Args:
            excluded_keys (Iterable[str]):  A list of keys to exclude.
        Returns:
            (str):  Returns the JSON representation of this object
                with values populated by their defaults if available.
        """
        return _json_dump(self.serialize_as_dict(excluded_keys=excluded_keys))

    @classmethod
    def serialize_as_geojson(cls, instances, geometry_column,
                                  excluded_keys=None):
        """
        Returns the instances as geojson FeatureCollection string.
        Will get default values as possible and will call bind_processor
        on any value.
        (e.g. bind_processor will turn geometries into geojson strings)

        Args:
            instances (Iterable[JsonSerializableOrmMixin]):  An iterable of instances to
                serialize.
            geometry_column (Optional[str]):  The name of the property of the model
                meant to represent the geometry.  Must have type GeographyGeoJson or GeographyWKT.
                May be set to None to return features with null geometries.
            excluded_keys (Iterable[str]):  A list of properties to exclude.
        Returns:
            (str):  A feature collection in dictionary form
                where feature properties match model properties.
                If as_str is true, this will be serialized to a string.
        Example:
            {
                'type': 'FeatureCollection',
                'features': [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [5, 7]
                        },
                        "properties": {
                            'prop1': 'prop1_value'
                        }
                    }
                ]
            }
        """
        return _dicts_to_geojson(
            dicts=[
                instance.serialize_as_dict(excluded_keys=excluded_keys)
                for instance in instances
            ],
            geometry_column=geometry_column,
            excluded_keys=excluded_keys,
            as_str=True
        )

    def __repr__(self):
        REPR_SIZE_LIMIT = 60
        PROPERTY_SIZE_LIMIT = 50

        has_context = True
        try:
            prop_val_pairs = list(self.serialize_as_dict().items())
        except DatabaseContextError:
            has_context = False
            attr_names = JsonSerializableOrmMixin.get_entity_property_names_to_columns(self)
            prop_val_pairs = [
                (property_name, getattr(self, property_name, None))
                for property_name, columns in attr_names.items()
                if (
                    (len(columns) == 1) and
                    (getattr(self, property_name, None) is not None)
                )
            ]

        prop_val_pairs = itertools.islice(
            sorted(prop_val_pairs, key=lambda ele: ele[0]),
            0, PROPERTY_SIZE_LIMIT,
        )
        column_strings = [
            '{}={}'.format(k, v)
            for k, v in prop_val_pairs
        ]
        column_strings = [
            s if len(s) < REPR_SIZE_LIMIT else s[:REPR_SIZE_LIMIT - 3] + '...'
            for s in column_strings
        ]
        return '{}({})'.format(
            type(self).__name__,
            ', '.join(column_strings),
        )


def bigquery_serialize_datetime(py_datetime):
    """
    Convert a python datetime object into a serialized format that Bigquery accepts.
    Accurate to milliseconds.
    Bigguery format: 'YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]'
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#canonical-format_3

    Args:
        py_datetime (datetime.datetime):  The date to convert.
    Returns:
        (str): The Serialized date.
    """
    return py_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')


'''
def bigquery_deserialize_datetime(serialized_str):
    return datetime.strptime(serialized_str + '00', '%Y-%m-%d %H:%M:%S.%f')
'''


def bigquery_json_serialize_default(value):
    """
    Serializes value to str in Bigquery's canonical formats.
    Args:
        value (Any):
    Returns:
        (str): Serialized value
    """
    if isinstance(value, datetime.datetime):
        return bigquery_serialize_datetime(value)
    elif isinstance(value, PythonEnum):
        return _json_dump(value.value)
    else:
        raise ValueError('Unrecognized type: {}, value: {}'.format(
            type(value), value
        ))


def _json_dump(obj):
    return json.dumps(
        obj, ensure_ascii=False,
        default=bigquery_json_serialize_default,
        sort_keys=True,
    )
