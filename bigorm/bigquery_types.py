import json
import functools

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.types import UserDefinedType
import shapely.wkt
from shapely.geometry import shape, mapping as shapely_to_geojson
from shapely.geometry.polygon import orient as shapely_orient
from shapely.geometry import Polygon as ShapelyPolygon, MultiPolygon as ShapelyMultiPolygon, LinearRing as ShapelyLinearRing

# As of 2/7/19, BigQuery only supports Geography types.
# https://cloud.google.com/bigquery/docs/gis-data
# https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromgeojson

"""
get_col_spec = SCHEMA Column Name
bind_processor = from you to database on your side
bind_expression = from you to database on the database side
column_expression = from database to you on the database side
result_processor = from database to you on your side
"""
def _orient_polygon(polygon, sign=1.0):
    """
    A sign of 1.0 means that the coordinates of the product
    will be oriented counter-clockwise.

    Args:
        polygon (Union[Polygon, LinearRing]):  The geometry to orient.
    Returns:
        (Union[Polygon, LinearRing]):  The same type is the input, but with the points
            oriented so that the area matches sign.
    """
    if isinstance(polygon, ShapelyLinearRing):
        polygon = ShapelyPolygon(polygon)
        result = _orient_polygon(polygon, sign=sign)
        return ShapelyLinearRing(result.exterior)

    exiertor = shapely_orient(polygon, sign=sign).exterior
    interiors = [_orient_polygon(interior, sign=sign) for interior in polygon.interiors]
    return ShapelyPolygon(shell=exiertor, holes=interiors)


def _geovalue_to_shapely(value, is_wkt=False):
    if is_wkt:
        shape_value = shapely.wkt.loads(value)
    else:
        shape_value = shape(value)
    if not shape_value.is_valid:
        raise ValueError('Invalid geometry:\nShapely: {}\nInput: {}'.format(shape_value, value))
    sign = 1.0

    if isinstance(shape_value, ShapelyPolygon):
        shape_value = _orient_polygon(shape_value, sign=sign)
    elif isinstance(shape_value, ShapelyMultiPolygon):
        shape_value = ShapelyMultiPolygon([
            _orient_polygon(p, sign=sign) for p in shape_value
        ])

    return shape_value


def convert_geovalue_to_geojson_str(value, is_wkt=False):
    """
    Args:
        value (Union[str, Dict[str, Any]]):  Value must be in WKT format or GEOJSON format.
    Returns:
        (str):  The GEOJSON format of value, with polygons properly oriented and the shape validated, as a string.
    """
    shape_value = _geovalue_to_shapely(value, is_wkt=is_wkt)
    geojson = shapely_to_geojson(shape_value)
    keys = list(geojson.keys())
    if len(keys) != 2 or ('type' not in keys) or ('coordinates' not in keys):
        raise ValueError(
            'BigQuery only understands geojsons with "type" and '
            '"coordinates" properties.  Found keys: {}'.format(keys)
        )

    return json.dumps(geojson)


class GeographyWKT(UserDefinedType):
    """
    Expects things as POINT(x, y) or POLYGON((0 0,1 0,1 1,0 1,0 0))
    """
    def get_col_spec(self):
        return "GEOGRAPHY"

    def bind_processor(self, dialect):
        return functools.partial(convert_geovalue_to_geojson_str, is_wkt=True)

    def bind_expression(self, bindvalue):
        return func.ST_GeogFromGeoJSON(bindvalue, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)


class _GeoJsonGeometryFormat(dict):

    @staticmethod
    def __recusrive_list_to_tuple(val):
        if isinstance(val, list) or isinstance(val, tuple):
            return tuple(
                _GeoJsonGeometryFormat.__recusrive_list_to_tuple(sub_val)
                for sub_val in val
            )
        return val

    def __key(self):
        return (
            self['type'],
            _GeoJsonGeometryFormat.__recusrive_list_to_tuple(self['coordinates'])
        )

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, _GeoJsonGeometryFormat):
            return self.__key() == other.__key()
        return (
            isinstance(other, dict)
            and super(
                _GeoJsonGeometryFormat, self
            ).__eq__(other)
        )


class GeographyGeoJson(UserDefinedType):
    """
    Expects things as {"type": "Point","coordinates": [-10.986328125, 27.049341619870376]}
    """

    def get_col_spec(self):
        return "GEOGRAPHY"

    def bind_processor(self, dialect):
        return convert_geovalue_to_geojson_str

    def bind_expression(self, bindvalue):
        return func.ST_GeogFromGeoJSON(bindvalue, type_=self)

    def column_expression(self, col):
        return func.ST_AsGeoJSON(col, type_=self)

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            value = json.loads(value)
            if value is None:
                return None
            return _GeoJsonGeometryFormat(
                value
            )
        return process


class Enum(sqlalchemy.types.Enum):

    def __init__(self, *enums, **kw):
        kw['create_constraint'] = False
        super(Enum, self).__init__(*enums, **kw)
