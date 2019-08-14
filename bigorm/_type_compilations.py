"""
Compilation Rules

loaded in __init__.py
"""
from sqlalchemy import Unicode, UnicodeText, Enum
from sqlalchemy.ext.compiler import compiles


@compiles(Unicode, "bigquery")
def compile_text(element, compiler, **kw):
    # As opposed to visit_unicode
    return compiler.visit_string(element, **kw)


@compiles(UnicodeText, "bigquery")
def compile_unicode_text(element, compiler, **kw):
    # As opposed to visit_unicode_text
    return compiler.visit_text(element, **kw)


@compiles(Enum, "bigquery")
def compile_enum(element, compiler, **kw):
    # As opposed to visit_enum -> visit_VARCHAR
    return compiler.visit_string(element, **kw)
