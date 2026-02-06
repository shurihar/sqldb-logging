"""
An extension to the Python logging library that allows logging to SQL databases using SQLAlchemy.
"""

from .handlers import SQLHandler

__all__ = ['SQLHandler']
