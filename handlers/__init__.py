"""
Handlers package for Camunda 8 Port Operations Worker.
Contains all task handler functions for the workflow.
Updated for 4-table schema: process_instance, container, storage, transport_mean
"""

from .crane_operations import handle_crane_loading, handle_crane_unloading
from .weighing_operations import handle_weighing
from .storage_operations import handle_storage

__all__ = [
    'handle_crane_loading',
    'handle_crane_unloading',
    'handle_weighing',
    'handle_storage'
]

__version__ = '3.0.0'
__author__ = 'Port Operations Team'