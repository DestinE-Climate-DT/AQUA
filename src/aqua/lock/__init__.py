"""Wrapper for filellock.SoftFileLock with heartbeat mechanism and stale lock removal"""
from .safelock import SafeFileLock

__all__ = ['SafeFileLock']
