# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: (C) 2026 mukyokyo

"""Physical device presence.

The broker owns the OS handle, so clients cannot tell by themselves whether the
underlying device still exists. Windows in particular keeps a handle nominally
open long after the USB device is gone, which is how a runner ended up
reconnecting to a port that no longer existed.

Everything here is deliberately free of Qt: the application can push
notifications in through notify_change() (wire it to DeviceChangeFilter), and
otherwise a short-lived cache keeps the polling cost down.
"""

import re
import threading
import time

_CACHE_TTL = 0.5

# One global lock for all port enumeration. On Windows list_ports goes through
# SetupAPI via ctypes, and calling it from two threads at once is not safe.
# The application's own port-list refresh should go through raw_comports() so
# that every enumeration in the process is serialized by this lock.
_enum_lock = threading.RLock()

_lock = threading.RLock()
_cache = None
_cache_at = 0.0
_listeners = []
_dirty = threading.Event()

_COM = re.compile(r'(COM\d+)', re.IGNORECASE)


def normalize(name):
  """'mux://COM11' / '\\\\.\\COM11' / 'COM11' -> 'COM11'."""
  m = _COM.search(str(name))
  if m:
    return m.group(1).upper()
  return str(name)


def is_virtual(device):
  """loop://, echo:// and friends have no physical existence to check."""
  return '://' in str(device) and not str(device).lower().startswith('mux://')


def raw_comports():
  """serial.tools.list_ports.comports(), serialized process-wide."""
  from serial.tools.list_ports import comports as _real
  with _enum_lock:
    return list(_real())


def list_devices(force=False):
  """Names of the serial ports the OS currently reports."""
  global _cache, _cache_at
  with _lock:
    now = time.monotonic()
    if not force and _cache is not None and (now - _cache_at) < _CACHE_TTL:
      return _cache
  try:
    names = [normalize(p.device) for p in raw_comports()]
  except Exception:
    # If enumeration fails we must not conclude the device is gone.
    with _lock:
      return _cache if _cache is not None else None
  with _lock:
    _cache = names
    _cache_at = time.monotonic()
    return names


def present(device, force=False):
  """True when the device exists, or when we genuinely cannot tell."""
  if is_virtual(device):
    return True
  names = list_devices(force=force)
  if names is None:
    return True
  return normalize(device) in names


def invalidate():
  global _cache_at
  with _lock:
    _cache_at = 0.0


def notify_change():
  """Call from the application's device add/remove callbacks.

  This runs inside the WM_DEVICECHANGE handler on Windows, so it must not
  enumerate anything here: doing SetupAPI work while the system is delivering
  a device notification is a re-entrancy hazard. Just mark the cache stale and
  let the manager's own thread do the work.
  """
  invalidate()
  _dirty.set()


def take_dirty():
  """True once after each notify_change(); consumed by the manager thread."""
  if _dirty.is_set():
    _dirty.clear()
    return True
  return False


def add_listener(fn):
  with _lock:
    if fn not in _listeners:
      _listeners.append(fn)


def remove_listener(fn):
  with _lock:
    if fn in _listeners:
      _listeners.remove(fn)


def comports():
  """serial.tools.list_ports.comports() with mux:// device names.

  Lets a runner populate its port combo with entries that can actually be
  opened through the multiplexer, instead of hand-written URLs. Also keeps the
  application's enumeration under the same lock as ours.
  """
  out = []
  for p in raw_comports():
    p.device = 'mux://%s' % normalize(p.device)
    out.append(p)
  return out
