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
_ident = {}                 # 'COM9' -> hardware identity string
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


def _identity_of(info):
  """A string that changes when the hardware behind a port name changes.

  A native-USB board (Arduino UNO R4, RP2040, anything with a CDC in the MCU)
  disappears and re-enumerates when it resets after a firmware upload. Windows
  usually hands the new device the same COM number, so watching the name alone
  cannot tell that the handle we are holding now refers to something that no
  longer exists: opening keeps working and nothing ever arrives.
  """
  parts = []
  for attr in ('hwid', 'serial_number', 'location', 'vid', 'pid'):
    v = getattr(info, attr, None)
    if v not in (None, ''):
      parts.append('%s=%s' % (attr, v))
  return '|'.join(parts) if parts else None


def list_devices(force=False):
  """Names of the serial ports the OS currently reports."""
  global _cache, _cache_at
  with _lock:
    now = time.monotonic()
    if not force and _cache is not None and (now - _cache_at) < _CACHE_TTL:
      return _cache
  try:
    infos = raw_comports()
  except Exception:
    # If enumeration fails we must not conclude the device is gone.
    with _lock:
      return _cache if _cache is not None else None
  names = [normalize(p.device) for p in infos]
  idents = {normalize(p.device): _identity_of(p) for p in infos}
  with _lock:
    _cache = names
    _ident.clear()
    _ident.update(idents)
    _cache_at = time.monotonic()
    return names


def identity(device, force=False):
  """Hardware identity of the port, or None when unknown/absent."""
  list_devices(force=force)
  with _lock:
    return _ident.get(normalize(device))


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


def wait_present(device, timeout=15.0, settle=0.3, poll=0.05):
  """Block until the port exists and has stayed there for `settle` seconds.

  A board that resets after an upload can enumerate more than once (bootloader,
  then the sketch), so the first appearance is not necessarily the final one.
  Returns the hardware identity, or None on timeout.
  """
  deadline = time.monotonic() + timeout
  stable_since = None
  while time.monotonic() < deadline:
    if present(device, force=True):
      if stable_since is None:
        stable_since = time.monotonic()
      elif (time.monotonic() - stable_since) >= settle:
        return identity(device)
    else:
      stable_since = None
    time.sleep(poll)
  return None


def wait_recycle(device, timeout=20.0, gone_timeout=5.0, settle=0.5):
  """Wait for the device to disappear and come back.

  Use after handing the port to an external flasher: the board resets, so the
  port that returns is a different device even when the name is unchanged.
  Returns True if a full cycle was observed (or the port simply became stable).
  """
  deadline = time.monotonic() + gone_timeout
  saw_gone = False
  while time.monotonic() < deadline:
    if not present(device, force=True):
      saw_gone = True
      break
    time.sleep(0.05)
  remaining = timeout if saw_gone else min(timeout, 3.0)
  return wait_present(device, timeout=remaining, settle=settle) is not None
