# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: (C) 2026 mukyokyo

"""On-demand broker lifecycle plus physical-device tracking.

A broker is created the first time somebody opens a given device, and it
disappears again once the last client has gone and the linger has expired.

The broker also follows the device's real existence: when the USB cable is
pulled it drops the handle and tells every client, and refuses to hand out new
connections until the device comes back. Without this the broker's own handle
made a vanished port look connectable.
"""

import atexit
import threading
import time

from .core import Server
from . import presence, registry

DEFAULT_LINGER = 5.0        # ephemeral, in-process only
BOUND_LINGER = 60.0         # explicitly bound, other processes may join
LOST_GRACE = 1.0            # ignore this much enumeration flapping

_lock = threading.RLock()
_instances = {}             # device -> _Instance
_reaper = None
_verbose = False


class _Instance:
  def __init__(self, device, server, linger):
    self.device = device
    self.server = server
    self.linger = linger
    self.refs = 0
    self.idle_since = time.monotonic()
    self.lost = False
    self.missing_since = None

  @property
  def endpoint(self):
    return self.server.host, self.server.port

  def reapable(self, now):
    if self.refs > 0 or self.server.client_count() > 0:
      return False
    if self.lost:
      return True          # nobody left and the device is gone: no reason to wait
    return (now - self.idle_since) >= self.linger


def set_verbose(flag):
  global _verbose
  _verbose = bool(flag)


def _log(*a):
  if _verbose:
    print('[manager]', *a, flush=True)


def _open_physical(device, **kw):
  import serial
  return serial.serial_for_url(device, timeout=0.05, **kw)


def _listen(device, bind):
  """Bind the broker's socket, preferring the device's deterministic port.

  Sitting on a predictable port is what lets another process find us without
  any shared file. If that port is taken by something that is not us, fall back
  to an ephemeral one and rely on the registry file instead.
  """
  if bind:
    return Server(None, host=bind[0], port=bind[1], verbose=_verbose)

  want = registry.default_port(device)
  try:
    return Server(None, host='127.0.0.1', port=want, verbose=_verbose)
  except OSError as e:
    _log('port %d unavailable (%s); using an ephemeral one' % (want, e))
    return Server(None, host='127.0.0.1', port=0, verbose=_verbose)


def _key(device):
  """Dictionary key for a device.

  Must be normalized: 'COM9' and 'com9' name the same hardware, and keying on
  the raw string quietly produces two brokers that then fight over the handle.
  """
  return presence.normalize(device)


def ensure(device, bind=None, **kw):
  """Return (host, port) for a broker owning `device`, creating it if needed.

  Order of preference:
    1. a broker this process already owns
    2. a broker any process advertises (TransferHub joining the GUI's)
    3. a new local broker
  """
  if not presence.present(device, force=True):
    raise IOError("no such port: %s" % device)

  device = _key(device)

  with _lock:
    inst = _instances.get(device)
    if inst is not None and inst.lost:
      # A stale broker for a device that went away; do not hand it out.
      _log('discarding lost instance for', device)
      _instances.pop(device, None)
      registry.unpublish(device)
      inst.server.shutdown()
      inst = None

    if inst is not None:
      inst.refs += 1
      return inst.endpoint

  # Somebody else may already own the handle. Joining beats colliding.
  remote = registry.lookup(device)
  if remote is not None:
    _log('joining remote broker for', device, '->', '%s:%d' % remote)
    return remote

  with _lock:
    inst = _instances.get(device)
    if inst is None:
      ser = _open_physical(device, **kw)
      try:
        server = _listen(device, bind)
      except Exception:
        try:
          ser.close()
        except Exception:
          pass
        raise
      server.attach(ser)
      inst = _Instance(device, server,
                       BOUND_LINGER if bind else DEFAULT_LINGER)
      _instances[device] = inst
      registry.publish(device, *inst.endpoint)
      _log('spawned', device, '->', '%s:%d' % inst.endpoint)
      _start_reaper()
    inst.refs += 1
    return inst.endpoint


def release(device):
  device = _key(device)
  with _lock:
    inst = _instances.get(device)
    if inst is None:
      return
    inst.refs = max(0, inst.refs - 1)
    inst.idle_since = time.monotonic()


def shutdown(device):
  device = _key(device)
  with _lock:
    inst = _instances.pop(device, None)
  if inst is not None:
    registry.unpublish(device)
    inst.server.shutdown()
    _log('reaped', device)


def shutdown_all():
  with _lock:
    devices = list(_instances)
  for d in devices:
    shutdown(d)


def active():
  with _lock:
    return {d: {'endpoint': i.endpoint, 'refs': i.refs,
                'clients': i.server.client_count(), 'lost': i.lost}
            for d, i in _instances.items()}


def is_available(device):
  """False when the device is physically absent, or its broker knows it is."""
  if not presence.present(device):
    return False
  with _lock:
    inst = _instances.get(_key(device))
    return not (inst is not None and inst.lost)


def owner_of(device):
  """'local' / 'remote' / None - who holds the handle for this device."""
  with _lock:
    if _key(device) in _instances:
      return 'local'
  # Cached probe: the registry file may be invisible to us (different temp
  # directory), so a file-only check would wrongly report 'nobody'.
  return 'remote' if registry.lookup(device) else None


# ---------------------------------------------------------------- presence

def _check_presence(force=False):
  """Reconcile every live broker against what the OS reports."""
  now = time.monotonic()
  with _lock:
    items = list(_instances.items())

  for device, inst in items:
    if presence.is_virtual(device):
      continue
    here = presence.present(device, force=force)

    if not here:
      if inst.missing_since is None:
        inst.missing_since = now
        continue
      if (now - inst.missing_since) < LOST_GRACE:
        continue
      if not inst.lost:
        inst.lost = True
        inst.idle_since = now
        inst.server.broker.mark_lost()
        _log('device lost', device)
    else:
      inst.missing_since = None
      if inst.lost:
        # Only meaningful while somebody is still attached; otherwise the
        # reaper will drop the instance and the next open() starts clean.
        if inst.server.client_count() > 0:
          err = inst.server.broker.mark_found()
          _log('device back', device, err or 'ok')
        else:
          inst.lost = True


def notify_device_change():
  """Wire this to the application's DeviceChangeFilter callbacks.

  Deliberately cheap: it only marks the cache stale. The reaper thread notices
  and does the enumeration, so nothing heavy runs inside the OS notification.
  """
  presence.notify_change()


def _start_reaper():
  global _reaper
  if _reaper is not None and _reaper.is_alive():
    return

  def loop():
    while True:
      time.sleep(0.5)
      try:
        dirty = presence.take_dirty()
        with _lock:
          # Nothing physical to track: do not enumerate at all.
          need = any(not presence.is_virtual(d) for d in _instances)
        if need:
          _check_presence(force=dirty)
      except Exception:
        pass
      now = time.monotonic()
      with _lock:
        dead = []
        for d in list(_instances):
          if _instances[d].reapable(now):
            dead.append((d, _instances.pop(d)))
        empty = not _instances
      for d, inst in dead:
        registry.unpublish(d)
        inst.server.shutdown()
        _log('reaped', d)
      if empty and not dead:
        return

  _reaper = threading.Thread(target=loop, daemon=True)
  _reaper.start()


atexit.register(shutdown_all)
