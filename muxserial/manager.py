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
from .presence import wait_present, wait_recycle  # noqa: F401  (re-export)

DEFAULT_LINGER = 1.0        # ephemeral, in-process only
BOUND_LINGER = 60.0         # explicitly bound, other processes may join
LOST_GRACE = 1.0            # ignore this much enumeration flapping

POLL_IDLE = 0.5             # reaper's resting granularity
POLL_BUSY = 0.02            # granularity right after a release
BUSY_ROUNDS = 25            # how long to stay at POLL_BUSY (~0.5s)

_lock = threading.RLock()
_instances = {}             # device -> _Instance
_closing = {}               # device -> _Instance, handle not yet given back
_reaper = None
_verbose = False
# 2026-09-07: Immediately notify the reaper thread when a release is requested.
# With plain sleep(0.5), it takes up to 0.5s + linger after refs reaches 0
# until the physical port is actually released. In situations where physical
# exclusivity is required (e.g., handing over to an external tool), this delay
# directly causes a failure.
_wake = threading.Event()


class _Instance:
  def __init__(self, device, server, linger):
    self.device = device
    self.server = server
    self.linger = linger
    self.refs = 0
    self.idle_since = time.monotonic()
    self.lost = False
    self.missing_since = None
    self.ident = None

  @property
  def endpoint(self):
    return self.server.host, self.server.port

  def reapable(self, now):
    if self.refs > 0 or self.server.client_count() > 0:
      return False
    if self.lost:
      return True          # nobody left and the device is gone: no reason to wait
    if self.linger <= 0:
      return True
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
  # May slightly overlap with the cleanup of a broker that used the same port
  # just before. Falling back to an ephemeral port here prevents other processes
  # from finding us, so retry briefly.
  last = None
  for attempt in range(6):
    try:
      return Server(None, host='127.0.0.1', port=want, verbose=_verbose)
    except OSError as e:
      last = e
      time.sleep(0.05)
  _log('port %d unavailable (%s); using an ephemeral one' % (want, last))
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

  # If in the middle of closing, wait until it finishes before deciding.
  deadline = time.monotonic() + 2.0
  while time.monotonic() < deadline:
    with _lock:
      if device not in _closing:
        break
    time.sleep(0.005)

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
      inst.ident = presence.identity(device)
      _instances[device] = inst
      registry.publish(device, *inst.endpoint)
      _log('spawned', device, '->', '%s:%d' % inst.endpoint)
      _start_reaper()
    inst.refs += 1
    return inst.endpoint


def set_linger(seconds, device=None):
  """How long a broker keeps the physical port after its last client leaves.

  0 means 'let go as soon as nobody is attached'. Use that when another program
  has to take the device over; use the default when ports are opened and closed
  repeatedly and re-acquiring the handle would toggle DTR (and reset the board).
  """
  global DEFAULT_LINGER
  if device is None:
    DEFAULT_LINGER = float(seconds)
    return
  with _lock:
    inst = _instances.get(_key(device))
    if inst is not None:
      inst.linger = float(seconds)
  _wake.set()


def release(device, immediate=False):
  """Drop one reference. With immediate=True the port is handed back at once."""
  device = _key(device)
  with _lock:
    inst = _instances.get(device)
    if inst is None:
      # Not ours: we were joined to somebody else's broker. Nothing to release,
      # but a waiting reaper elsewhere may care about the socket going away.
      return
    inst.refs = max(0, inst.refs - 1)
    inst.idle_since = time.monotonic()
    if immediate:
      inst.linger = 0.0
    idle = inst.refs == 0
  if idle:
    _wake.set()


def shutdown(device):
  device = _key(device)
  with _lock:
    inst = _instances.pop(device, None)
    if inst is not None:
      _closing[device] = inst
  if inst is not None:
    registry.unpublish(device)
    try:
      inst.server.shutdown()
    finally:
      with _lock:
        if _closing.get(device) is inst:
          del _closing[device]
    _log('reaped', device)


def shutdown_all():
  with _lock:
    devices = list(_instances)
  for d in devices:
    shutdown(d)
  deadline = time.monotonic() + 2.0
  while time.monotonic() < deadline:
    with _lock:
      if not _closing:
        return
    time.sleep(0.005)


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


def is_local(device):
  """True while this process still holds the OS handle for the device.

  Deliberately cheap, unlike owner_of(): no socket probe, so it can be polled.
  Stays true until the teardown has actually finished.
  """
  key = _key(device)
  with _lock:
    return key in _instances or key in _closing


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
          inst.ident = presence.identity(device)
          _log('device back', device, err or 'ok')
        else:
          inst.lost = True
        continue

      # 2026-09-07: The hardware device may have been swapped even if the name is identical.
      # Native USB boards disappear from USB and re-appear upon reset after flashing,
      # and Windows re-assigns the same COM number. If disappearance and return is shorter
      # than LOST_GRACE, it appears as if it was "always present", letting requests pass through
      # while holding a handle to the stale instance -- leading to the state where open succeeds
      # but nothing flows through.
      now_ident = presence.identity(device)
      if now_ident is not None and inst.ident is not None \
         and now_ident != inst.ident:
        _log('device replaced', device)
        inst.ident = now_ident
        if inst.server.client_count() > 0:
          inst.server.broker.recycle()
        else:
          # If nobody is attached, there is no need to re-acquire. The next open starts clean.
          inst.linger = 0.0
          _wake.set()
      elif inst.ident is None:
        inst.ident = now_ident


def notify_device_change():
  """Wire this to the application's DeviceChangeFilter callbacks.

  Deliberately cheap: it only marks the cache stale. The reaper thread notices
  and does the enumeration, so nothing heavy runs inside the OS notification.
  """
  presence.notify_change()


def release_device(device, timeout=2.0):
  """Hand the physical port back now, and wait until it is actually gone.

  For the cases this package cannot mediate: an external flasher, a 1200bps
  touch, anything that must own the OS handle. Returns True once released.
  Clients still attached are disconnected, so only call this when that is the
  intent; SerialPort.detach() is the polite version.
  """
  shutdown(device)
  deadline = time.monotonic() + timeout
  key = _key(device)
  while time.monotonic() < deadline:
    with _lock:
      if key not in _instances:
        return True
    time.sleep(0.01)
  with _lock:
    return key not in _instances


def _reap_once():
  now = time.monotonic()
  with _lock:
    dead = []
    for d in list(_instances):
      if _instances[d].reapable(now):
        # 2026-09-07: If marked as "released" immediately upon removing from _instances,
        # the caller might proceed to the next step while the physical handle is still held.
        # Keep it in _closing until shutdown completes so it remains visible to is_local().
        dead.append((d, _instances.pop(d)))
        _closing[d] = dead[-1][1]
    empty = not _instances
  for d, inst in dead:
    registry.unpublish(d)
    try:
      inst.server.shutdown()
    finally:
      with _lock:
        if _closing.get(d) is inst:
          del _closing[d]
    _log('reaped', d)
  with _lock:
    return empty and not dead and not _closing


def _start_reaper():
  global _reaper
  if _reaper is not None and _reaper.is_alive():
    return

  def loop():
    busy = 0
    while True:
      # Wake up immediately on release request. However, at the time release() is called,
      # the broker side might not have processed the socket EOF yet and client_count()
      # may not have decreased, so monitor at a fine interval for a while after being woken up.
      interval = POLL_BUSY if busy > 0 else POLL_IDLE
      if _wake.wait(interval):
        _wake.clear()
        busy = BUSY_ROUNDS
      elif busy > 0:
        busy -= 1

      try:
        dirty = presence.take_dirty()
        with _lock:
          # Nothing physical to track: do not enumerate at all.
          need = any(not presence.is_virtual(d) for d in _instances)
        # Enumeration is heavy, so do not perform it every cycle while looping frequently.
        if need and (busy == 0 or dirty):
          _check_presence(force=dirty)
      except Exception:
        pass

      try:
        if _reap_once():
          return
      except Exception:
        pass

  _reaper = threading.Thread(target=loop, daemon=True)
  _reaper.start()


atexit.register(shutdown_all)
