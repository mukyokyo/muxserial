# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: (C) 2026 mukyokyo

"""Cross-process broker discovery.

The in-process manager only knows about brokers this interpreter created.
TransferHub runs as its own process, so it needs a way to notice that the GUI
already owns COM11 and join that broker instead of fighting for the handle.

A broker publishes a small file per device; a would-be client reads it and
verifies the endpoint by actually connecting. Liveness is decided by the socket,
not by a pid, because a pid can be recycled and a socket cannot lie.
"""

import json
import os
import socket
import tempfile
import time
import zlib

from . import presence
from .core import HDR, MAGIC, T_EVENT, FrameReader

DIR_NAME = 'muxserial'
_LOOKUP_TTL = 1.0

# Deterministic port range. Discovery must not depend on a shared file: a
# process started from an MSYS2 shell and one started from Explorer disagree
# about where the temp directory is, so the registry file alone silently fails
# to connect the two. A port derived from the device name needs no shared state
# at all, and the handshake below proves who is actually listening there.
# Kept clear of the ephemeral ranges of both platforms (Linux 32768-60999,
# Windows 49152-65535), so an unrelated outgoing connection cannot be sitting
# on the port we want to listen on.
PORT_BASE = 28000
PORT_SPAN = 2000

_lookup_cache = {}      # device -> (expires_at, endpoint or None)


def default_port(device):
  """The port a broker for this device prefers to listen on."""
  key = presence.normalize(device).encode('utf-8', 'replace')
  return PORT_BASE + (zlib.crc32(key) % PORT_SPAN)


def _base_dir():
  # Prefer a Windows-native location so that MSYS2 and native processes agree.
  for var in ('LOCALAPPDATA', 'XDG_RUNTIME_DIR'):
    v = os.environ.get(var)
    if v and os.path.isdir(v):
      return v
  return tempfile.gettempdir()


def _dir():
  d = os.path.join(_base_dir(), DIR_NAME)
  try:
    os.makedirs(d, exist_ok=True)
  except OSError:
    pass
  return d


def _path(device):
  safe = presence.normalize(device)
  safe = ''.join(c if c.isalnum() or c in '-_.' else '_' for c in safe)
  return os.path.join(_dir(), '%s.json' % safe)


def publish(device, host, port):
  invalidate(device)
  rec = {'device': presence.normalize(device), 'host': host, 'port': int(port),
         'pid': os.getpid(), 'ts': time.time()}
  try:
    path = _path(device)
    tmp = path + '.%d.tmp' % os.getpid()
    with open(tmp, 'w', encoding='utf-8') as f:
      json.dump(rec, f)
    os.replace(tmp, path)
  except OSError:
    pass


def invalidate(device=None):
  if device is None:
    _lookup_cache.clear()
  else:
    _lookup_cache.pop(presence.normalize(device), None)


def unpublish(device):
  try:
    rec = _read(device)
    if rec and rec.get('pid') == os.getpid():
      os.remove(_path(device))
  except OSError:
    pass


def _read(device):
  try:
    with open(_path(device), encoding='utf-8') as f:
      return json.load(f)
  except (OSError, ValueError):
    return None


def _reachable(host, port, timeout=0.3):
  return probe(host, port, None, timeout=timeout) is not None


def probe(host, port, device=None, timeout=0.4):
  """Connect and confirm a muxserial broker is listening.

  A broker greets every client with a 'ready' event naming the device it owns,
  so one short connection settles both 'is this ours?' and 'is it the right
  port?'. Returns the device name it reported, or None.
  """
  try:
    sock = socket.create_connection((host, int(port)), timeout=timeout)
  except (OSError, ValueError, TypeError):
    return None
  try:
    sock.settimeout(timeout)
    fr = FrameReader()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
      try:
        chunk = sock.recv(4096)
      except OSError:
        return None
      if not chunk:
        return None
      try:
        frames = fr.feed(chunk)
      except ValueError:
        return None          # not our protocol; somebody else owns this port
      for t, payload in frames:
        if t != T_EVENT:
          continue
        try:
          msg = json.loads(payload.decode('utf-8'))
        except ValueError:
          return None
        if msg.get('ev') != 'ready':
          continue
        found = presence.normalize(msg.get('port', ''))
        if device is not None and found != presence.normalize(device):
          return None        # a broker, but for a different device
        return found
    return None
  finally:
    try:
      sock.close()
    except OSError:
      pass


def lookup(device, verify=True):
  """-> (host, port) of a live broker owned by any process, or None.

  Verification costs a TCP connect, which the broker sees as a client joining
  and leaving. Results are cached briefly so that repeated queries (a port
  combo refresh, say) do not churn the broker's client list.
  """
  key = presence.normalize(device)
  if verify:
    hit = _lookup_cache.get(key)
    if hit is not None and hit[0] > time.monotonic():
      return hit[1]

  rec = _read(device)
  if not rec:
    # No usable record. Try the address a broker for this device would have
    # chosen on its own; this is what makes discovery survive processes that
    # disagree about the temp directory.
    ep = ('127.0.0.1', default_port(device))
    if verify and probe(ep[0], ep[1], device) is not None:
      _lookup_cache[key] = (time.monotonic() + _LOOKUP_TTL, ep)
      return ep
    if verify:
      _lookup_cache[key] = (time.monotonic() + _LOOKUP_TTL, None)
    return None
  # Note: a record written by this same process is still usable. The local
  # manager is consulted first, so reaching here means it had no entry -- which
  # happens if the package got imported twice under different paths. Joining
  # over the loopback socket works regardless of which copy owns the broker.
  host, port = rec.get('host'), rec.get('port')
  if not host or not port:
    return None
  if verify:
    ok = probe(host, port, device) is not None
    _lookup_cache[key] = (time.monotonic() + _LOOKUP_TTL,
                          (host, int(port)) if ok else None)
    if not ok:
      try:
        os.remove(_path(device))
      except OSError:
        pass
      return None
  return host, int(port)


def known_devices(scan=False):
  """Devices any process currently advertises a broker for.

  With scan=True every port a known serial device would use is probed, which
  finds brokers whose registry file we cannot see.
  """
  out = {}
  try:
    names = os.listdir(_dir())
  except OSError:
    names = []
  for n in names:
    if not n.endswith('.json'):
      continue
    rec = _read(n[:-5])
    if rec and probe(rec.get('host'), rec.get('port'), rec.get('device')):
      out[rec['device']] = (rec['host'], int(rec['port']))

  if scan:
    try:
      devices = [presence.normalize(p.device) for p in presence.raw_comports()]
    except Exception:
      devices = []
    for dev in devices:
      if dev in out:
        continue
      ep = ('127.0.0.1', default_port(dev))
      if probe(ep[0], ep[1], dev):
        out[dev] = ep
  return out
