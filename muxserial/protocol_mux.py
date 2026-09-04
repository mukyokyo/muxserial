# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: (C) 2026 mukyokyo

"""
pyserial URL handler:  mux://...

  mux://COM11                  device name -> local broker, spawned on demand
  mux://COM11@127.0.0.1:5555   same, but bound so other processes can join
  mux://127.0.0.1:5555         join a broker that already exists

Everything an application already does with pyserial keeps working. The only
additions are lease() / detach() / reattach(), and they are optional.
"""

import contextlib
import json
import socket
import threading
import time

from serial.serialutil import (SerialBase, SerialException, PortNotOpenError,
                               to_bytes)

from .core import T_DATA, T_CTRL, T_EVENT, FrameReader, pack, pack_ctrl
from . import manager, presence


def parse_target(spec):
  """-> (device, host, port). device is None when joining an existing broker."""
  if '@' in spec:
    device, _, bind = spec.partition('@')
    host, _, port = bind.rpartition(':')
    return device, (host or '127.0.0.1'), int(port)
  host, sep, port = spec.rpartition(':')
  if sep and host and port.isdigit():
    return None, host, int(port)
  return spec, '127.0.0.1', 0


class Serial(SerialBase):
  BAUDRATES = ()

  # -- lifecycle -------------------------------------------------------------

  def open(self):
    if self._port is None:
      raise SerialException('port must be configured before use')
    if self.is_open:
      raise SerialException('port already open')

    spec = self.portstr.split('://', 1)[-1]
    device, host, port = parse_target(spec)
    self._device = device

    if device is not None:
      bind = (host, port) if port else None
      try:
        host, port = manager.ensure(device, bind=bind, baudrate=self._baudrate)
      except IOError as e:
        # Mirror the native behaviour: a vanished port must not be openable
        # just because a broker happens to be holding a stale handle.
        raise SerialException(
          "could not open port '%s': %s" % (device, e))

    self._rx = bytearray()
    self._cv = threading.Condition()
    self._err = None
    self._modem = {'cts': False, 'dsr': False, 'ri': False, 'cd': False}
    self._events = []
    self._evcv = threading.Condition()
    self._wlock = threading.Lock()
    self._suspended = False
    self._lost = False

    try:
      self._sock = socket.create_connection((host, int(port)), timeout=5.0)
    except OSError as e:
      if device is not None:
        manager.release(device)
      raise SerialException('cannot connect to %s: %s' % (self.portstr, e))
    self._sock.settimeout(None)
    self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    self.is_open = True
    self._thread = threading.Thread(target=self._reader, daemon=True)
    self._thread.start()
    # the broker registers the client asynchronously; wait for it, otherwise
    # data arriving right after open() would be missed
    try:
      self._wait_event(('ready',), 5.0)
    except SerialException:
      self.close()
      raise
    self._reconfigure_port()

  def close(self):
    if not self.is_open:
      return
    self.is_open = False
    try:
      self._sock.shutdown(socket.SHUT_RDWR)
    except OSError:
      pass
    try:
      self._sock.close()
    except OSError:
      pass
    with self._cv:
      self._cv.notify_all()
    with self._evcv:
      self._evcv.notify_all()
    if getattr(self, '_device', None) is not None:
      manager.release(self._device)

  @property
  def device(self):
    """The underlying port name, e.g. 'COM11'. None when joining by address."""
    return getattr(self, '_device', None)

  # -- receive path ----------------------------------------------------------

  def _reader(self):
    fr = FrameReader()
    while self.is_open:
      try:
        chunk = self._sock.recv(65536)
      except OSError as e:
        if self.is_open:
          self._fail(str(e))
        return
      if not chunk:
        if self.is_open:
          self._fail('connection closed by broker')
        return
      try:
        frames = fr.feed(chunk)
      except ValueError as e:
        self._fail(str(e))
        return
      for t, payload in frames:
        if t == T_DATA:
          with self._cv:
            self._rx += payload
            self._cv.notify_all()
        elif t == T_EVENT:
          self._on_event(json.loads(payload.decode('utf-8')))

  def _on_event(self, msg):
    ev = msg.get('ev')
    if ev == 'modem':
      self._modem.update({k: msg[k] for k in ('cts', 'dsr', 'ri', 'cd')
                          if k in msg})
    elif ev == 'suspended':
      self._suspended = True
    elif ev == 'resumed':
      self._suspended = False
    elif ev == 'device_lost':
      self._lost = True
      # Wake read() with an exception so the runner tears the session down
      # exactly as it would on a native unplug.
      self._fail('device removed: %s' % msg.get('msg', ''))
      return
    elif ev == 'device_found':
      self._lost = False
      if msg.get('error'):
        self._fail('device reopen failed: %s' % msg['error'])
        return
    elif ev == 'error':
      self._fail(msg.get('msg', 'broker error'))
      return
    with self._evcv:
      self._events.append(msg)
      self._evcv.notify_all()

  def _fail(self, msg):
    with self._cv:
      self._err = msg
      self._cv.notify_all()
    with self._evcv:
      self._evcv.notify_all()

  def _wait_event(self, names, timeout=10.0):
    deadline = time.monotonic() + timeout
    with self._evcv:
      while True:
        for i, m in enumerate(self._events):
          if m.get('ev') in names:
            return self._events.pop(i)
        if self._err:
          raise SerialException(self._err)
        if not self._evcv.wait(max(0.0, deadline - time.monotonic())):
          raise SerialException('timeout waiting for %s' % (names,))

  # -- pyserial surface ------------------------------------------------------

  @property
  def in_waiting(self):
    if not self.is_open:
      raise PortNotOpenError()
    with self._cv:
      return len(self._rx)

  def read(self, size=1):
    if not self.is_open:
      raise PortNotOpenError()
    deadline = None if self._timeout is None else time.monotonic() + self._timeout
    with self._cv:
      while len(self._rx) < size:
        if self._err and not self._rx:
          raise SerialException(self._err)
        if self._err:
          break
        if deadline is None:
          self._cv.wait(0.5)
          if not self.is_open:
            break
        else:
          remain = deadline - time.monotonic()
          if remain <= 0:
            break
          self._cv.wait(remain)
      out = bytes(self._rx[:size])
      del self._rx[:size]
    return out

  def write(self, data):
    if not self.is_open:
      raise PortNotOpenError()
    data = to_bytes(data)
    self._send(pack(T_DATA, data))
    return len(data)

  def _send(self, frame):
    try:
      with self._wlock:
        self._sock.sendall(frame)
    except OSError as e:
      raise SerialException('write failed: %s' % e)

  def _ctrl(self, op, **kw):
    if self.is_open:
      self._send(pack_ctrl(op, **kw))

  def reset_input_buffer(self):
    """Per-client only. Does not disturb what other clients have pending."""
    if not self.is_open:
      raise PortNotOpenError()
    with self._cv:
      del self._rx[:]

  def reset_output_buffer(self):
    self._ctrl('purge_tx')

  def _reconfigure_port(self):
    self._ctrl('config',
               baudrate=self._baudrate, bytesize=self._bytesize,
               parity=self._parity, stopbits=self._stopbits,
               xonxoff=self._xonxoff, rtscts=self._rtscts, dsrdtr=self._dsrdtr)

  def _update_dtr_state(self):
    self._ctrl('dtr', v=bool(self._dtr_state))

  def _update_rts_state(self):
    self._ctrl('rts', v=bool(self._rts_state))

  def _update_break_state(self):
    self._ctrl('break', v=bool(self._break_state))

  @property
  def cts(self):
    return self._modem['cts']

  @property
  def dsr(self):
    return self._modem['dsr']

  @property
  def ri(self):
    return self._modem['ri']

  @property
  def cd(self):
    return self._modem['cd']

  # -- extensions (optional; plain pyserial code never touches these) --------

  @contextlib.contextmanager
  def lease(self, rx='tee', timeout=10.0):
    """Exclusive multi-write transaction.

    rx='own' -> the response is delivered only to this client (raw REPL)
    rx='tee' -> other clients keep seeing it read-only (xmodem, stk500)
    """
    self.reset_input_buffer()
    self._ctrl('lease', acquire=True, rx=rx, timeout=timeout)
    ev = self._wait_event(('lease_granted', 'lease_denied'), timeout + 1.0)
    if ev['ev'] == 'lease_denied':
      raise SerialException('lease denied')
    try:
      yield self
    finally:
      time.sleep(0.05)
      self.reset_input_buffer()
      self._ctrl('lease', acquire=False)

  def detach(self, timeout=10.0):
    """Make the broker release the OS handle; returns the real port name."""
    self._ctrl('detach')
    ev = self._wait_event(('detached', 'detach_denied'), timeout)
    if ev['ev'] == 'detach_denied':
      raise SerialException('detach denied')
    return ev['port']

  def reattach(self, timeout=10.0):
    self._ctrl('reattach')
    ev = self._wait_event(('resumed',), timeout)
    if ev.get('error'):
      raise SerialException('reattach failed: %s' % ev['error'])

  @property
  def suspended(self):
    return self._suspended

  @property
  def device_present(self):
    """False once the broker has told us the physical device went away."""
    if getattr(self, '_lost', False):
      return False
    dev = getattr(self, '_device', None)
    return True if dev is None else presence.present(dev)
