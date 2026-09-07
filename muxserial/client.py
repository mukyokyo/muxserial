# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: (C) 2026 mukyokyo

"""A port object for client applications.

TransferHub and friends must run standalone as well as alongside the GUI, and
should not have to care which situation they are in. SerialPort gives them one
object that behaves the same either way:

    from muxserial.client import SerialPort

    port = SerialPort('COM11', do_not_open=True)   # or 'mux://COM11', or a URL
    port.baudrate = 115200
    port.open()

    with port.lease(rx='tee'):        # exclusive on mux, no-op when native
        port.write(b'...')

    real = port.detach()              # 'COM11' - hand the device to esptool
    ...
    port.reattach()

Every other attribute is forwarded to the underlying pyserial object, so
existing code that only touches read/write/dtr/rts/in_waiting needs no changes.
"""

import contextlib

import serial

from . import presence, manager
from .manager import notify_device_change  # noqa: F401  (re-export)

PREFER_MUX = True     # try to share the port even when nobody else has it yet


class SerialPort(object):
  """pyserial-compatible port that transparently shares when it can."""

  def __init__(self, port, *args, prefer_mux=None, **kwargs):
    self._raw_port = port
    self._prefer_mux = PREFER_MUX if prefer_mux is None else prefer_mux
    self._detached_name = None
    self._last_mux_error = None
    self._args = args
    self._kwargs = dict(kwargs)

    do_not_open = self._kwargs.pop('do_not_open', False)
    url, self._is_mux = self._resolve(port, self._prefer_mux)
    self._url = url

    self._ser = serial.serial_for_url(url, *args, do_not_open=True, **self._kwargs)
    if not do_not_open:
      self.open()

  # ---------------------------------------------------------------- routing

  @staticmethod
  def _resolve(port, prefer_mux):
    """-> (url, is_mux). An explicit URL from the caller always wins."""
    text = str(port)
    if '://' in text:
      return text, text.lower().startswith('mux://')
    if not prefer_mux:
      return text, False
    # Physical name. Share it if anybody already owns it, or if we are allowed
    # to be the one who starts sharing.
    return 'mux://%s' % text, True

  @property
  def is_multiplexed(self):
    return self._is_mux

  @property
  def device(self):
    """The underlying physical port name, e.g. 'COM11'."""
    if self._detached_name:
      return self._detached_name
    dev = getattr(self._ser, 'device', None)
    if dev:
      return dev
    return presence.normalize(self._raw_port)

  # ---------------------------------------------------------------- lifecycle

  def open(self):
    try:
      self._ser.open()
      return
    except Exception as mux_err:
      if not self._is_mux:
        raise
      self._last_mux_error = str(mux_err)

    # Sharing was not possible (no broker running, the device is held natively
    # by somebody else, the handler is missing...). Fall back to owning the
    # handle outright rather than failing the task outright.
    native = presence.normalize(self._raw_port)
    self._ser = serial.serial_for_url(native, *self._args,
                                      do_not_open=True, **self._kwargs)
    try:
      self._ser.open()
    except Exception as native_err:
      # Both routes failed. Report both, because the native message alone
      # ("access is denied") hides the fact that sharing was attempted and why.
      raise serial.SerialException(
        "%s\n  shared open failed: %s\n  hint: is the other application "
        "opening this port through mux:// too? (muxserial.active() / "
        "muxserial.registry.known_devices())"
        % (native_err, self._last_mux_error))
    self._url, self._is_mux = native, False

  def close(self, immediate=False):
    """Close the port. immediate=True also releases the physical handle now."""
    try:
      if immediate:
        fn = getattr(self._ser, 'close_now', None)
        if fn is not None:
          return fn()
      self._ser.close()
    except Exception:
      pass
    return True

  @property
  def release_immediate(self):
    return getattr(self._ser, 'release_immediate', True)

  @release_immediate.setter
  def release_immediate(self, flag):
    if hasattr(type(self._ser), 'release_immediate'):
      self._ser.release_immediate = bool(flag)

  def __enter__(self):
    if not self._ser.is_open:
      self.open()
    return self

  def __exit__(self, *exc):
    self.close()
    return False

  def hand_over(self, wait_back=20.0, settle=0.5, reopen=True):
    """Release the port for an external tool, then wait for the board to return.

    For arduino-cli, avrdude, dfu-util and friends: they need the OS handle to
    themselves, and the board resets when they are done. On a native-USB board
    the port that comes back is a different device with the same name, so
    simply reopening at once lands on a handle that never delivers anything.

        real = port.device
        port.hand_over_begin()
        subprocess.run(['arduino-cli', 'upload', '-p', real, ...])
        port.hand_over(wait_back=20)

    Returns True when the port is back and open again.
    """
    dev = self.device
    ok = presence.wait_recycle(dev, timeout=wait_back, settle=settle)
    manager.notify_device_change()
    if not ok:
      return False
    if reopen:
      self._ser = serial.serial_for_url(self._url, *self._args,
                                        do_not_open=True, **self._kwargs)
      self.open()
    return True

  def hand_over_begin(self, wait=2.0):
    """Give the OS handle back and confirm it is gone. Pairs with hand_over()."""
    return self.release_now(wait=wait)

  def release_now(self, wait=2.0):
    """Give the OS handle back and confirm it is gone.

    detach() is the polite form (the broker keeps the session and other clients
    stay attached). This is the blunt one, for when nothing may hold the device.
    """
    if not self._is_mux:
      self.close()
      return True
    dev = self.device
    self.close(immediate=True)
    return manager.release_device(dev, timeout=wait)

  # ---------------------------------------------------------------- extras

  @contextlib.contextmanager
  def lease(self, rx='tee', timeout=30.0):
    """Exclusive multi-write transaction.

    On a multiplexed port this blocks other clients' writes for the duration,
    which is what keeps an XMODEM block or an STK500 exchange from being
    interleaved. On a native port there is nobody to exclude, so it does
    nothing and the calling code stays identical.
    """
    fn = getattr(self._ser, 'lease', None)
    if fn is None:
      yield self
      return
    with fn(rx=rx, timeout=timeout):
      yield self

  def detach(self, timeout=10.0):
    """Give up the OS handle and return the real port name.

    Use this before invoking an external tool that insists on owning the port
    (esptool, dfu-util, a 1200bps touch). Works on native ports too, where it
    simply closes them.
    """
    fn = getattr(self._ser, 'detach', None)
    if fn is not None:
      self._detached_name = fn(timeout=timeout)
      return self._detached_name
    name = presence.normalize(self._ser.port or self._raw_port)
    self._detached_name = name
    self.close()
    return name

  def reattach(self, timeout=10.0):
    fn = getattr(self._ser, 'reattach', None)
    if fn is not None:
      fn(timeout=timeout)
    elif not self._ser.is_open:
      self._ser.open()
    self._detached_name = None

  @property
  def device_present(self):
    v = getattr(self._ser, 'device_present', None)
    if v is not None:
      return v
    return presence.present(self.device)

  @property
  def sharing(self):
    """'local' / 'remote' / None - who owns the handle behind a mux port."""
    if not self._is_mux:
      return None
    return manager.owner_of(self.device)

  # ---------------------------------------------------------------- delegation

  @property
  def last_mux_error(self):
    """Why sharing was not possible, when this port fell back to native."""
    return self._last_mux_error

  @property
  def raw(self):
    """The wrapped pyserial object, for the rare case it is needed directly."""
    return self._ser

  def __getattr__(self, name):
    # Only reached for names not defined on this class. Guard against being
    # called before __init__ has assigned _ser, otherwise any error raised in
    # the constructor surfaces as a bare KeyError/AttributeError for '_ser'.
    ser = self.__dict__.get('_ser')
    if ser is None:
      raise AttributeError(name)
    return getattr(ser, name)

  def __setattr__(self, name, value):
    if name.startswith('_') or name in self.__class__.__dict__:
      object.__setattr__(self, name, value)
      return
    ser = self.__dict__.get('_ser')
    if ser is None:
      object.__setattr__(self, name, value)
    else:
      setattr(ser, name, value)

  def __repr__(self):
    kind = 'mux' if self._is_mux else 'native'
    return '<SerialPort %s %s open=%s>' % (kind, self._url, self._ser.is_open)
