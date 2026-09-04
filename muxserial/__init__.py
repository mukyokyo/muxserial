# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: (C) 2026 mukyokyo

"""
muxserial - transparent serial port multiplexer.

    import muxserial
    ser = muxserial.open('COM11', 115200, timeout=0.5)

A broker for COM11 is spawned on first use and reaped a few seconds after the
last client closes. Nothing else in the application has to change: the object
returned is an ordinary pyserial Serial.

Explicit forms also work through serial.serial_for_url():

    mux://COM11                     device, ephemeral local broker
    mux://COM11@127.0.0.1:5555      device, bound so other processes can join
    mux://127.0.0.1:5555            join a broker that already exists
"""

import serial as _pyserial

from .core import (MAGIC, T_DATA, T_CTRL, T_EVENT, FrameReader,
                   pack, pack_ctrl, pack_event, Broker, Server, serve)
from . import manager, presence, registry
from .manager import (shutdown, shutdown_all, active, set_verbose,
                      is_available, notify_device_change, owner_of)
from .presence import comports, normalize, raw_comports

if 'muxserial' not in _pyserial.protocol_handler_packages:
  _pyserial.protocol_handler_packages.append('muxserial')


def to_url(port, bind=None):
  """'COM11' -> 'mux://COM11'. Anything that is already a URL is left alone."""
  if '://' in str(port):
    return port
  if bind:
    return 'mux://%s@%s:%d' % (port, bind[0], bind[1])
  return 'mux://%s' % port


def open(port, *args, bind=None, **kwargs):
  """Drop-in for serial.Serial()/serial_for_url() that routes through the mux."""
  return _pyserial.serial_for_url(to_url(port, bind), *args, **kwargs)


def SerialPort(*args, **kwargs):
  """Lazy re-export so `import muxserial` stays cheap."""
  from .client import SerialPort as _SerialPort
  return _SerialPort(*args, **kwargs)


__all__ = ['open', 'to_url', 'SerialPort', 'shutdown', 'shutdown_all', 'active',
           'set_verbose', 'is_available', 'notify_device_change', 'owner_of',
           'comports', 'raw_comports', 'normalize',
           'Broker', 'Server', 'serve', 'manager', 'presence', 'registry']
