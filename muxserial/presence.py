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

import fnmatch
import re
import sys
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


def _natural_key(name):
  """COM2 before COM10."""
  return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', str(name))]


def _win_bus_labels():
  """{'COM33': 'RA4M2 Console', ...} straight from SetupAPI.

  pyserial's description is the friendly name the driver builds ("USB Serial
  Device (COM33)"), identical for every CDC port. What the device says about
  itself (the interface string of a composite device, else the product string)
  is only exposed as DEVPKEY_Device_BusReportedDeviceDesc, which Device Manager
  shows as "Bus reported device description". Same classes as pyserial walks.
  """
  import ctypes
  from ctypes import wintypes

  setupapi = ctypes.WinDLL('setupapi', use_last_error=True)
  advapi32 = ctypes.WinDLL('advapi32', use_last_error=True)

  class GUID(ctypes.Structure):
    _fields_ = [('Data1', wintypes.DWORD), ('Data2', wintypes.WORD), ('Data3', wintypes.WORD),
                ('Data4', ctypes.c_ubyte * 8)]

  def guid(text):
    h = text.replace('-', '')
    g = GUID()
    g.Data1, g.Data2, g.Data3 = int(h[0:8], 16), int(h[8:12], 16), int(h[12:16], 16)
    for i in range(8):
      g.Data4[i] = int(h[16 + 2 * i:18 + 2 * i], 16)
    return g

  class SP_DEVINFO_DATA(ctypes.Structure):
    _fields_ = [('cbSize', wintypes.DWORD), ('ClassGuid', GUID), ('DevInst', wintypes.DWORD),
                ('Reserved', ctypes.c_void_p)]

  class DEVPROPKEY(ctypes.Structure):
    _fields_ = [('fmtid', GUID), ('pid', wintypes.ULONG)]

  HDEVINFO = ctypes.c_void_p
  HKEY = ctypes.c_void_p
  INVALID_HANDLE = ctypes.c_void_p(-1).value
  DIGCF_PRESENT = 0x00000002
  DICS_FLAG_GLOBAL = 0x00000001
  DIREG_DEV = 0x00000001
  KEY_READ = 0x00020019
  DEVPROP_TYPE_STRING = 0x00000012

  get_class_devs = setupapi.SetupDiGetClassDevsW
  get_class_devs.argtypes = [ctypes.POINTER(GUID), wintypes.LPCWSTR, wintypes.HWND, wintypes.DWORD]
  get_class_devs.restype = HDEVINFO
  enum_device_info = setupapi.SetupDiEnumDeviceInfo
  enum_device_info.argtypes = [HDEVINFO, wintypes.DWORD, ctypes.POINTER(SP_DEVINFO_DATA)]
  enum_device_info.restype = wintypes.BOOL
  open_dev_reg_key = setupapi.SetupDiOpenDevRegKey
  open_dev_reg_key.argtypes = [HDEVINFO, ctypes.POINTER(SP_DEVINFO_DATA), wintypes.DWORD, wintypes.DWORD,
                               wintypes.DWORD, wintypes.DWORD]
  open_dev_reg_key.restype = HKEY
  get_device_property = setupapi.SetupDiGetDevicePropertyW
  get_device_property.argtypes = [HDEVINFO, ctypes.POINTER(SP_DEVINFO_DATA), ctypes.POINTER(DEVPROPKEY),
                                  ctypes.POINTER(wintypes.ULONG), ctypes.c_void_p, wintypes.DWORD,
                                  ctypes.POINTER(wintypes.DWORD), wintypes.DWORD]
  get_device_property.restype = wintypes.BOOL
  destroy_list = setupapi.SetupDiDestroyDeviceInfoList
  destroy_list.argtypes = [HDEVINFO]
  destroy_list.restype = wintypes.BOOL
  reg_query_value = advapi32.RegQueryValueExW
  reg_query_value.argtypes = [HKEY, wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD),
                              ctypes.c_void_p, ctypes.POINTER(wintypes.DWORD)]
  reg_query_value.restype = wintypes.LONG
  reg_close = advapi32.RegCloseKey
  reg_close.argtypes = [HKEY]
  reg_close.restype = wintypes.LONG

  # DEVPKEY_Device_BusReportedDeviceDesc
  bus_desc_key = DEVPROPKEY(guid('540b947e-8b40-45bc-a8a2-6a0b894cbda2'), 4)

  def port_name(hdev, info):
    key = open_dev_reg_key(hdev, ctypes.byref(info), DICS_FLAG_GLOBAL, 0, DIREG_DEV, KEY_READ)
    if not key or key == INVALID_HANDLE:
      return ''
    try:
      buf = ctypes.create_unicode_buffer(256)
      size = wintypes.DWORD(ctypes.sizeof(buf))
      if reg_query_value(key, 'PortName', None, None, buf, ctypes.byref(size)) != 0:
        return ''
      return buf.value
    finally:
      reg_close(key)

  def bus_desc(hdev, info):
    buf = ctypes.create_unicode_buffer(512)
    ptype = wintypes.ULONG(0)
    need = wintypes.DWORD(0)
    ok = get_device_property(hdev, ctypes.byref(info), ctypes.byref(bus_desc_key), ctypes.byref(ptype),
                             buf, ctypes.sizeof(buf), ctypes.byref(need), 0)
    if not ok or ptype.value != DEVPROP_TYPE_STRING:
      return ''
    return buf.value.strip()

  out = {}
  # Ports (COM & LPT) and Modem (some CDC drivers install there)
  for class_guid in ('4d36e978-e325-11ce-bfc1-08002be10318', '4d36e96d-e325-11ce-bfc1-08002be10318'):
    g = guid(class_guid)
    hdev = get_class_devs(ctypes.byref(g), None, None, DIGCF_PRESENT)
    if not hdev or hdev == INVALID_HANDLE:
      continue
    try:
      index = 0
      while True:
        info = SP_DEVINFO_DATA()
        info.cbSize = ctypes.sizeof(SP_DEVINFO_DATA)
        if not enum_device_info(hdev, index, ctypes.byref(info)):
          break
        index += 1
        name = port_name(hdev, info)
        if not name or name.upper().startswith('LPT'):
          continue
        label = bus_desc(hdev, info)
        if label:
          out[normalize(name)] = label
    finally:
      destroy_list(hdev)
  return out


def labels():
  """Port name -> the name the USB device reports for that port.

  Windows: "Bus reported device description" (see _win_bus_labels).
  Linux: the USB interface string, else the product string (pyserial reads
  both from sysfs). Ports that report nothing are left out, and so is every
  port when enumeration fails: an empty dict only means nothing is known.
  """
  try:
    if sys.platform == 'win32':
      with _enum_lock:
        return _win_bus_labels()
    out = {}
    for p in raw_comports():
      name = (getattr(p, 'interface', None) or getattr(p, 'product', None) or '').strip()
      if name:
        out[normalize(p.device)] = name
    return out
  except Exception:
    return {}


def find_by_label(pattern, prefer=None, table=None):
  """The port whose label matches pattern, or None.

  Case-insensitive, surrounding spaces ignored, * and ? as in fnmatch.
  When several match, `prefer` wins if it is one of them (keeps a runner on
  the board it already has), otherwise the lowest port number.
  `table` is a labels() result to reuse instead of enumerating again.
  """
  want = str(pattern or '').strip().casefold()
  if not want:
    return None
  if table is None:
    table = labels()
  hits = sorted((name for name, label in table.items() if fnmatch.fnmatchcase(label.strip().casefold(), want)),
                key=_natural_key)
  if not hits:
    return None
  if prefer and normalize(prefer) in hits:
    return normalize(prefer)
  return hits[0]


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
