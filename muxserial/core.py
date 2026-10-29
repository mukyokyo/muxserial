# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: (C) 2026 mukyokyo

"""Wire format, broker and stoppable server. Imported via muxserial/__init__."""

import json
import socket
import struct
import threading
import time

MAGIC = 0x4D
T_DATA, T_CTRL, T_EVENT = 0, 1, 2
HDR = struct.Struct('>BBI')


def pack(t, payload):
  return HDR.pack(MAGIC, t, len(payload)) + payload


def pack_ctrl(op, **kw):
  kw['op'] = op
  return pack(T_CTRL, json.dumps(kw).encode('utf-8'))


def pack_event(ev, **kw):
  kw['ev'] = ev
  return pack(T_EVENT, json.dumps(kw).encode('utf-8'))


class FrameReader:
  def __init__(self):
    self.buf = bytearray()

  def feed(self, data):
    self.buf += data
    out = []
    while len(self.buf) >= HDR.size:
      magic, t, n = HDR.unpack_from(self.buf)
      if magic != MAGIC:
        raise ValueError('frame desync')
      if len(self.buf) < HDR.size + n:
        break
      out.append((t, bytes(self.buf[HDR.size:HDR.size + n])))
      del self.buf[:HDR.size + n]
    return out


class _Client:
  def __init__(self, sock, addr):
    self.sock = sock
    self.addr = addr
    self.reader = FrameReader()
    self.wlock = threading.Lock()
    self.alive = True
    # 2026-09-03: [Tester] The latest settings requested by this client (baudrate, etc.).
    # Since there is only one physical port, values may change due to other clients,
    # but this client's perception of "how it should talk" is retained here.
    # Broker.handle_data() makes the physical port follow this just before writing.
    self.settings = {}

  def send(self, frame):
    if not self.alive:
      return
    try:
      with self.wlock:
        self.sock.sendall(frame)
    except OSError:
      self.alive = False

  def close(self):
    self.alive = False
    try:
      self.sock.close()
    except OSError:
      pass


class Broker:
  """Owns exactly one physical serial port and fans it out to N clients."""

  MODEM_BITS = ('cts', 'dsr', 'ri', 'cd')

  def __init__(self, ser, verbose=False):
    self.ser = ser
    self.verbose = verbose
    self.clients = []
    self.lock = threading.RLock()
    self.cv = threading.Condition(self.lock)
    # Serialize only writes to the physical port and accompanying setting changes.
    # Keep separate from self.lock because writes do not return until the device receives them
    # (enclosing writes with self.lock stops _pump and broadcast, causing deadlocks with echoing devices).
    # Acquisition order is txlock -> lock; never acquire txlock while holding lock.
    self.txlock = threading.Lock()
    self.lease_owner = None
    self.lease_rx = 'tee'
    self.detached = False
    self.lost = False
    self.stop = threading.Event()
    self._modem = None
    self._threads = [threading.Thread(target=self._pump, daemon=True),
                     threading.Thread(target=self._modem_poll, daemon=True)]
    for t in self._threads:
      t.start()

  def log(self, *a):
    if self.verbose:
      print('[broker]', *a, flush=True)

  def client_count(self):
    with self.lock:
      return len(self.clients)

  # -- fan-out ---------------------------------------------------------------

  def _targets(self):
    with self.lock:
      if self.lease_owner is not None and self.lease_rx == 'own':
        return [self.lease_owner]
      return list(self.clients)

  def broadcast(self, frame, only=None):
    for c in (only if only is not None else self._targets()):
      c.send(frame)

  def _pump(self):
    while not self.stop.is_set():
      with self.lock:
        idle = self.detached or self.lost
      if idle:
        # Avoid bare sleep so it can exit immediately on stop
        self.stop.wait(0.02)
        continue
      try:
        # 2026-09-03: [Tester] Intentionally unlocked here. Benchmarks showed that
        # enclosing this read loop in self.lock on every iteration causes it to hold
        # the lock for timeout=0.05 even when idle, then immediately re-acquire it.
        # This starves handle_data/handle_ctrl from acquiring the lock for long periods.
        # While read() and reconfigure(setattr) could theoretically overlap briefly,
        # that window existed anyway, and serializing writes is far more critical.
        data = self.ser.read(1)
        if data and self.ser.in_waiting:
          data += self.ser.read(self.ser.in_waiting)
      except Exception as e:
        if self.stop.is_set():
          return
        # A read failure on a device that has been yanked is the usual case;
        # let the presence tracker decide, but stop hammering the handle.
        self.mark_lost(str(e))
        continue
      if data:
        self.broadcast(pack(T_DATA, data))

  def _modem_poll(self):
    while not self.stop.is_set():
      self.stop.wait(0.1)
      if self.stop.is_set():
        return
      with self.lock:
        if self.detached or self.lost or not self.clients:
          continue
      try:
        st = {k: bool(getattr(self.ser, k)) for k in self.MODEM_BITS}
      except Exception:
        continue
      if st != self._modem:
        self._modem = st
        self.broadcast(pack_event('modem', **st), only=list(self.clients))

  # -- client lifecycle ------------------------------------------------------

  def add(self, c):
    with self.lock:
      self.clients.append(c)
    self.log('client +', c.addr, len(self.clients))
    if self._modem:
      c.send(pack_event('modem', **self._modem))

  def remove(self, c):
    with self.cv:
      if c in self.clients:
        self.clients.remove(c)
      if self.lease_owner is c:
        self.lease_owner = None
        self.cv.notify_all()
    c.close()
    self.log('client -', c.addr, len(self.clients))

  def mark_lost(self, reason='device removed'):
    """The physical device is gone: drop the handle and tell everyone."""
    with self.cv:
      if self.lost:
        return
      self.lost = True
      self.lease_owner = None
      self.cv.notify_all()
    try:
      if getattr(self.ser, 'is_open', False):
        self.ser.close()
    except Exception:
      pass
    self._modem = None
    self.log('lost:', reason)
    self.broadcast(pack_event('device_lost', msg=reason), only=list(self.clients))

  def recycle(self, reason='device re-enumerated'):
    """Swap the OS handle for a fresh one without ending anybody's session.

    A native-USB board that resets keeps its COM name but becomes a different
    device; the handle we hold still opens and reads, and simply never delivers
    anything. Reopening is the only cure, and it is not the client's fault, so
    the clients stay attached and merely get told.
    """
    with self.cv:
      if self.detached:
        return None
      err = None
      try:
        if getattr(self.ser, 'is_open', False):
          self.ser.close()
      except Exception:
        pass
      try:
        self.ser.open()
      except Exception as e:
        err = str(e)
      self._modem = None
      if err is None:
        self.lost = False
      self.cv.notify_all()
    self.log('recycled:', reason, err or 'ok')
    if err is not None:
      self.mark_lost(err)
      return err
    self.broadcast(pack_event('device_reset', msg=reason),
                   only=list(self.clients))
    return None

  def mark_found(self):
    """The device came back. Returns an error string, or None on success."""
    err = None
    with self.cv:
      if not self.lost:
        return None
      try:
        self.ser.open()
      except Exception as e:
        err = str(e)
      else:
        self.lost = False
        self._modem = None
      self.cv.notify_all()
    self.broadcast(pack_event('device_found', error=err), only=list(self.clients))
    return err

  def shutdown(self, join_timeout=0.25):
    self.stop.set()
    with self.cv:
      clients = list(self.clients)
      self.clients = []
      self.lease_owner = None
      self.cv.notify_all()
    for c in clients:
      c.close()
    # 2026-09-07: Let _pump exit before closing the physical handle. Closing from another
    # thread during read() causes delayed release on Windows due to pending I/O
    # (in the worst case, the handle remains and blocks the next open). Since read() timeout is 0.05,
    # waiting here practically lasts for only one iteration.
    for t in self._threads:
      if t is not threading.current_thread():
        t.join(join_timeout)
    try:
      if getattr(self.ser, 'is_open', False):
        self.ser.close()
    except Exception:
      pass

  # -- control ---------------------------------------------------------------

  def handle_ctrl(self, c, msg):
    op = msg.get('op')

    if op == 'config':
      # 2026-09-03: [Tester] Apply immediately to the physical port as before (covering
      # reads for clients that haven't written yet). Additionally, store in c.settings
      # so c can re-assert its settings before its next write. Even if another client
      # changes settings in between, this remains the "correct settings" for c.
      with self.txlock, self.lock:
        if self._blocked(c):
          return
        for k in ('baudrate', 'bytesize', 'parity', 'stopbits',
                  'xonxoff', 'rtscts', 'dsrdtr'):
          if k in msg:
            c.settings[k] = msg[k]
            try:
              setattr(self.ser, k, msg[k])
            except Exception as e:
              c.send(pack_event('error', msg='%s: %s' % (k, e)))

    elif op in ('dtr', 'rts', 'break'):
      with self.lock:
        if self.lease_owner not in (None, c):
          self.log('drop %s from non-owner' % op)
          return
      try:
        setattr(self.ser, {'break': 'break_condition'}.get(op, op), bool(msg['v']))
      except Exception as e:
        c.send(pack_event('error', msg=str(e)))

    elif op == 'purge_tx':
      try:
        self.ser.reset_output_buffer()
      except Exception:
        pass

    elif op == 'lease':
      if msg.get('acquire'):
        deadline = time.monotonic() + float(msg.get('timeout', 10.0))
        with self.cv:
          while self.lease_owner is not None and self.lease_owner is not c:
            if not self.cv.wait(max(0.0, deadline - time.monotonic())):
              c.send(pack_event('lease_denied'))
              return
          self.lease_owner = c
          self.lease_rx = msg.get('rx', 'tee')
        self._drain_tx()
        try:
          self.ser.reset_input_buffer()
        except Exception:
          pass
        c.send(pack_event('lease_granted'))
      else:
        with self.cv:
          if self.lease_owner is c:
            self.lease_owner = None
            self.cv.notify_all()
        c.send(pack_event('lease_released'))

    elif op == 'detach':
      with self.cv:
        if self.lease_owner not in (None, c):
          c.send(pack_event('detach_denied'))
          return
        name = self.ser.port
        self.detached = True
      self._drain_tx()
      time.sleep(0.05)
      try:
        self.ser.close()
      except Exception:
        pass
      self.broadcast(pack_event('suspended', port=name), only=list(self.clients))
      c.send(pack_event('detached', port=name))
      self.log('detached', name)

    elif op == 'reattach':
      err = None
      with self.cv:
        if self.detached:
          try:
            self.ser.open()
          except Exception as e:
            err = str(e)
          self.detached = False
          self._modem = None
      self.broadcast(pack_event('resumed', error=err), only=list(self.clients))
      self.log('reattached', err or 'ok')

  def _drain_tx(self):
    """Wait if another client's write is in progress (before acquiring or releasing lease)"""
    with self.txlock:
      pass

  def _blocked(self, c):
    return self.lease_owner is not None and self.lease_owner is not c

  def wait_writable(self, c, timeout=30.0):
    deadline = time.monotonic() + timeout
    with self.cv:
      if self.lost:
        return False
      while self._blocked(c) or self.detached:
        if self.stop.is_set():
          return False
        if not self.cv.wait(max(0.0, deadline - time.monotonic())):
          return False
      return True

  def _sync_client_config(self, c):
    """[Tester] Synchronize physical port to c's latest desired settings.
    Do not touch properties whose values already match (avoid unconditional setattr each time).
    Assumes caller holds self.lock. Even in use cases where baud rate changes, like
    bootloader -> app, as long as the writing client asserts its settings, the physical layer follows."""
    for k, v in c.settings.items():
      try:
        if getattr(self.ser, k) != v:
          setattr(self.ser, k, v)
      except Exception as e:
        c.send(pack_event('error', msg='%s: %s' % (k, e)))

  def handle_data(self, c, data):
    if self.lost:
      c.send(pack_event('device_lost', msg='device removed'))
      return
    if not self.wait_writable(c):
      c.send(pack_event('error', msg='write blocked by lease'))
      return
    try:
      # Align physical port with c's settings right before writing.
      # Use txlock to prevent interleaved writes/reconfigurations from other clients, holding self.lock only during alignment.
      # Since _pump continues reading during writes, receiving will not stall if device TX is blocked.
      # If unplugged, mark_lost closes the handle and aborts pending writes.
      with self.txlock:
        with self.lock:
          self._sync_client_config(c)
        self.ser.write(data)
    except Exception as e:
      c.send(pack_event('error', msg=str(e)))


class Server:
  """Listening socket + broker. Stoppable, so it can be reaped when idle."""

  def __init__(self, ser, host='127.0.0.1', port=0, verbose=False):
    self.verbose = verbose
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Deterministic ports require both "can be re-bound immediately after the previous broker leaves"
    # and "can never be hijacked from a running broker". Options providing these behaviors differ by OS:
    # on Windows, SO_REUSEADDR allows hijacking active sockets, so SO_EXCLUSIVEADDRUSE is correct.
    # On POSIX, SO_REUSEADDR merely relaxes TIME_WAIT without allowing hijacking active listeners.
    if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
      try:
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
      except OSError:
        pass
    else:
      self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.sock.bind((host, port))
    self.sock.listen(8)
    self.sock.settimeout(0.5)
    self.host, self.port = self.sock.getsockname()
    self.stop = threading.Event()
    self.broker = Broker(ser, verbose=verbose) if ser is not None else None
    self.thread = None
    if self.broker is not None:
      self._start()

  def attach(self, ser):
    """Give a socket-only Server its port and start serving."""
    if self.broker is None:
      self.broker = Broker(ser, verbose=self.verbose)
      self._start()
    return self.broker

  def _start(self):
    if self.thread is None:
      self.thread = threading.Thread(target=self._accept, daemon=True)
      self.thread.start()

  def client_count(self):
    return self.broker.client_count() if self.broker else 0

  def _accept(self):
    while not self.stop.is_set():
      try:
        sock, addr = self.sock.accept()
      except socket.timeout:
        continue
      except OSError:
        break
      threading.Thread(target=self._session, args=(sock, addr),
                       daemon=True).start()

  def _session(self, sock, addr):
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    c = _Client(sock, addr)
    self.broker.add(c)
    c.send(pack_event('ready', port=str(getattr(self.broker.ser, 'port', ''))))
    if self.broker.lost:
      c.send(pack_event('device_lost', msg='device removed'))
    try:
      while not self.stop.is_set():
        chunk = sock.recv(65536)
        if not chunk:
          break
        for t, payload in c.reader.feed(chunk):
          if t == T_DATA:
            self.broker.handle_data(c, payload)
          elif t == T_CTRL:
            self.broker.handle_ctrl(c, json.loads(payload.decode('utf-8')))
    except (OSError, ValueError):
      pass
    finally:
      self.broker.remove(c)

  def shutdown(self):
    self.stop.set()
    # Close listen socket first to stop new connections without waiting for accept timeout
    try:
      self.sock.close()
    except OSError:
      pass
    if self.broker is not None:
      self.broker.shutdown()


def serve(ser, listen_port, host='127.0.0.1', verbose=False):
  """Blocking entry point for the standalone CLI."""
  srv = Server(ser, host=host, port=listen_port, verbose=verbose)
  print('muxserial: %s -> %s:%d' % (ser.port, srv.host, srv.port), flush=True)
  try:
    while True:
      time.sleep(0.5)
  except KeyboardInterrupt:
    srv.shutdown()
