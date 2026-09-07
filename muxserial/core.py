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
    # 2026-09-03: [Tester] このクライアントが最後に要求した設定(baudrate等)。
    # 物理ポートは1つしか無いので他クライアントの都合で値が変わり得るが、この
    # クライアント自身の「これで喋るはず」という認識はここに握ったまま譲らない。
    # Broker.handle_data()が書き込み直前に物理ポート側をこれへ追従させる
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
        # stopで即座に抜けられるよう、素のsleepでは待たない
        self.stop.wait(0.02)
        continue
      try:
        # 2026-09-03: [Tester] ここは意図的にロックしない。実測したところ、この
        # 読み取りループを毎周self.lockで囲むと、無データ時でもtimeout=0.05分
        # ロックを持ち続けては即座に再取得を繰り返すため、handle_data/handle_ctrl
        # 側が長時間ロックを取れなくなる(再取得の方が有利になりがちな飢餓状態)。
        # read()とreconfigure(setattr)が理論上ごく短時間だけ重なる余地は残るが、
        # 元々あった隙であり、書き込みの方を確実に直列化することの方が重要
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
    # 2026-09-07: 物理ハンドルを閉じる前に_pumpを抜けさせる。read()の最中に別
    # スレッドからclose()すると、Windowsでは保留中のI/Oを巻き込んで解放が遅れる
    # (最悪ハンドルが残って次のopenが弾かれる)。read()のtimeoutは0.05なので、
    # ここでの待ちは実質1周分にしかならない
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
      # 2026-09-03: [Tester] 即座に物理ポートへ反映するのはこれまで通り(まだ一度も
      # 書いていないクライアントの読み取りもこれでカバーされる)。加えて、cが次に
      # 書き込む時にも同じ内容を再主張できるよう、c.settingsに覚えておく。他の
      # クライアントが間で勝手に設定を変えても、cにとっての「正しい設定」はこれ
      with self.lock:
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
    """[Tester] 物理ポートをcの最後の希望設定に追従させる。値が既に一致している
    項目は触らない(毎回無条件のsetattrはしない)。呼び出し元でself.lockを保持
    している前提。ブートローダ→アプリでボーレートが飛ぶような使い方でも、書き
    込み元のクライアントが自分の値を主張し続ければ物理層もそこに追従する"""
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
      # 2026-09-03: [Tester] 「今まさに書き込もうとする時に想定と違えば設定変え
      # てから処置する」。再設定と書き込みを同じロックの中で行い、他クライアント
      # の書き込み/再設定/_pumpの読み取りが割り込まないようにする
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
    # 決定論的ポートは「直前のブローカが去った直後に再び掴めること」と「稼働中の
    # ブローカからは絶対に奪えないこと」の両方が要る。この2つを意味するオプション
    # はOSで違い、WindowsのSO_REUSEADDRは稼働中の横取りを許してしまうため
    # SO_EXCLUSIVEADDRUSEが正しい。POSIXのSO_REUSEADDRはTIME_WAITを緩めるだけで
    # 稼働中のlistenerは奪えない
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
    # 先にlistenを畳む。acceptのtimeoutを待たずに新規接続を止められる
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
