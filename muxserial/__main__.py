# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: (C) 2026 mukyokyo

import argparse

import serial

from .core import serve
from . import manager


def main():
  ap = argparse.ArgumentParser(prog='muxserial')
  ap.add_argument('--port', required=True, help='physical port, e.g. COM3')
  ap.add_argument('--baud', type=int, default=115200)
  ap.add_argument('--listen', type=int, default=5555)
  ap.add_argument('--host', default='127.0.0.1')
  ap.add_argument('-v', '--verbose', action='store_true')
  args = ap.parse_args()

  manager.set_verbose(args.verbose)
  if args.port in ('echo', 'echo://'):
    from .echoport import EchoPort
    ser = EchoPort()
  else:
    ser = serial.serial_for_url(args.port, args.baud, timeout=0.05)
  serve(ser, args.listen, host=args.host, verbose=args.verbose)


if __name__ == '__main__':
  main()
