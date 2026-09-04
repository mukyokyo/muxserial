# muxserial

Transparent serial port multiplexer for Python.

`muxserial` allows multiple processes or threads to share a single physical serial port simultaneously without fighting over the OS handle. It acts as a drop-in replacement for `pyserial`, automatically spawning a background broker on first use, and handles hardware disconnects gracefully.

## Features

* **Zero Configuration:** Just use `muxserial.open('COM11')` and it handles the background broker automatically.
* **Pyserial Compatible:** Returns a standard `serial.Serial` compatible object. Integrates cleanly as a `pyserial` protocol handler (`mux://COM11`).
* **Cross-Process Sharing:** Run a GUI monitor and a firmware flasher (e.g., `esptool`) simultaneously on the same port.
* **Exclusive Transactions:** Use `.lease()` to temporarily block other clients from interleaving data (essential for XMODEM or STK500 transfers).
* **Handle Handoff:** Temporarily give up the OS handle with `.detach()` to let external CLI tools use the raw port, then `.reattach()` without dropping clients.
* **Physical Presence Tracking:** Gracefully handles USB unplugs and replugs.

## Installation

```bash
pip install .
```

*(Note: Requires `pyserial`)*

## Usage

### Basic Multiplexing

The easiest way is to use `muxserial.open()` as a drop-in replacement for `serial.Serial()`:

```python
import muxserial

# Automatically spawns a broker if one isn't running, or joins an existing one
ser = muxserial.open('COM11', 115200, timeout=0.5)

ser.write(b'Hello')
print(ser.read(10))
```

### Using the pyserial URL Handler

```python
import serial
import muxserial  # Register the mux:// handler

ser = serial.serial_for_url('mux://COM11', 115200)
```

### Leasing and Detaching (Advanced)

For operations that require exclusive access or need to hand off the physical port to an external tool, use `SerialPort`:

```python
from muxserial.client import SerialPort
import subprocess

port = SerialPort('COM11', 115200)

# 1. Exclusive write (blocks other multiplexed clients temporarily)
with port.lease(rx='tee'):
    port.write(b'Exclusive command')

# 2. Detach to let external tools use the raw port exclusively
real_port_name = port.detach()
print(f"Port released. Executing tool on {real_port_name}...")
subprocess.run(['esptool.py', '--port', real_port_name, 'flash'])

# 3. Resume multiplexing
port.reattach()
```

### Standalone Server

You can also run the broker manually from the command line:

```bash
python -m muxserial --port COM11 --baud 115200
```
