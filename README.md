# Pygate-for-P2P


## Firmware Modification
This software is licensed under the GNU GPL version 3 or any
later version, with permitted additional terms. For more information
see the Pycom Licence v1.0 document supplied with this file, or
available at https://www.pycom.io/opensource/licensing

The file, lora_pkt_fwd.c, was modified to introduce a simplified UDP/LoRa packet forwarding API.
This enables packets to be transmitted by sending UDP packets to the loopback interface on port
6001. The api reference can be found in 'firmwarereference.pdf'.

Helper functions were written defined in:
	udp_pkt_fwd.h
	udp_pkt_fwd.c

In order to build the firmware, first set up the Pycom ESP32 toolchain as explained in detail at https://github.com/pycom/pycom-micropython-sigfox. 
Ensure the vanilla Pygate firmware can be built by running:
	make BOARD=<Name of pycom development board> PYGATE=1

If this builds without errors, insert the three files, udp_pkt_fwd.h, udp_pkt_fwd.c and lora_pkt_fwd.c into the directory esp32/pygate/lora_pkt_fwd/. 
The lora_pkt_fwd.c file will be overwritten.

Now rebuild the firmware by running: 
	make BOARD=<Name of pycom development board> PYGATE=1

Provided the firmware compiles without errors the firmware may now be flashed by running:
	make BOARD=<Name of pycom development board> PYGATE=1 flash


If any errors are experienced, check that the PATH and IDF_PATH environment variables are set as per the instructions at https://github.com/pycom/pycom-micropython-sigfox.

## Pygate Driver
A simple MicroPython driver was written to enable easy control over the UDP packet forwarder and can be found in the 'pycom-driver' directory
