import machine
import socket
import struct
import ujson
import json
import math
import queue
import ubinascii

class Pygate:
    def __init__(self):
        # Read the GW config file from Filesystem
        fp = open('/flash/config.json','r')
        buf = fp.read()
        machine.pygate_init(buf)
        # disable degub messages
        machine.pygate_debug_level(0)

        self.ip = "127.0.0.1"
        self.udp_receive_port = 6000
        self.udp_send_port = 6001

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.udp_receive_port))
        self.sock.setblocking(0)

        # Attempt with small queue
        self.tx_stat_queue = queue.Queue(maxsize=50)
        self.lora_queue = queue.Queue(maxsize=50)

        self.LORA_PACKET = 0x01
        self.TX_STAT_PACKET = 0x02

        self.set_pygate_channels()

    def set_pygate_channels(self):
        self.pygate_channels = []
        self.pygate_channels.append((0,867.1))
        self.pygate_channels.append((0,867.3))
        self.pygate_channels.append((0,867.5))
        self.pygate_channels.append((0,867.7))
        self.pygate_channels.append((0,867.9))
        self.pygate_channels.append((0,868.1))
        self.pygate_channels.append((0,868.3))
        self.pygate_channels.append((0,868.5))

    def send_ch(self, sf, channel, payload):
        if channel > 7 or channel < 0:
            return
        ch = self.pygate_channels[channel]
        self.send(sf, ch[0], ch[1], payload)

    def send(self, sf, radio, freq, payload):
        payload_size = len(payload)

        sf_string = ""
        # check sf in range
        if sf < 7 or sf > 12:
            return
        else:
            sf_string = "SF{}BW125".format(sf)
        if len(payload) > 255:
            return

        # Base64 encode payload
        if type(payload) is str:
            payload = bytes(payload,'utf-8')
        b64 = ubinascii.b2a_base64(payload)
        b64_str = b64.decode('utf-8')
        b64_str = b64_str.replace("\n","")

        # msg object
        msg = {
            "txpk": {
                "freq":freq,
                "rfch":radio,
                "powe":14,
                "datr":sf_string,
                "codr":"4/5",
                "size":payload_size,
                "data":b64_str
            }
        }
        json = ujson.dumps(msg)
        self.sock.sendto(json, (self.ip, self.udp_send_port))

    def receive_lora(self):
        queue_empty = False
        packet = []
        # First check if LoRa packet queue has items
        try:
            packet = self.lora_queue.get(False)
            queue_empty = False
        except OSError as e:
            queue_empty = True

        # Receive a packet. If not LoRa add to TX queue
        if queue_empty:
            packet = self.sock.recv(512) # Check this size

        if len(packet) >= 3:
            # Check which type
            packet_type = struct.unpack('b', packet)[0]

            # if its a TX stat packet. Add to queue and return
            if packet_type == self.TX_STAT_PACKET:
                put_in_queue = False
                while not put_in_queue:
                    try:
                        self.tx_stat_queue.put(packet, block=False)
                        put_in_queue = True
                    except Exception as e:
                        print(e)
                        # Pop an item and discard
                        self.tx_stat_queue.get(False)
                return (None, None)
            # if LoRa packet then process it...
            elif packet_type == self.LORA_PACKET:
                packet_type, payload_size, header_size = struct.unpack('bbb', packet)

                header_string = packet[3:3 + header_size].decode("utf-8")
                header_string = str("{" + header_string.replace('\x00', '')) # Firmware will fix this
                # convert to json
                header = json.loads(header_string)
                payload = packet[3 + header_size:3+header_size+payload_size]
                # Send heartbeat
                formatString = ""
                for b in packet:
                    formatString += ("0x{:02x} ".format(b))
                #print('Packet received at pygate: ', formatString)
                return (header, payload)
        return (None, None)

    def receive_stat(self):
        queue_empty = False
        # First check if TX stat queue has items
        try:
            packet = self.tx_stat_queue.get(False)
            queue_empty = False
        except OSError as e:
            queue_empty = True

        # Receive a packet. If not LoRa add to TX queue
        if queue_empty:
            packet = self.sock.recv(512) # Check this size

        if len(packet) >= 3:
            # Check which type
            packet_type = struct.unpack('b', packet)[0]

            # if its  lora packet then add to queue and return
            if packet_type == self.LORA_PACKET:
                # This might throw an exception if queue is full
                put_in_queue = False
                while not put_in_queue:
                    try:
                        self.lora_queue.put(packet, block=False)
                        put_in_queue = True
                    except Exception as e:
                        print(e)
                        # Pop an item and discard
                        self.lora_queue.get(False)

                return -1
            elif packet_type == self.TX_STAT_PACKET:
                packet_type, toa = struct.unpack('<bl', packet)
                return toa
        return -1
