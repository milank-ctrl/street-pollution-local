import struct
import serial
from datetime import datetime
import time
from pymongo import MongoClient
from urllib.parse import quote_plus
import json

class SDS011(object):
    
    """Provides method to read from a SDS011 air particlate density sensor
    using UART.
    """

    HEAD = b'\xaa'
    TAIL = b'\xab'
    CMD_ID = b'\xb4'

    # The sent command is a read or a write
    READ = b"\x00"
    WRITE = b"\x01"

    REPORT_MODE_CMD = b"\x02"
    ACTIVE = b"\x00"
    PASSIVE = b"\x01"

    QUERY_CMD = b"\x04"

    # The sleep command ID
    SLEEP_CMD = b"\x06"
    # Sleep and work byte
    SLEEP = b"\x00"
    WORK = b"\x01"

    # The work period command ID
    WORK_PERIOD_CMD = b'\x08'

    def __init__(self, serial_port, baudrate=9600, timeout=2, use_query_mode=True):
        #"""Initialise and open serial port."""
        self.ser = serial.Serial(port=serial_port, baudrate=baudrate, timeout=timeout)
        self.ser.flush()
        self.set_report_mode(active=not use_query_mode)

    def _execute(self, cmd_bytes):
        """Writes a byte sequence to the serial.
        """
        self.ser.write(cmd_bytes)

    def _get_reply(self):
        """Read reply from device."""
        raw = self.ser.read(size=10)
        data = raw[2:8]
        if len(data) == 0:
            return None
        if (sum(d for d in data) & 255) != raw[8]:
            return None  #TODO: also check cmd id
        return raw

    def cmd_begin(self):
        """Get command header and command ID bytes.
        @rtype: list
        """
        return self.HEAD + self.CMD_ID

    def set_report_mode(self, read=False, active=False):
        """Get sleep command. Does not contain checksum and tail.
        @rtype: list
        """
        cmd = self.cmd_begin()
        cmd += (self.REPORT_MODE_CMD
                + (self.READ if read else self.WRITE)
                + (self.ACTIVE if active else self.PASSIVE)
                + b"\x00" * 10)
        cmd = self._finish_cmd(cmd)
        self._execute(cmd)
        self._get_reply()

    def query(self):
        """Query the device and read the data.

        @return: Air particulate density in micrograms per cubic meter.
        @rtype: tuple(float, float) -> (PM2.5, PM10)
        """
        cmd = self.cmd_begin()
        cmd += (self.QUERY_CMD
                + b"\x00" * 12)
        cmd = self._finish_cmd(cmd)
        self._execute(cmd)

        raw = self._get_reply()
        if raw is None:
            return None  #TODO:
        data = struct.unpack('<HH', raw[2:6])
        pm25 = data[0] / 10.0
        pm10 = data[1] / 10.0
        return (pm25, pm10)

    def sleep(self, read=False, sleep=True):
        """Sleep/Wake up the sensor.

        @param sleep: Whether the device should sleep or work.
        @type sleep: bool
        """
        cmd = self.cmd_begin()
        cmd += (self.SLEEP_CMD
                + (self.READ if read else self.WRITE)
                + (self.SLEEP if sleep else self.WORK)
                + b"\x00" * 10)
        cmd = self._finish_cmd(cmd)
        self._execute(cmd)
        self._get_reply()

    def set_work_period(self, read=False, work_time=0):
        """Get work period command. Does not contain checksum and tail.
        @rtype: list
        """
        assert work_time >= 0 and work_time <= 30
        cmd = self.cmd_begin()
        cmd += (self.WORK_PERIOD_CMD
                + (self.READ if read else self.WRITE)
                + bytes([work_time])
                + b"\x00" * 10)
        cmd = self._finish_cmd(cmd)
        self._execute(cmd)
        self._get_reply()

    def _finish_cmd(self, cmd, id1=b"\xff", id2=b"\xff"):
        """Add device ID, checksum and tail bytes.
        @rtype: list
        """
        cmd += id1 + id2
        checksum = sum(d for d in cmd[2:]) % 256
        cmd += bytes([checksum]) + self.TAIL
        return cmd

    def _process_frame(self, data):
        """Process a SDS011 data frame.

        Byte positions:
            0 - Header
            1 - Command No.
            2,3 - PM2.5 low/high byte
            4,5 - PM10 low/high
            6,7 - ID bytes
            8 - Checksum - sum of bytes 2-7
            9 - Tail
        """
        raw = struct.unpack('<HHxxBBB', data[2:])
        checksum = sum(v for v in data[2:8]) % 256
        if checksum != data[8]:
            return None
        pm25 = raw[0] / 10.0
        pm10 = raw[1] / 10.0
        return (pm25, pm10)

    def read(self):
        """Read sensor data.

        @return: PM2.5 and PM10 concetration in micrograms per cude meter.
        @rtype: tuple(float, float) - first is PM2.5.
        """
        byte = 0
        while byte != self.HEAD:
            byte = self.ser.read(size=1)
            d = self.ser.read(size=10)
            if d[0:1] == b"\xc0":
                data = self._process_frame(byte + d)
                return data

def getTime():
    now_ = datetime.now()
    iso_date = now_.isoformat()
    return iso_date

def getUnix():
    now_ns = int(time.time_ns() / 1000)
    return int(str(now_ns)[:10])

def write_json(new_data, filename):
    with open(filename, 'r+') as f:
        file_data = json.load(f)
        file_data["data"].append(new_data)
        f.seek(0)
        json.dump(file_data, f, indent=4)


json_path = "/home/pi/Desktop/python/pollution/aqi/sensor/keys.json"
sensorDataPath = '/home/pi/Desktop/python/pollution/aqi/sensor/data/sensorData.json'

with open(json_path, "r") as f: 
    auth_keys = json.load(f)


user = quote_plus(auth_keys["mongo_user"])
pw = quote_plus(auth_keys["mongo_pw"])

uri = 'mongodb://%s:%s@127.0.0.1:27017'%(user, pw)
client = MongoClient(uri)
db = client.aqi

port = "/dev/ttyUSB0"
sensor = SDS011(port)


while True:
    
    print("Measuring pm10 and pm25 values...")
    pm25, pm10 = sensor.query()
    currentDateTime = getTime()
    unix_ts = getUnix()

    doc = {"unix": unix_ts, "sampleTime": currentDateTime, "pm10": pm10, "pm25": pm25, "test": 0, "unit": "ug/m3"}
    print("Row created: {}".format(doc))
    
    
    write_json(doc, sensorDataPath)
    print("Saving the file locally...")

    mdb = db.sensor.insert_one(doc)
    print("Adding to mongodb... {}".format(mdb))

    print("Sleep 5 sec...")
    print("---")
    
    #go to sleep
    time.sleep(5)



