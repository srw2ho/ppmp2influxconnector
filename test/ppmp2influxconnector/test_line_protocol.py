import unittest
import re
from influxconnector.convertor.ppmp.v3.machine import PPMPMachine as PPMPMachineV3
from influxconnector.client import InfluxClient

TEST_PAYLOAD = """{
    "content-spec": "urn:spec://eclipse.org/unide/machine-message#v3",
    "device": {
        "additionalData": {
            "ActualSequence": {
                "ProcessData": "TestAblauf",
                "Serialnumber": "123456",
                "StartTest": "",
                "TestSequence": "TestAblauf",
                "Tester": "...",
                "TypeData": "TestAblauf"
            },
            "Company": "ABC GmbH",
            "InstalledVersion": "8.0.0.0",
            "hostname": "hostname_ID"
        },
        "id": "hostname_ID",
        "mode": "manual",
        "state": "OK"
    },
    "messages": [
        {
            "additionalData": {
                "firmware": ""
            },
            "code": "",
            "description": "",
            "hint": "",
            "origin": "",
            "severity": "",
            "title": "",
            "ts": "2019-04-16T13:48:27.826Z",
            "type": ""
        }
    ]
}"""


class Test(unittest.TestCase):

    def test_line_protocol(self):
        timestamps = []

        with open('./test/ppmp2influx/debug.txt') as f:
            for line in f:
                # 'hda-100002.ho.de.bosch.com,test-tag=28 curve.angle=0.0 1552029873715035904\n'
                timestamps.append(re.search('[0-9]{19}', line).group(0))
                # print(timestamp)

        # timestamp_diffs = map(lambda (i, ts): ts - (timestamps[i] if i > 0 else 0), enumerate(timestamps))
        timestamp_diffs = [int(ts) - (int(timestamps[i - 1]) if i > 0 else int(timestamps[i])) for i, ts in enumerate(timestamps)]

        timestamp_diffs_human = [f"{ts / 1000} μs" for ts in timestamp_diffs]

        print(timestamp_diffs_human)

    def test_machine_message(self):
        try:
            INFLUX_CLIENT = InfluxClient()
            INFLUX_CLIENT.connect(host="10.33.198.103", port=8086, username="admin", password="", database="ppmp")

            ppmp_obj = PPMPMachineV3(TEST_PAYLOAD)
            line_protocol = ppmp_obj.export_to_line_protocol()

            print(line_protocol)

            INFLUX_CLIENT.write(line_protocol, protocol='line')

            print('done!')
        except Exception:
            self.fail("test_machine_message() raised Exception unexpectedly!")


if __name__ == '__main__':
    unittest.main()
