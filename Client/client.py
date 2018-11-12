import http.client
import json
import re

class Client():
    
    def __init__(self, host):
        self.host = host
        self.headers = {'Content-type': 'application/json'}
        p = '(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
        m = re.search(p,host)
        self.connection = http.client.HTTPConnection(m.group('host'), m.group('port'))

    def send_entry(self, data):
        self.connection.request('POST', '/api/v1/entries', json.dumps(data), self.headers)
        response = self.connection.getresponse()
        print(response.read().decode())
    
    def get_entries(self):
        self.connection.request('GET', '/api/v1/entries')
        response = self.connection.getresponse()

        print(json.dumps(response.read().decode(), sort_keys=True, indent=4))