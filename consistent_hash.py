from bisect import bisect
from Client.client import Client
from hashlib import md5
import csv
import sys
import pandas as pd 
import concurrent.futures
import requests
import time

class Consistent_Hash():

    def __init__(self, replicas = 3):
        nodes = self.generate_nodes(replicas)
        hnodes = [hashit(node) for node in nodes]
        hnodes.sort()

        self.replicas = replicas
        self.nodes = nodes
        self.hnodes = hnodes
        self.node_map = {hashit(node): node.split("-")[1] for node in nodes}

    

    @staticmethod
    def generate_nodes(replicas):
        nodes = []
        for n in range(replicas):
            for server in servers:
                nodes.append("{0}-{1}".format(n, server))
        return nodes

    def get_node(self, key):
        position = bisect(self.hnodes, hashit(key))
        if position == len(self.hnodes):
            return self.node_map[self.hnodes[0]]
        else:
            return self.node_map[self.hnodes[position]]


def send_request():
    for server in servers:
        connections[server] = Client(server)
    h_ring = Consistent_Hash()
    data = csv_parser(file_name)
    i = 0
    for key, value in data:
        if connections[h_ring.get_node(key)].send_entry(dict([(str(hashit(key)), value)])) == 200:
            i = i + 1

    print('Uploaded all %s entries.' %i)

    print('Verifying the data')
    for connection in connections.values():
        connection.get_entries()

file_name = 'causes-of-death.csv'
connections = {}
servers = ['http://localhost:5000','http://localhost:5001','http://localhost:5002', 'http://localhost:5003']

def csv_parser(file_name):
    reader = csv.DictReader(open(file_name))
    for line in reader:
        yield line['Year'] + line['Cause Name'] + line['State'] , line['Year'] +','+ line['113 Cause Name'] +','+ line['Cause Name'] +','+ line['State'] +','+ line['Deaths'] +','+ line['Age-adjusted Death Rate']

def hashit(val):
        i = md5(val.encode())
        return int(i.hexdigest(), 16)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    send_request()
