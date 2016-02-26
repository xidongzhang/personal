# coding=utf-8
'''
Created on 2013-9-22

@author: hanqunfeng
'''

import sys
sys.path.append('../') #导入上下文环境

from PairServer import Client
from thrift import Thrift
from ttypes import *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol

def pythonServerExe():
    try:
        #transport = TSocket.TSocket('10.8.12.204', 2041) 
        transport = TSocket.TSocket('10.8.12.31', 2040) 
        #transport = TSocket.TSocket('10.0.10.105', 9501) 
        #transport = TSocket.TSocket('172.16.1.24', 9501) 
        #transport = TTransport.TBufferedTransport(transport)
        transport = TTransport.TFramedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        #protocol = TCompactProtocol.TCompactProtocol(transport)
        client = Client(protocol)
        transport.open()

        pairRequest = PairServerRequest()
        
        pairRequest.uid = "0";
        pairRequest.device_id = "0";
        pairRequest.tid = int(sys.argv[1]);
        pairRequest.model_version = {1:"base", 2:"base"}

        csResponse=client.makePair(pairRequest)

        transport.close()
    except Thrift.TException, tx:
        print '%s' % (tx.message)
        
        
if __name__ == '__main__':
    pythonServerExe()
