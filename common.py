'''
Created on 2012. 10. 12.

@author: Administrator
'''
'''
Created on 2012. 9. 27.

@author: Administrator
'''
import sys
import os
import datetime
import time
import ConfigParser
from ftplib import FTP
import re
import logging
import glob
import logging.handlers
import base64
import hashlib
import paramiko
import socket



class Decode():
    
    def _en(self,_in) : return base64.b64encode(_in) 
    
    def _de(self,_in) : return base64.b64decode(_in)
    
    def fenc(self,str):
        e0  = self._en(str)
        e1  = self._en(e0)
        m   = hashlib.md5(e0).hexdigest()
        e1 = e1.replace('=','@')
        e = e1 + '@' + m
        return e
    
    def fdec(self,e):
        r = e.rfind('@')
        if r == -1:
            pass
        d1  = e[:r]
        d1  = d1.replace('@','=')
        d0  = self._de( d1 );
        d   = self._de( d0 );
        return d

    def decBit(self,fileName):
        with open(fileName) as f:
            tmp = f.read()
        if re.search('###\*\*\*',tmp):
            return True
        else:
            return False
    
    def fileDec(self,fileName):
        if self.decBit(fileName):
            return None
        with open(fileName) as f:
            str = f.read()
        with open(fileName,'w') as f:
            f.write(self.fdec(str))
        return self.decBit(fileName)
    
    def fileDecReText(self,fileName):
        if self.decBit(fileName):
            with open(fileName) as f:
                reList = f.read()
            
        else:
            with open(fileName) as f:
                str = f.read()
            reList = self.fdec(str)
        
        return reList
        
    
    def fileEncDec(self,fileName):
        if self.decBit(fileName)==False:
            return None
        with open(fileName) as f:
            str = f.read()
        with open(fileName,'w') as f:
            f.write(self.fenc(str))
        return self.decBit(fileName)
    
    def getPasswd(self,passwd):
        
        pw=passwd
        if len(passwd) > 40:
            try:
                pw=self.fdec(passwd)
            except:
                pass
        else:
            pw=passwd
        return pw
    
    def setPasswd(self,passwd):
        if len(passwd) < 40:
            pw = self.fenc(passwd)
        else:
            pw = passwd
        return pw
    
        
class Common():
    def __init__(self):
        pass
    
    def getNow(self,format='%Y-%m-%d %H:%M:%S'):
        return time.strftime(format)

    def getHeadMsg(self,title='FLETA BATCH LAOD'):
        now = self.getNow()
        msg = '\n'
        msg += '#'*79+'\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#### '+('TITLE     : %s'%title).ljust(71)+'###\n'
        msg += '#### '+('DATA TIME : %s'%now).ljust(71)+'###\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#'*79+'\n'
        return msg
    
    
    def getEndMsg(self):
        now = self.getNow()
        msg = '\n'
        msg += '#'*79+'\n'
        msg += '####  '+('END  -  DATA TIME : %s'%now).ljust(70)+'###\n'
        msg += '#'*79+'\n'
        return msg


    def ipCheck(self,ip):
        if re.match('(?:\d{1,3}\.){3}\d{1,3}',ip):
            return True
        else:
            return False
    
    def portScan(self,ip,port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((ip, port))
            if result == 0:
                bit = True
            else:
                bit = False
            print 'IP : %s PORT : %s TCP SCAN : %s'%(ip,port,bit)
            sock.close()
        except:
            bit = False
        return bit
    
    
    
    
    
    def getLogger(self,msg,level='info'):    
        config = ConfigParser.RawConfigParser()
        cfgFile= os.path.join('config','fleta.cfg')
        config.read(cfgFile)
        try:
            logFile = config.get('fleta_log','log_name')
        except:
            logFile = 'fleta_agent_log.out'
            
        LOG_FILENAME = os.path.join('log',logFile)
        logger = logging.getLogger('fleta')
        handler = logging.handlers.RotatingFileHandler(
                   LOG_FILENAME, maxBytes=100000, backupCount=5)
        formatter = logging.Formatter('%(asctime)s : %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        if level == 'info':
            logger.info(msg)
        elif level =='error':
            logger.error(msg)
        elif level =='debug':
            logger.debug(msg)
        elif level == 'critical':
            logger.critical(msg)
        else:
            logger.info(msg)
        
        try:
            for i in logger.handlers:
                i.close()
            logger.handlers.remove(handler)
            logger.removeHandler(handler)        
        except:
            pass
    
    
    def flog(self,msg,level='info'):
        self.getLogger(msg, level)
    
    
    
            
            
            
    
    
    
    
def decodeTest():
    li=glob.glob('D:\\Fleta\\data\\diskinfo.SCH\\*.tmp')
#    for i in li:
#        decBit = Decode().decBit(i)
#        print i,decBit
#            
#        
#        if decBit :
#            print 'encoding...'
#            print Decode().fileEnc(i)
#        else:
#            print 'decoding...'
#            print Decode().fileDec(i)
    for i in li:
        print Decode().fileDecReText(i)




def logTest():
    common = Common()
    logger=common.flog()
    logger.info('log test')
    
    

if __name__=='__main__':
#    logTest()
#    fname=os.path.join('data','fs_ibk-test05.tmp')
#    print os.path.isfile(fname)
#    print Common().fletaPutFtp(fname,'diskinfo.SCH')
    logTest()


    en = Decode().fenc('PASSWORD')
    
    print en
    print len(en)
    en2 = Decode().fenc('2')
    print en2
    print len(en2)
    
    
    
