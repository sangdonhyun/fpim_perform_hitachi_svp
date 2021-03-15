'''
Created on 2017. 7. 11.

@author: muse
SVP -> localhost
Redis

'''
import os
import ConfigParser
import glob
import zipfile
import shutil
import datetime
import time
import redis
#
import common
import random
import fletaDbms




class SvpLoad():
    """
    svp data load postgresql
    """
    def __init__(self):
        self.cfg = self.getCfg()
        # self.conn = redis.Redis('localhost')
        self.r=self.getRedis()
        self.latest_keys=[]
        self.serial = ""

    def getRedis(self):
        """
[redis]
host = 121.170.193.222
port = 6379
password = kes2719!

        """
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config', 'config.cfg')
        cfg.read(cfgFile)
        redis_ip = cfg.get('redis', 'host')
        redis_port = cfg.get('redis', 'port')
        redis_password = cfg.get('redis', 'password')
        r = redis.StrictRedis(host=redis_ip, port=int(redis_port), db=0, password=redis_password)
        return r

    def getCfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','config.cfg')
        cfg.read(cfgFile)
        return cfg
   
    def getFileList(self,dataDir='.\data'):
        monList=self.monList()
        listDir=[]
        mon_bit = self.cfg.get('common', 'mon_bit')
        print 'mon_bit :',mon_bit
        for (path, dir, files) in os.walk(dataDir):
            for filename in files:
                f = os.path.join(path, filename)
                if mon_bit == 'True':
                    if filename in monList:

                        listDir.append(f)
                else:
                    # print os.path.splitext(filename)[-1]
                    if os.path.splitext(filename)[-1] == '.csv':
                        listDir.append(f)
        return listDir

    
    def monList(self):
        monList=[]
        for mon in sorted(set(self.cfg.options('monitor'))):
            monList.append(self.cfg.get('monitor',mon))
        return monList
    
    def zipExtract(self):
        zlist=glob.glob(os.path.join('data','*'))
        print zlist
        
        for zname in zlist:
            print zname
            zip_ref = zipfile.ZipFile(zname, 'r')
            fname= os.path.basename(zname)
            dir = os.path.abspath(fname)
            print dir
            print fname
            zip_ref.extractall('data')
            zip_ref.close()

    def deletePath(self):
        print os.listdir(os.path.join('data'))
        for arg in os.listdir(os.path.join('data')):
            targetPath = os.path.join('data',arg)
            print arg,os.path.isdir(targetPath)
            if os.path.isdir(targetPath):
                shutil.rmtree(targetPath)   


    def getFlagNm(self,fileName):
        fname = os.path.basename(fileName)
        flag_nm = fname.split('.')[0]
        """
        mon01=PHY_Short_MP.csv
        mon02=PHY_Short_Write_Pending_Rate.csv
        mon03=Port_IOPS.csv
        mon04=Port_KBPS.csv
        mon05=Port_Response.csv
        """
        
        if fname=='PHY_Short_MP.csv' :
            flag_nm = 'MP'
        if fname=='PHY_Short_Write_Pending_Rate.csv' :
            flag_nm = 'WP'
        if fname=='Port_IOPS.csv' :
            flag_nm = 'PI'
        if fname=='Port_KBPS.csv' :
            flag_nm = 'PK'
        if fname=='Port_Response.csv' :
            flag_nm = 'PR'
            
        return flag_nm
    
    def insRedis(self,keyList):
        print self.latest_keys
        lkey_list=[]
        time_list=[]
        for keys in keyList:
            key,k,v= keys
            str_set = key.split('::')
            check_date = str_set[0]

            serial = str_set[1]
            if check_date not in time_list:
                time_list.append(check_date)
            print key,k,v
            self.r.hset(key,k,v)
        for time_str in time_list:
            print time_str
            check_date_time = datetime.datetime.strftime(datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S'),
                                                         '%Y-%m-%d %H')

            lkey = 'hitachi_latest_time_{SERIAL}_{DATETIME}'.format(SERIAL=self.serial,DATETIME=check_date_time)
            print lkey
            print time_str
            if not time_str in self.r.lrange(lkey,0,-1):
                self.r.lpush(lkey,time_str)
        print '-'*30
        print max(time_list)
        llkey = 'hitachi_latest_time_{SERIAL}'.format(SERIAL=self.serial)
        self.r.set(llkey,max(time_list))

        
        
    def makeCopy(self,insFile):
        with open(insFile) as f:
            tmp = f.read().splitlines()
        dataBit = False
        #flag_nm = self.getFlagNm(insFile)
        file_nm = os.path.basename(insFile)
        flag_nm = flag= os.path.splitext(file_nm)[0]
        if flag_nm not in self.r.lrange('hitachi_flag_list',0,-1):
            self.r.lpush('hitachi_flag_list',flag_nm)
        keyList=[]
        print len(tmp)

        print len(tmp) - 7
        delta_min = len(tmp) - 7
        for i in range(len(tmp)):
            line = tmp[i]
            if 'Serial number :' in line:
                serial = line.split(':')[-1]
                if '(' in serial:
                    serial=serial.split('(')[0].strip()
                serial = serial.zfill(20)
                self.serial=serial
                if self.serial not in self.r.lrange('hitachi_device_list',0,-1):
                    self.r.lpush('hitachi_device_list',self.serial)
            if 'To   :' in line:
                "2017/04/18 11:07"
                '%Y-%m-%d %H:%M:%S'
                datestr = line.split('o   :')[-1].strip()
                print datestr
                check_date = datetime.datetime.strptime(datestr, "%Y/%m/%d %H:%M")
                latest_time = datetime.datetime.strftime(check_date,'%Y-%m-%d %H')

            if ',' in line and '\"' in line:
                # print line
                line = line.replace('"','')
                if 'No.' in line:
                    keys = line.split(',')
                    dataBit = True
                if 'No.' not in line and dataBit:
                    vals = line.split(',')
                    for j in range(len(keys)):
                        k = keys[j].strip()
                        v = vals[j].strip()
                        if k == 'time':
                            #"2017/04/18 11:07"
                            check_date = datetime.datetime.strptime(v.strip(), "%Y/%m/%d %H:%M")
                            check_date = datetime.datetime.now() - datetime.timedelta(minutes=delta_min)
                            delta_min = delta_min - 1
                            check_date_str = datetime.datetime.strftime(check_date,'%Y-%m-%d %H:%M:00')

                        if k == 'No.' or k == 'time':
                            pass
                        else:

                            key="{CHECK_DATE}::{SERIAL}::{FLAG}".format(CHECK_DATE=check_date_str,SERIAL=serial,FLAG=flag_nm)
                            print key,k,v
                            if 'RATE' in flag_nm.upper():
                                v = random.randrange(0, 99)
                            else:
                                v=random.randrange(-3,1500)
                            keyList.append((key,k,v))
                            #lineList.append(str)

        return keyList

    def set_flag(self,flag):
        self.r.lrange('hitachi_flag_list')
    def load(self):
        self.deletePath()
        self.zipExtract()
        fList= self.getFileList()
        print 'flie List:',fList
        for f in fList:
            keyList=self.makeCopy(f)

            self.insRedis(keyList)
           

class SvpInfo():
    """
    get svp info
    """
    def __init__(self,svp):
        self.svp=svp
        self.cfg = self.getCfg()
        self.setClasspath()
    
    def getCfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','config.cfg')
        cfg.read(cfgFile)
        return cfg
    
    def getCmd(self):
        cmdList=[]
        for c in sorted(set(self.cfg.options('command'))):
            cmd=self.cfg.get('command',c)
            print cmdList.append(cmd)
        return cmdList
    
    def setClasspath(self):
        jlist= glob.glob(os.path.join('.','lib','*.jar'))
        cpath= ';'.join(jlist)

        env = os.environ    
        if 'CLASSPATH' not in env.keys():
            env['CLASSPATH'] = cpath 

    def makeCfg(self):
        """ cfgfile sample
svpip 40.10.12.199
login 40.10.12.199 "40.10.12.199"
show
group PhyProc                   ; Micro-Processor usage
group PhyESW                    ; Access Paths and Write Pending
group Port                      ; Port usage
shortrange  -0005:              ; Number of monitored CUs is 64 or less
longrange -000001:
outpath out                     ; Specifies the sub-directory in which files will be saved
option compress                 ; Specifies whether to compress files
apply                           ; Executes processing for saving monitoring data in files
"""
        name= self.svp['name']
        cfgFile = os.path.join('.','cfg','%s.txt'%name)
        print os.path.isfile(cfgFile)
        if not os.path.isfile(cfgFile):
            with open(cfgFile,'w') as f:
                f.write('svpip %s\n'%self.svp['ip'])
                f.write('login %s "%s" \n'%(self.svp['user'],self.svp['passwd']))
                f.write('show \n')
                f.write('group PhyProc\n')
                f.write('group PhyESW\n')
                f.write('group Port\n')
                f.write('shortrange  -0005:\n')  # ; Number of monitored CUs is 64 or less
                f.write('longrange -000001:\n')
                f.write('outpath data\n')
                f.write('option compress\n')
                f.write('apply\n')
        return cfgFile
    
    def deletePath(self):
        print os.listdir(os.path.join('data'))
        for arg in os.listdir(os.path.join('data')):
            targetPath = os.path.join('data',arg)
            print arg,os.path.isdir(targetPath)
            if os.path.isdir(targetPath):
                shutil.rmtree(targetPath)
     
    def run(self):
        print self.svp
        cfgFile = self.makeCfg()
        cmdList=self.getCmd()
        for cmd in cmdList:
            print cmd
            if '<CFGFILE>' in cmd:
                cmd = cmd.replace('<CFGFILE>',cfgFile)
            print cmd
            print 'wait a few minite'
            print os.popen(cmd).read()

        SvpLoad().load()
    
    
class Manager():
    """
    manager ./config/list.cfg set
    """
    def __init__(self):
        
        self.listcfg = self.getListCfg()
        self.com = common.Common()
        
    
    def getListCfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','list.cfg')
        cfg.read(cfgFile)
        return cfg
        
    
    def getList(self):
        sList= []
        for sec in self.listcfg.sections():
            s={}
            s['name'] = sec
            for opt in self.listcfg.options(sec):
                s[opt] = self.listcfg.get(sec,opt)
            sList.append(s)
        return sList
    
    def main(self):
        # STEP 1 : svc list
        slist = self.getList()
        for s in slist:
            print s
            SvpInfo(s).run()


if __name__=='__main__':
    while True:
        Manager().main()
        time.sleep(60*5)
