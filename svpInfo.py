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
import fletaDbms




class SvpLoad():
    """
    svp data load postgresql
    """
    def __init__(self):
        self.cfg = self.getCfg()
        # self.conn = redis.Redis('localhost')
        self.r=self.getRedis()

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
        for (path, dir, files) in os.walk(dataDir):
            for filename in files:
                if filename in monList:
                    f=os.path.join(path,filename)
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
    
    def insRedis(self,lineList):
        for line in lineList:
            check_date, ctrl_unum, flag_nm, cols_nm, cols_value= line.strip().split('\t')
            key = ctrl_unum+'.'+flag_nm+'.'+cols_nm
            print key,check_date,cols_value
            self.r.hset(key, check_date, cols_value)
            self.r.expire(key, datetime.timedelta(days=2))
            
        
        
    def makeCopy(self,insFile):
        with open(insFile) as f:
            tmp = f.read().splitlines()
        dataBit = False
        flag_nm = self.getFlagNm(insFile)
        lineList=[]
        for i in range(len(tmp)):
            line = tmp[i]
            if 'Serial number :' in line:
                serial = line.split(':')[-1]
                if '(' in serial:
                    serial=serial.split('(')[0].strip()
            if 'To   :' in line:
                "2017/04/18 11:07"
                '%Y-%m-%d %H:%M:%S'
                datestr = line.split('o   :')[-1].strip()
                print datestr
                check_date = datetime.datetime.strptime(datestr, "%Y/%m/%d %H:%M")
            if ',' in line and '\"' in line:
                print line
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
                        if k == 'No.' or k == 'time':
                            pass
                        else:
                            str= "%s\t%s\t%s\t%s\t%s\n"%(check_date.strftime('%Y-%m-%d %H:%M'),serial,flag_nm,k,v)
                            print str
                            lineList.append(str)
        return lineList
    
    def load(self):
        self.deletePath()
        self.zipExtract()
        fList= self.getFileList()
        for f in fList:
            print f
            lineList=self.makeCopy(f)
            self.insRedis(lineList)
           

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
    Manager().main()
