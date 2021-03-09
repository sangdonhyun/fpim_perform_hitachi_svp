'''
Created on 2016. 8. 8.

@author: muse
'''
import glob
import os
import sys
import fletaDbms
import time
import datetime
import subprocess
import ConfigParser
import zipfile
import common
import shutil
import redis



class SvcPerformLoad():
    def __init__(self,fname):
        self.fname = fname
        # self.conn = redis.Redis('localhost')
        self.r  = self.getRedis()

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
    def getTargetDate(self,format):
        today= datetime.datetime.today()
        ago = today - datetime.timedelta(time=3)
        #format '%Y-%m-%d'
        return ago.strftime(format)


    
    
    def qrirte(self,msg,wbit='a'):
        with open('query.txt',wbit) as f:
            f.write(msg+'\n')
    
        
        
    
    def getFlagNm(self):

        fname = os.path.basename(self.fname)
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
        elif fname=='PHY_Short_Write_Pending_Rate.csv' :
            flag_nm = 'WP'
        elif fname=='Port_IOPS.csv' :
            flag_nm = 'PI'
        elif fname=='Port_KBPS.csv' :
            flag_nm = 'PK'
        elif fname=='Port_Response.csv' :
            flag_nm = 'PR'
        else:
            flag_nm = fname.split('.')[0]  
        return flag_nm
    
    
    def execute(self,cmd) :
        fd = subprocess.Popen(cmd, shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        ret = fd.stdout.read()
        print ret
        return ret
  
    def getData(self):
        with open(self.fname) as f:
            tmp = f.read().splitlines()
        
        dataList=[]
        
        dataBit = False
        flag_nm = self.getFlagNm()
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
                            check_date = datetime.datetime.strptime(v.strip(), "%Y/%m/%d %H:%M")
                            
                        if k == 'No.' or k == 'time':
                            pass
                        else:
                            str= "%s\t%s\t%s\t%s\t%s\n"%(check_date.strftime('%Y-%m-%d %H:%M'),serial,flag_nm,k,v)
                            performData = {}
                            performData['ctrl_unum'] = serial
                            performData['flag_nm'] = flag_nm
                            performData['check_date'] = check_date.strftime('%Y-%m-%d %H:%M:%S')
                            performData['cols_nm'] = k
                            performData['cols_value'] = v
                            dataList.append(performData)
                            
        return dataList
        
        
        
        
            
        
    def timeDelta(self):
        dir= 'C:\\Fleta\\data\\Perform.Isilon.SCH.MV1'
        sList = glob.glob(os.path.join(dir,'*'))
        dd= datetime.datetime.now() + datetime.timedelta(hours=-3)
        cd= dd.strftime('%Y-%m-%d %H:%M:%S')
        print 

    def main(self):
        
        dlist =self.getData()
        for d in dlist:
            key = d['ctrl_unum']+'.'+d['flag_nm']+'.'+d['cols_nm']
            print key
#             self.conn.zadd(key, d["check_date"], d["cols_value"])
            self.r.hset(key, d["check_date"], d["cols_value"])
            self.r.expire(key, datetime.timedelta(days=2))
        
        
    
class Manager():
    def __init__(self):
        self.cfg = self.getCfg()
        self.listcfg = self.getListCfg()
        self.com = common.Common()
        self.fletaPath = self.cfg.get('common','fleta_path')
        self.setClasspath()
    
    
    def getListCfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','list.cfg')
        cfg.read(cfgFile)
        return cfg
        
    def getCfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','config.cfg')
        cfg.read(cfgFile)
        return cfg
    
    def monList(self):
        monList=[]
        for mon in sorted(set(self.cfg.options('monitor'))):
            monList.append(self.cfg.get('monitor',mon))
        
        return monList
        
    
    def getFileList(self,dataDir):
        monList=self.monList()
        listDir=[]
        for (path, dir, files) in os.walk(dataDir):
            for filename in files:
#                 if filename in monList:
                f=os.path.join(path,filename)
                listDir.append(f)
        return listDir
    
    def deletePath(self):
        print os.listdir(os.path.join('data'))
        for arg in os.listdir(os.path.join('data')):
            targetPath = os.path.join('data',arg)
            print arg,os.path.isdir(targetPath)
            if os.path.isdir(targetPath):
                shutil.rmtree(targetPath)
#             os.removedirs(targetPath)
    def backup(self):
        now = self.com.getNow('%Y%m%d%H%M%S')
        backuppath = os.path.join('backup',now)
        if not os.path.isdir(backuppath):
            if not os.path.isdir('backup'):
                os.mkdir('backup')
            os.mkdir(backuppath)
        try:
            self.deletePath()
        except:
            pass
        blist=glob.glob(os.path.join('data','*'))
        for b in blist:
            nname =os.path.join(backuppath,os.path.basename(b)) 
            os.rename(b, nname)
    
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
        
    
    
    
    
    def run(self):
        
        targetDir = os.path.join('.','data')
        print targetDir
#         targetDir = 'C:\\Fleta\\data\\Perform.Real.Hitachi.tmp\\Port_dat'
#         print self.getFileList(targetDir)
        self.zipExtract()
        sList = self.getFileList(targetDir)
        tot = len(sList)
        
#         sList = ['C:\\Fleta\\data\\Perform.Real.Hitachi.tmp\\Port_dat\\Port_Response.csv'] 
         
        cnt = 1
        t = time.time()
        to = t 
        for s in sList:
            t1 = time.time()
            print s,'(%s/%s)'%(cnt,tot)
            SvcPerformLoad(s).main()
            cnt = cnt+1
            print t - t1
            t = t1
        t2 = time.time()    
        print to - t2
        self.deletePath()
#         self.backup()
        
    def getList(self):
        sList= []
        for sec in self.listcfg.sections():
            s={}
            s['name'] = sec
            for opt in self.listcfg.options(sec):
                s[opt] = self.listcfg.get(sec,opt)
            sList.append(s)
        return sList
    
    def makeCfg(self,s):
        ip = s['ip']
        user = s['ip']
        pw = s['ip']
        msg="""svpip %s
login %s "%s"
show
group PhyProc                   ; Micro-Processor usage
group PhyESW                    ; Access Paths and Write Pending
group Port                      ; Port usage
shortrange  -0005:              ; Number of monitored CUs is 64 or less
longrange -000001:
outpath out                     ; Specifies the sub-directory in which files will be saved
option compress                 ; Specifies whether to compress files
apply                           ; Executes processing for saving monitoring data in files
"""%(ip,user,pw)

        cfgname = os.path.join('cfg','%s.txt'%s['name'])
        
        with open(cfgname,'w') as f:
            f.write(msg)
            
    
    
   
    
    
    
    def main(self):
        slist = self.getList()
        cmdList=self.getCmd()
        
        for s in slist:
            print s['name']
            name = s['name']
            cfgFile = os.path.join('.','cfg','%s.txt'%name)
            print cfgFile,os.path.isfile(cfgFile)
            if not os.path.isfile(cfgFile):
                self.makecfg(s)
            cmd = cmdList[0]
            if '<CFGFILE>' in cmd:
                cmd = cmd.replace('<CFGFILE>',cfgFile)
            print cmd
            
            print os.popen(cmd).read()
            time.sleep(1)
            os.popen('move *.ZIP .\\data')
            self.run()
            # step 3 - daily table insert
            
            
    def getCmd(self):
        cmdList=[]
        for c in self.cfg.options('command'):
            cmd=self.cfg.get('command',c)
            print cmdList.append(cmd)
        return cmdList
    def setClasspath(self):
        jlist= glob.glob(os.path.join('.','lib','*.jar'))
        cpath= ';'.join(jlist)

        env = os.environ    
        if 'CLASSPATH' not in env.keys():
            env['CLASSPATH'] = cpath 
        
if __name__=='__main__':
#     Manager().run()
    Manager().main()
    