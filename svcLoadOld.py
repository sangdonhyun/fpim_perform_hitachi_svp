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



class SvcPerformLoad():
    def __init__(self,fname):
        self.fname = fname
        self.db = fletaDbms.FletaDb()

    def getTargetDate(self,format):
        today= datetime.datetime.today()
        ago = today - datetime.timedelta(time=3)
        #format '%Y-%m-%d'
        return ago.strftime(format)


    
    
    def qrirte(self,msg,wbit='a'):
        with open('query.txt',wbit) as f:
            f.write(msg+'\n')
    
        
        
    
    def getFlagNm(self):
#         if 'MP' in self.fname:
#             flag_nm = 'MP'
#         elif 'Write_Pending_' in self.fname:
#             flag_nm = 'WP'
#         elif 'IOPS' in self.fname:
#             flag_nm = 'PI'
#         elif 'KBPS' in self.fname:
#             flag_nm = 'PK'
#         elif 'Response' in self.fname:
#             flag_nm = 'PR'
#         else:
#             flag_nm = 'unKowon'
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
        if fname=='PHY_Short_Write_Pending_Rate.csv' :
            flag_nm = 'WP'
        if fname=='Port_IOPS.csv' :
            flag_nm = 'PI'
        if fname=='Port_KBPS.csv' :
            flag_nm = 'PK'
        if fname=='Port_Response.csv' :
            flag_nm = 'PR'
            
        return flag_nm
    
    def getQuery(self,data):
        query = "INSERT INTO monitor.pm_auto_hitachi_real_info("#check_date, ctrl_unum, flag_nm, cols_nm, cols_value)VALUES (?, ?, ?, ?, ?);"
        query = query + ','.join(data.keys())
        query = query + ') VALUES (\''
        query = query + "','".join(data.values())
        
#         for val in data.values():
#             query = query + val +'\',\''
        query = query + '\');'
        return query
    
    def execute(self,cmd) :
        fd = subprocess.Popen(cmd, shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        ret = fd.stdout.read()
        print ret
        return ret
    def fwrite(self,msg,wbit = 'a'):
        with open('query.txt',wbit) as f:
            f.write(msg)
    def run(self):
        with open(self.fname) as f:
            tmp = f.read().splitlines()

        
        queryStr="COPY monitor.pm_auto_hitachi_real_info (check_date, ctrl_unum, flag_nm, cols_nm, cols_value) FROM stdin;\n"
        self.fwrite(queryStr,'w')
        print queryStr
        dataBit = False
        flag_nm = self.getFlagNm()
        with open('query.txt','w') as f:
            f.write(queryStr)
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
    #                     print keys
                        print vals
            
                        
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
    #                             print str
                                f.write(str)
            f.write('\.\n')
        print '-'*50
        cmd = "psql -U fletaAdmin fleta < query.txt"
        print self.execute(cmd)
        
            
        
    def timeDelta(self):
        dir= 'C:\\Fleta\\data\\Perform.Isilon.SCH.MV1'
        sList = glob.glob(os.path.join(dir,'*'))
        dd= datetime.datetime.now() + datetime.timedelta(hours=-3)
        cd= dd.strftime('%Y-%m-%d %H:%M:%S')
        print 
        
class AVG():
    def __init__(self):
        self.db = fletaDbms.FletaDb()
        self.com = common.Common()
    
    def getDailyTable(self):
        tname = 'pm_auto_hitachi_real_info_avg_%s'%self.com.getNow('%Y%m%d')
        query = "select count(*) from pg_tables where tablename = '%s' and schemaname = 'monitor'"%tname
        rows=self.db.getRaw(query)
        
        
        print len(rows),len(rows) == 0,rows[0],type(rows[0])
        
        if rows[0][0] == 0 :
            cquery="""
CREATE TABLE monitor.%s
(
  check_date character varying(30) NOT NULL,
  ctrl_unum character varying(100) NOT NULL,
  flag_nm character varying(50) NOT NULL,
  cols_nm character varying(100) NOT NULL,
  cols_value_max character varying(100) NOT NULL,
  cols_value_max_datetime character varying(100) NOT NULL,
  cols_value_avg character varying(100) NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE monitor.%s
  OWNER TO "fletaAdmin";
            """%(tname,tname)
            print query
            print cquery
            with open('cquery.txt','w') as f:
                f.write(cquery)
            cmd = "psql -U fletaAdmin fleta < cquery.txt"
            print os.popen(cmd).read()
            
            
            
        print query
        rows=self.db.getRaw(query)
        print rows,rows[0][0],rows[0][0] == 1
        
        if rows[0][0] == 1:
            print 'table exist : %s'%tname
        else:
            print 'table not exist'
            sys.exit()
        return tname
        
#         print os.popen('psql -U fletaAdmin -c "%s" fleta '%query).read()
    def delQuery(self):
        query  ="delete from "
    
    def avg(self):
        query  ="""
insert into monitor.pm_auto_hitachi_real_info_avg
select substr(check_date,1,13),ctrl_unum,flag_nm,cols_nm,max(cols_value),round(avg(cols_value),2)
 from (
SELECT check_date,ctrl_unum,flag_nm, cols_nm,
            case when cols_value ~ '[0-9]' THEN CAST(cols_value AS decimal) end as cols_value 
            
            FROM monitor.pm_auto_hitachi_real_info
) aa 
group by ctrl_unum,flag_nm,cols_nm,substr(check_date,1,13)
        """
        
    def upset(self):
        query ="""
WITH s AS (
       select substr(check_date,1,13) check_date,ctrl_unum,flag_nm,cols_nm,to_char(max(cols_value),'999999999') cols_value_max,to_char(round(avg(cols_value),2),'999999999') cols_value_avg,max(check_date) cols_value_max_datetime
     from (
    SELECT check_date,ctrl_unum,flag_nm, cols_nm,
            case when cols_value ~ '[0-9]' THEN CAST(cols_value AS decimal) end as cols_value 
            
            FROM monitor.pm_auto_hitachi_real_info
    ) aa 
    group by ctrl_unum,flag_nm,cols_nm,substr(check_date,1,13)
),
upd AS (
     UPDATE monitor.pm_auto_hitachi_real_info_avg
     SET cols_value_avg=s.cols_value_avg,
     cols_value_max=s.cols_value_max
     FROM   s
     WHERE      monitor.pm_auto_hitachi_real_info_avg.check_date = s.check_date
            and monitor.pm_auto_hitachi_real_info_avg.ctrl_unum  = s.ctrl_unum
            and monitor.pm_auto_hitachi_real_info_avg.flag_nm    = s.flag_nm
            and monitor.pm_auto_hitachi_real_info_avg.cols_nm    = s.cols_nm
     RETURNING monitor.pm_auto_hitachi_real_info_avg.*
)
INSERT INTO monitor.pm_auto_hitachi_real_info_avg 
SELECT check_date,
       ctrl_unum,
       flag_nm,
       cols_nm,
       cols_value_max,
       cols_value_avg
FROM   s
WHERE  NOT EXISTS (SELECT check_date,ctrl_unum,flag_nm,cols_nm FROM monitor.pm_auto_hitachi_real_info_avg r where s.check_date=r.check_date and s.ctrl_unum=r.ctrl_unum and s.flag_nm=r.flag_nm and s.cols_nm = r.cols_nm )
            
        """


    def daily(self,tname):
        query ="""
WITH s AS (
       select DD.*, (select check_date from monitor.pm_auto_hitachi_real_info where substr(check_date,1,13) = DD.check_date and ctrl_unum = DD.ctrl_unum and flag_nm = DD.flag_nm and cols_nm = DD.cols_nm and to_number(cols_value,'99999999999') = to_number(DD.cols_value_max,'999999999999') Limit 1)  cols_value_max_datetime
from (

  select substr(check_date,1,13) check_date,ctrl_unum,flag_nm,cols_nm,cast(max(cols_value)as text) cols_value_max,cast(round(avg(cols_value),2)as text) cols_value_avg


  
     from (
    SELECT check_date,ctrl_unum,flag_nm, cols_nm,
            case when cols_value ~ '[0-9]' THEN CAST(cols_value AS decimal) end as cols_value 
            
            FROM monitor.pm_auto_hitachi_real_info
    ) aa 
    group by ctrl_unum,flag_nm,cols_nm,substr(check_date,1,13)

    ) DD
),
upd AS (
     UPDATE monitor.%s c
     SET cols_value_avg=s.cols_value_avg,
     cols_value_max=s.cols_value_max
     FROM   s
     WHERE  c.cols_value_max    = s.cols_value_max
            and c.cols_value_avg    = s.cols_value_avg
            and c.cols_value_max_datetime    = s.cols_value_max_datetime
     RETURNING *
)
INSERT INTO monitor.%s 
SELECT check_date,
       ctrl_unum,
       flag_nm,
       cols_nm,
       cols_value_max,
       cols_value_max_datetime,
       cols_value_avg
FROM   s
WHERE  NOT EXISTS (SELECT check_date,ctrl_unum,flag_nm,cols_nm FROM monitor.%s r where s.check_date=r.check_date and s.ctrl_unum=r.ctrl_unum and s.flag_nm=r.flag_nm and s.cols_nm = r.cols_nm )
  
        """%(tname,tname,tname)
        print query
        with open('dquery.txt','w') as f:
            f.write(query)
        cmd = "psql -U fletaAdmin fleta < dquery.txt"
        print 'daily query'
        print cmd
        print os.popen(cmd).read()
    
    
    
    
    def main(self):
        tname = self.getDailyTable()
        self.daily(tname)
        
        
        
    
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
                if filename in monList:
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
            SvcPerformLoad(s).run()
            cnt = cnt+1
            print t - t1
            t = t1
        t2 = time.time()    
        print to - t2
        self.deletePath()
        self.backup()
        
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
        
        # STEP 1 : svc list
        slist = self.getList()
        cmdList=self.getCmd()
        
        for s in slist:
            print s['name']
            name = s['name']
            cfgFile = os.path.join('.','cfg','%s.txt'%name)
            print cfgFile,os.path.isfile(cfgFile)
            if not os.path.isfile(cfgFile):
                self.makeCfg(s)
            cmd = cmdList[0]
            if '<CFGFILE>' in cmd:
                cmd = cmd.replace('<CFGFILE>',cfgFile)
            print cmd
            
            print os.popen(cmd).read()
            time.sleep(1)
#             os.popen('move *.ZIP .\\data')
            self.run()
            # step 3 - daily table insert
#             AVG().main()
            
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
    