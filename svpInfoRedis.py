'''
Created on 2017. 6. 30.

@author: muse
'''
import random
import redis
import datetime
import time
import os

import fletaDbms
import common
class RedisDaily():
    
    def __init__(self):

        self.conn = redis.Redis('localhost')

        host='localhost'
        self.redis_client = redis.StrictRedis(host)
        
    def getKeys(self):
        
        return  self.redis_client.execute_command('keys *')
    
    
    def getTableName(self,date):
        return 'pm_auto_hitachi_real_info_%s'%date

    
    def getAvg(self,rList,tt):
        
        for t in tt:
            vList=[]
            for r in rList:
                if r[1] == t:
                    vList.append(int(r[-1]))
            rList.append(vList)
              
            print vList
            return len(rList),max(vList),round(sum(vList) / float(len(vList)),2)

    def insQuery(self,dateList,lineList):
        date = dateList[0]
        if '-' in date:
            date = date.replace('-','')
        tableName=self.getTableName(date)
        with open('insQuery.txt' ,'w') as fw:
            fw.write("COPY monitor.%s (check_date, ctrl_unum, flag_nm, cols_nm, cols_value) FROM stdin;\n"%tableName)
            for line in lineList:
                fw.write('\t'.join(line)+'\n')
            fw.write('\.\n')
            
        with open('insQuery.txt') as f:
            print f.read()
        
        cmd = "psql -U fletaAdmin fleta < insQuery.txt"
        print cmd
        print os.popen(cmd).read()
    
    def getRedis(self):
        klist = self.getKeys()
        print klist
        dateList=[]
        lineList=[]
        for key in klist:
            for check_date in self.redis_client.hgetall(key):
                
                cols_value =self.redis_client.hget(key,check_date)
                tmp= key.split('.')
                #check_date, ctrl_unum, flag_nm, cols_nm, cols_value
                ctrl_unum=tmp[0]
                flag_nm = tmp[1]
                cols_nm = '.'.join(tmp[2:])
                print '-'*50
                print  ctrl_unum,flag_nm, cols_nm
                print check_date
                cdate =  check_date.split()[0]
                if cdate not in dateList:
                    dateList.append(cdate)
                lineList.append([check_date, ctrl_unum, flag_nm, cols_nm, cols_value])
        return dateList,lineList
    
    def main(self):
        t1 = time.clock()
                
        dateList,lineList=self.getRedis()        
        
        if len(dateList) ==1:
            self.insQuery(dateList,lineList)
        t2 = time.clock()
            
        print 'END -'
        t2 = time.clock()
        print t2-t1
        
            
        
                    
                    
if __name__=='__main__':
    RedisDaily().main()
                        