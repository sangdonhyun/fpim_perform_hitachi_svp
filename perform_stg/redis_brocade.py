'''
Created on 2019. 12. 6.

@author: user
'''
import redis
import os
import time
import datetime
import fleta_dbms
import ConfigParser
print 'redis '

class Avg():
    def __init__(self):
        self.serial=''
        # r=redis.Redis(host='121.170.193.222', port=6379)
        self.r = self.getRedis()
        # redis_db.lpush('test','1,2,3,4,5')

        self.now=datetime.datetime.now().strftime('%Y-%m-%d %H')
        self.hour=self.now[-1:-2]
        self.rangemin=''
        self.portnumList=None
        self.rdb=fleta_dbms.FletaDb()
        self.pdb=fleta_dbms.FpimDb()
        self.todaty = datetime.datetime.now().strftime('%Y%m%d')
        
    
    def getRedis(self):
        """
                [redis]
host = 121.170.193.222
port = 6379
password = kes2719!

        """
        cfg=ConfigParser.RawConfigParser()
        cfgFile=os.path.join('config','config.cfg')
        cfg.read(cfgFile)
        redis_ip=cfg.get('redis','host')
        redis_port=cfg.get('redis','port')
        redis_password=cfg.get('redis','password')
        r=redis.StrictRedis(host=redis_ip, port=int(redis_port), db=0, password=redis_password)
        return r


    def getAvg(self,datehour,target):
        portVal={}
        """
        lrange "brocade_latest_time_000000000ALJ2503G08G_2019-12-09 10" 0 -1
        
        b121.170.193.222:6379> keys "2019-12-06 13:04:34::000000000ALJ2503G08G:*"
        1) "2019-12-06 13:04:34::000000000ALJ2503G08G::SFP_RX_POWOR::uWatts"
        2) "2019-12-06 13:04:34::000000000ALJ2503G08G::CRC::count"
        3) "2019-12-06 13:04:34::000000000ALJ2503G08G::throughput::byte"
        4) "2019-12-06 13:04:34::000000000ALJ2503G08G::SFP_TX_POWOR::uWatts"
        """
        lastkey='brocade_latest_time_{}_{}'.format(self.serial,datehour)
        print lastkey
#         serial=lastkey.split('_')[3]
        timelist= self.r.lrange(lastkey, 0, -1)
        #print timelist
        
        htot,mtot=0,0
        timelist
        rangeMinList=[]
        
        for mtime in timelist:

#             print mtime
            key='{}::{}::{}'.format(mtime,self.serial,target)
            txInfo= self.r.hgetall(key)
            
            keys=txInfo.keys()
            #print keys
            #print self.portnumList
            #if self.portnumList == [] or self.portnumList==None:
            #    self.portnumList=keys
            self.portnumList=keys
            #print self.portnumList
            vals=txInfo.values()
            rangemin=mtime[:-3]
            datemin=mtime[:-3]
            
            #print self.portnumList
                
            for i in range(len(vals)):
                val=vals[i]
                portnum=self.portnumList[i]
                if portnum not in portVal.keys():
                    portVal[portnum] = []  
                portVal[portnum] = portVal[portnum]+[((mtime,float(val)))]
                htot=htot+1
        print 'htot :',htot
        
        return portVal
        
    def main(self):
        """
        1) "SFP_TX_POWOR"
2) "SFP_RX_POWOR"
3) "CRC"
4) "throughput"
        """
        
        while True:
            dnow=datetime.datetime.now().strftime('%Y-%m-%d %H')
            if not dnow==self.now:
                flagList=self.getFlagList()
                for flag in flagList:
                    self.run(flag)
            self.now=dnow
            time.sleep(60)
    def minAvg(self,minInfoList,target,portnum):

        dicList=[]
        for minInfo in minInfoList:
            keys=minInfo.keys()
            vals=minInfo.values()
            if len(vals) > 0:
                val_avg= round(sum(vals,0.0)/len(vals),2)
                val_max= round(max(vals),2)
                val_max_index = vals.index(val_max)
                val_max_date=keys[val_max_index]
                val_min= round(min(vals),2)
                val_min_index = vals.index(val_min)
                val_min_date=keys[val_min_index]
                mdate= keys[0][:-1]+'0'
                """
                ins_date, check_date, device_type, flag_1, 
               cols_max_date, cols_value_max, cols_value_avg, mdb_key
               
                """
                minDic={}
                minDic['ins_date'] = mdate[:10]
                minDic['check_date'] = mdate [:15]+'0'
                minDic['device_type'] = 'brocade'
                minDic['flag_1'] = self.serial
                minDic['flag_2'] = target
                minDic['flag_3'] = ''
                minDic['flag_4'] = portnum
                minDic['cols_max_date'] = val_max_date[-8:]
                if 'SFP' in target:
                    minDic['cols_value_max'] = val_min
                else:
                    minDic['cols_value_max'] = val_max
                minDic['cols_value_avg'] = val_avg
                minDic['mdb_key'] = '{}::{}'.format(self.serial,target)
                dicList.append(minDic)
        day_table = datetime.datetime.strptime(minDic['ins_date'],'%Y-%m-%d').strftime('y%Ym%md%d')
        #self.pdb.dbListInsert(dicList, 'monitor.perform_stg_{}'.format(day_table))
        return dicList


    def getFlagList(self):
        flagList=self.r.lrange('brocade_flag_list',0,-1)
        return  flagList

    def set_avg_day(self, day_str):
        query = """delete FROM monitor.perform_stg psymd WHERE ins_date = '{}' AND device_type='brocade' AND char_length(check_date) = '10'  
        """.format(day_str)
        self.pdb.queryExec(query)

        query = """
INSERT INTO  monitor.perform_stg (ins_date,check_date,device_type,flag_1,flag_2,flag_3,flag_4,cols_value_max,cols_value_avg,cols_max_date,mdb_key) 
SELECT ins_date,ins_date as check_date,device_type ,flag_1, flag_2,'' AS flag_3, flag_4,max(cols_value_max) cols_value_max ,avg(cols_value_avg::float) cols_value_avg  ,max(cols_max_date) AS cols_max_date,mdb_key
    FROM monitor.perform_stg psymd WHERE ins_date = '{}' AND device_type='brocade' AND char_length(check_date) = '13' 
    GROUP BY ins_date,device_type ,flag_1, flag_2, flag_4 ,mdb_key
            """.format(day_str)
        print query

        self.pdb.queryExec(query)



    def redis_set(self):
        self.hour_dict_list=[]
        print datetime.datetime.now()
        self.r.lrange('brocade_device_list',0,-1)
        device_set=self.r.lrange('brocade_device_list',0,-1)
        dicList=[]
        tot = len(device_set)
        cnt = 1
        #print device_set
        for serial in device_set:
            print 'count :',cnt,'/',tot
            cnt = cnt +1
            self.serial = serial
            print '*'*50
            flagList=self.getFlagList()
            #print flagList
            for flag in flagList:
                #print flag
                dicList = dicList + self.run(flag)
        day_table = datetime.datetime.strptime(dicList[0]['ins_date'],'%Y-%m-%d').strftime('y%Ym%md%d')
        self.pdb.dbListInsert(dicList, 'monitor.perform_stg_{}'.format(day_table))
        self.pdb.dbListInsert(self.hour_dict_list, 'monitor.perform_stg_{}'.format(day_table))
        print datetime.datetime.now()
    def run(self,flag):
        

        check_date=datetime.datetime.now() - datetime.timedelta(hours=1)


        check_date= check_date.strftime('%Y-%m-%d %H')

        print check_date
        portVal= self.getAvg(check_date,flag)
        
        #print portVal
        dic_list=[]
        portnumlist=portVal.keys()
        #print 'portnumlist :',portnumlist
        dicList=[]
        minInfo0,minInfo1,minInfo2,minInfo3,minInfo4,minInfo5={},{},{},{},{},{}
        for i in range(len(portVal.values())):
            val_avg,val_max,val_min,val_max_date,val_min_date = None,None,None,None,None
            keys,vals=[],[]
            portnum = portnumlist[i]
#             print 'PORT Index :',portnum
            portDevVal= portVal[portnum]
#             print portDevVal
            for j in range(len(portDevVal)):
                
                key,val=portDevVal[j]
                minchr=key[14:15]
                if minchr=='0':
                    minInfo0[key]=val
                elif minchr=='1':
                    minInfo1[key]=val
                elif minchr=='2':
                    minInfo2[key]=val
                elif minchr=='3':
                    minInfo3[key]=val
                elif minchr=='4':
                    minInfo4[key]=val
                elif minchr=='5':
                    minInfo5[key]=val
                else:
                    pass
                
                keys.append(key)
                vals.append(val)
            val_avg= round(sum(vals,0.0)/len(vals),2)
            val_max= round(max(vals),2)
            val_max_index = vals.index(val_max)
            val_max_date=keys[val_max_index]
            val_min= round(min(vals),2)
            val_min_index = vals.index(val_min)
            val_min_date=keys[val_min_index]
            if val_max < val_avg:
                print 'avg:',val_avg,'max:',val_max,val_max_date

                
                
            """
            ins_date, check_date, device_type, flag_1, flag_2,  flag_3,  flag_4,  
            cols_max_date, cols_value_max, cols_value_avg, mdb_key
            """
            
#             print check_date,portnum,val_avg,val_max,val_min,val_max_date,val_min_date
#             print minInfo0
            hourDic={}
            hourDic['ins_date'] = check_date[:10]
            hourDic['check_date'] = check_date[:13]
            hourDic['device_type'] = 'brocade'
            hourDic['flag_1'] = self.serial
            hourDic['flag_2'] = flag
            hourDic['flag_3'] = ''
            hourDic['flag_4'] = portnum
            if 'SFP' in flag:
                hourDic['cols_max_date']  =val_min_date[-8:]
                hourDic['cols_value_max'] = val_min
            else:
                hourDic['cols_max_date'] = val_max_date[-8:]
                hourDic['cols_value_max'] = val_max
            hourDic['cols_value_avg']  = val_avg
            hourDic['mdb_key'] = '{}::{}'.format(self.serial,flag)
            #print hourDic

            #day_table = datetime.datetime.strptime(hourDic['ins_date'],'%Y-%m-%d').strftime('y%Ym%md%d')
            #self.pdb.dbInsert(hourDic, 'monitor.perform_stg_{}'.format(day_table))
            self.hour_dict_list.append(hourDic)
            #print hourDic
            minInfoList=[minInfo0,minInfo1,minInfo2,minInfo3,minInfo4,minInfo5]
            dicList = dicList +self.minAvg(minInfoList,flag,portnum)
        return dicList
            
if __name__=='__main__':
    
    
    Avg().redis_set()
    
