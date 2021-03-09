'''
Created on 2017. 6. 30.

@author: muse
'''
import random
import redis
import datetime
import time

class RedisDaily():
    
    def __init__(self):

        self.conn = redis.Redis('localhost')

        host='localhost'
        self.redis_client = redis.StrictRedis(host)
        
    def getKeys(self):
        
        return  self.redis_client.execute_command('keys *')

    def getAvg(self,rList,tt):
        
        for t in tt:
            vList=[]
            for r in rList:
                if r[1] == t:
                    vList.append(int(r[-1]))
            rList.append(vList)
              
            print vList
            return len(rList),max(vList),round(sum(vList) / float(len(vList)),2)


    def timeDelta(self,time):
        
        
        dd= datetime.datetime.now() + datetime.timedelta(hours=-3)
        cd= dd.strftime('%Y-%m-%d %H:%M:%S')
        print dd,cd
        

    def main(self):
        t1 = time.clock()
        klist = self.getKeys()
        for key in klist:
            
            
                
            rvalue = ''
            rList=[]
            tList=[]
            for vals in self.redis_client.hgetall(key):

                rvalue =self.redis_client.hget(key,vals)
                t=vals[:13]
                print key,vals,t
                rList.append((key,t,vals,rvalue))
                tList.append(t)
                
                print key.split('.'),vals,rvalue
                print vals
        
        print 'END -'
        t2 = time.clock()
        print t2-t1
        
            
        
                    
                    
if __name__=='__main__':
    RedisDaily().main()
                        