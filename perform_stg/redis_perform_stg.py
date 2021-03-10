'''
Created on 2019. 12. 12.

@author: user
'''
import datetime
import os
import time
import redis_brocade_20200924
import redis_brocade
import redis_hitachi
# import redis_unity
# import redis_netapp

olddate=datetime.datetime.now().strftime('%Y:%m:%d %H')
oldday=datetime.datetime.now().strftime('%Y-%m-%d')

print 'DATE' ,olddate
cnt=0
while True:
    newdate=datetime.datetime.now().strftime('%Y:%m:%d %H')
    newday = datetime.datetime.now().strftime('%Y-%m-%d')
    if newdate != olddate:
        try:
            redis_brocade.Avg().redis_set()
        except:
            pass
        try:
            redis_hitachi.Avg().redis_set()
        except:
            pass
#         try:
#             redis_unity.Avg().redis_set()
#         except:
#             pass
#         try:
#             redis_netapp.Avg().redis_set()
#         except:
#             pass
#         try:
#             redis_isilon.Avg().redis_set()
#         except:
#             pass
        print 'DATE CHANGE :',olddate,newdate 
        olddate=newdate
    print newday,oldday,newday == oldday
    redis_brocade.Avg().set_avg_day(oldday)
    if not newday == oldday:
        try:
            redis_brocade.Avg().set_avg_day(oldday)
            oldday = newday
        except:
            pass
    print 'CNT :',cnt
    cnt = cnt + 1
    time.sleep(60*5)



