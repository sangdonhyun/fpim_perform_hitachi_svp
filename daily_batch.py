'''
Created on 2017. 6. 26.

@author: muse
'''
import common
import fletaDbms
import os

class DailyBatch():
    def __init__(self):
        self.com  = common.Common()
        self.db = fletaDbms.FletaDb()
        self.today = self.com.getNow('%Y%m%d')
    
    def deleteInfoTable(self):
        yd= self.getDateFormat(self.today)
        query = "delete from monitor.pm_auto_hitachi_real_info where check_date not like '%"+yd+"%'"
        print query
    
    def createTable(self,td):
#         td=self.com.getNow('%Y%m%d')
        tableName = 'pm_auto_hitachi_real_info_avg_%s'%td
        query = "select count(*) from pg_tables where tablename like '%s'"%tableName
        rows=self.db.getRaw(query)
        if  int(rows[0][0]) == 0:
            cq="""
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
ALTER TABLE monitor.pm_auto_hitachi_real_info_avg_20170627
  OWNER TO "fletaAdmin";

            """%tableName
            self.db.queryExec(cq)
        query = "select count(*) from pg_tables where tablename like '%s'"%tableName
        rows=self.db.getRaw(query)
        print rows
        if  int(rows[0][0]) == 1 : 
            return True
        else:
            return False
        
        
    def backup(self):
        query = "SELECT distinct(substr(check_date,1,10))  FROM monitor.pm_auto_hitachi_real_info;"
        rows=self.db.getRaw(query)
        for row in rows:
            tdate= row[0]
            td = tdate.replace('-','')
            print td
            if td < self.today:
                self.createTable(td)
                self.query(td)
            
            
    def getDateFormat(self,td):
        yyyy = td[:4]
        mm = td[4:6]
        dd = td[-2:]
        print yyyy,mm,dd
        return '%s-%s-%s'%(yyyy,mm,dd)
    def query(self,td):
        targetDate = self.getDateFormat(td)
        tbName = 'pm_auto_hitachi_real_info_avg_%s'%td
        query = """WITH s AS (
select DD.*, (select cols_value_max_datetime from monitor.%s where substr(check_date,1,10) = DD.check_date and ctrl_unum = DD.ctrl_unum and flag_nm = DD.flag_nm and cols_nm = DD.cols_nm  Limit 1)  cols_value_max_datetime
from (

  select substr(check_date,1,10) check_date,ctrl_unum,flag_nm,cols_nm, cast(max(cols_value_max) as text) cols_value_max, cast(round(avg(CAST(cols_value_avg AS decimal)),2) as text) cols_value_avg


  
     from (
    SELECT *
            
            FROM monitor.%s
    ) aa 
    group by ctrl_unum,flag_nm,cols_nm,substr(check_date,1,10)

    ) DD    where check_date < to_char(current_timestamp, 'YYYYMMDD') and check_date like '%s'


),
upd AS (
     UPDATE monitor.pm_auto_hitachi_real_info_avg_daily c
     SET cols_value_avg=s.cols_value_avg,
     cols_value_max=s.cols_value_max
     FROM   s
     WHERE  c.cols_value_max    = s.cols_value_max
            and c.cols_value_avg    = s.cols_value_avg
            and c.cols_value_max_datetime    = s.cols_value_max_datetime
     RETURNING *
)
INSERT INTO monitor.pm_auto_hitachi_real_info_avg_daily
SELECT check_date,
       ctrl_unum,
       flag_nm,
       cols_nm,
       cols_value_max,
       cols_value_max_datetime,
       cols_value_avg
FROM   s
WHERE  NOT EXISTS (SELECT check_date,ctrl_unum,flag_nm,cols_nm FROM  monitor.pm_auto_hitachi_real_info_avg_daily r where s.check_date=r.check_date and s.ctrl_unum=r.ctrl_unum and s.flag_nm=r.flag_nm and s.cols_nm = r.cols_nm )
            """%(tbName,tbName,targetDate)  
        with open('dailyQuery.txt','w') as f:
            f.write(query)
        print query
        cmd = 'psql -U fletaAdmin fleta < dailyQuery.txt'
        print cmd
        print os.popen(cmd).read()
    
    
    def main(self):
        
        self.backup()
        
        self.createTable(self.today)
        self.deleteInfoTable()

        

if __name__=='__main__':
    DailyBatch().main()