#
# fpim for hitachi SVP

1. list.cfg
[94448]    <= serial
passwd = expadmin  <= SVP user
user = expadmin    <= SVP pass
ip = 40.10.12.199  <= SVP IP


2. 필수 jar
./lib/JSanExportLoader.jar
    SVP 의 성능 수집 실핼파일 (.bat) 열어보면

#-classpath "./lib/JSanExport.jar;./lib/JSanRmiApiEx.jar;./lib/JSanRmiServerUx.jar"
SET CLASSPATH="./lib/JSanExport.jar;./lib/JSanRmiApiEx.jar;./lib/JSanRmiServerUx.jar";%CLASSPATH
java  -Xmx536870912 -Dmd.command=<CFGFILE> -Dmd.logpath=log -Dmd.rmitimeout=20 sanproject.getmondat.RJMdMain

    %<CFGFILE> = ./94448.txt

    명령어가 있음.


---------------------------------------------------------------------------------------------------
svpip 40.10.12.199            ; Specifies IP address of SVP <===== Change to IP Address of SVP
login expadmin "expadmin"         ; Logs user into SVP          <===== Change to predefined Userid/password for exclusive use by export tool
show                            ; Outputs storing period & gethering interval to standard output

;  +---------------------------------------------------------------------------------------------------+
;  | Group commands define the data to be exported.
;  +---------------------------------------------------------------------------------------------------+
;  | For Physical, there is no need to specify Long or Short - the following will get Long and Short
;  | Of course, if you specify Short, you only get short!
;  +---------------------------------------------------------------------------------------------------+
;group PhyPG                     ; Parity Groups
;group PhyLDEV                   ; Logical Volumes
group PhyProc                   ; Micro-Processor usage
;group PhyExG                    ; External Volume Group usage
;group PhyExLDEV                 ; External Volume usage
group PhyESW                    ; Access Paths and Write Pending
;group PhyMPPK                   ; MPPK Performance Information
;  +---------------------------------------------------------------------------------------------------+
;group PG                        ; Parity Group Statistics
;group LDEV                     ; LDEV usage in PGs, External Volume Groups or V-VOL Groups
;                               ; Not required when using LDEVEachOfCU
group Port                      ; Port usage
;group PortWWN                   ; Stats for HBAs connected to ports.
;group LU                        ; LDEV usage Summarised by LU Path
;group PPCGWWN                   ; Stats about HBAs
;group RemoteCopy                ; Remote Copy Usage Summarized by Subsystem
;group RCLU                      ; Remote Copy Usage Summarized by LU path
;;group RCLDEV                    ; Remote Copy Usage Summarized by LDEV
;group UniversalReplicator       ; Remote Copy Usage by UR Summarized by Subsystem
;group URJNL                     ; Remote Copy Usage by UR Summarized by Journal Group
;group URLU                      ; Remote Copy Usage by UR Summarized by LU Path
;group URLDEV                    ; Remote Copy Usage by UR Summarized by LDEV
;group LDEVEachOfCU              ; LDEV usage in CUs - Recommended
;  +---------------------------------------------------------------------------------------------------+
;  | end of group statements
;  +---------------------------------------------------------------------------------------------------+

;  +---------------------------------------------------------------------------------------------------+
;  | To limit the data collection within a date/time range, use the following sub-commands:-
;  | shortrange start_timestamp:end_timestamp
;  | longrange start_timestamp:end_timestamp
;  |
;  | Where start_timestamp and end_timestamp are in the format:- yyyyMMddHHmm
;  |
;  | For example:-
;  |            yyyyMMddHHmm:yyyyMMddHHmm
;  | shortrange 200607101200:200607111159
;  | longrange  200607101200:200607111159
;  |
;  | The above example will collect shortrange and longrange data between 12:00 on 10th July 2006
;  | and 11:59 on 11th July 2006
;  |
;  | NB - this is the time on the SVP - not on your server - and it may very well be GMT!
;  |
;  | Example below says get the latest 24 hours
;  |            (hhmm format)
;  | shortrange -2400:
;  |
;  | Example below says get the latest 72 hours
;  |           (ddhhmm format)
;  | longrange -030000:
;  +---------------------------------------------------------------------------------------------------+
shortrange  -0005:              ; Number of monitored CUs is 64 or less
;shortrange -0800:              ; Number of monitored CUs is 65 or more
longrange -000001:
;  +---------------------------------------------------------------------------------------------------+
;  | end of time statements
;  +---------------------------------------------------------------------------------------------------+

outpath data                     ; Specifies the sub-directory in which files will be saved
option compress                 ; Specifies whether to compress files
apply                           ; Executes processing for saving monitoring data in files
---------------------------------------------------------------------------------------------------------


shotrange : 수집 최소 단위 (-5 가 미니멈)
outpath   : 결과 파일 적재 위치
option compress  : 결과값을 ZIP 파일로 적재



3.
    a) python svpInfo.py 실행
    b) ./config/list.cfg 에서 리스트 생성
    c) ./data/ 밑 파일 삭제
    d) commnad 실행 (SVP 통신)
    e) ./data/ 에 적재
    f) redis 에 data insert

4.  ./perform_stg/redis_hitachi.py 평균데이터 postgre 적재
