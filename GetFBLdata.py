import mysql.connector as mariadb
import sys
import codecs
import os
import time
import bisect
from datetime import timedelta

#DB Information is hardcoded in for ease
def connectToDBSlot():
     dbName = 'travisdb'
     dbHost = 'zhiyuan.science'
     dbUser = 'zjp'
     dbPass = 'zjp'
     try:
          print "connecting to database..."
          db = mariadb.connect(host=dbHost,user=dbUser, password=dbPass, database=dbName)
          print "...OK!"
     except: 
          print "Could not connect to database", dbName
          sys.exit(2)
     return db

def escape(string):
     a=string.split('/')
     b=a[0]
     for i in range(1,len(a)):
          b=b+'_'+a[i]
     return b

def keydate(d):
     return d[0]#compare the date

def asdays(d):
     return timedelta(days=d)

def tosec(d):
     return float(d.days*86400+d.seconds)

def rmtuple(d):
     for i in range(len(d)):
          d[i]=d[i][0]
     return d
def getint(d):
     if d-int(d)<0.5:
          return int(d)
     return int(d)+1

#def tofloatlist(d):
 #    for i in range(len(d)):
  #        d[i]=float(d[i])
   #  return d

def getsum(a):
     c=0
     for b in a:
          try:
               c=c+b
          except:
               continue
     return c
def getnumlen(a):
     c=0
     for b in a:
          try:
               float(b)
               c=c+1
          except:
               continue
     return c
               

def mean(d):
     try:
          return getsum(d)/float(getnumlen(d))
     except:
          return None


def percentTest(a,b):
     try:
          return getsum(a)/float(getsum(b))
     except:
          return None

def dividex(a,b):
     try:
          return a/b
     except:
          return ''

def tomeanlist(a,b=[]):
     for i in a:
          try:
               if i=='':
                    i=0
               b.append(float(i))
          except:
               continue
     return mean(b)

def get_index(a,b):
     
     try:
          return a.index(b)
     except:
          return len(a)

def get_num_e(a,b):#get the number of b in list a
     counter=0
     for i in a:
          if i==b:
               counter=counter+1
     return counter


#def main():
FBL_LEN=2#at least the length of a fbl

db=connectToDBSlot()


fileout=codecs.open('Data_train.txt', 'w+',encoding='utf8')
itemlabel='IsPassed,CommitId,Branch,ProjectName,avg_TimeSpendPerCommit,Num_TotalTries,Num_errored,Num_failed,\
avg_TestCodeChangeDensity,avg_FileChange,avg_TestOK_Percent,avg_TestFail_Percent,CoreCommit_Percent,TeamSize,\
Total_Commits,Num_Comments,Num_PullRequest,avg_TestDuration,PJ_TimeSpan,avg_JobCount,gh_lang\n'
fileout.write(itemlabel)

filetest=codecs.open('Data_test.txt', 'w+',encoding='utf8')
filetest.write(itemlabel)

sqlBase='git_diff_test_churn, gh_diff_files_added, gh_diff_files_deleted, gh_diff_files_modified,\
          gh_diff_tests_added,gh_diff_tests_deleted,gh_by_core_team_member,gh_team_size,\
          gh_num_commit_comments,gh_lang,gh_build_started_at,gh_is_pr'


cursor = db.cursor()
sql='select gh_project_name from pj_list'
cursor.execute(sql)
pglist=cursor.fetchall()
for i in range(len(pglist)):
     #print pglist[i]
     pglist[i]=pglist[i][0]

for i in range(len(pglist)):
     sql='select distinct(git_branch) from build_id_list1 where gh_project_name=\''+str(pglist[i])+'\''
     cursor.execute(sql)
     branchlist=cursor.fetchall()
     for b in range(len(branchlist)):
          branchlist[b]=branchlist[b][0]
     
     
     
     sql='select gh_first_commit_created_at from travistorrent_11_1_2017\
          where gh_project_name=\''+str(pglist[i])+'\' order by tr_build_id limit 1'
     cursor.execute(sql)
     initdate=cursor.fetchone()[0]
     
     for x in range(len(branchlist)):#start to get build ids in a branch
          print str(x+1)+','+str(len(branchlist))+','+str(i+1)+','+str(len(pglist))
          sql='select tr_build_id from build_id_list1 where gh_project_name=\''+str(pglist[i])+'\' and git_branch=\''+unicode(branchlist[x])+'\''
          cursor.execute(sql)
          build_id=cursor.fetchall()
          #print 'a'
          sql='select tr_prev_build from build_id_list1 where gh_project_name=\''+str(pglist[i])+'\' and git_branch=\''+unicode(branchlist[x])+'\''
          cursor.execute(sql)
          prev_id=cursor.fetchall()
          #print 'b'
          sql='select tr_status from build_id_list1 where gh_project_name=\''+str(pglist[i])+'\' and git_branch=\''+unicode(branchlist[x])+'\''
          cursor.execute(sql)
          tr_status=cursor.fetchall()
          #print 'c'
          for m in range(len(build_id)):
               build_id[m]=build_id[m][0]
               prev_id[m]=prev_id[m][0]
               tr_status[m]=tr_status[m][0]
          #end getting the build id list
          #start to find FBLs
          FBLs=[]#store list of fbls in a branch
          status=[]#store tr_status in cor to FBLs
          
          
          startpoint=min(get_index(tr_status,"failed"),get_index(tr_status,"error"))

          if startpoint==len(build_id):#no failed or error in this branch, skip
               continue
          #print "start this branch"
          for m in range(startpoint,len(build_id)):
               
               
               if (prev_id[m]==None or m==startpoint)and(tr_status[m]=='errored'or tr_status[m]=='failed'):
                    #list is in time order, so the first could be a good start
                    
                    tempfbl=[]# store a fbl temply
                    tempstatus=[]
                    
                    tempfbl.append(build_id[m])
                    tempstatus.append(tr_status[m])
                    
                    prev_pos=get_index(prev_id,build_id[m])
                    while prev_pos<len(build_id):
                         #m=prev_pos
                         if tr_status[prev_pos]!='passed':
                              tempfbl.append(build_id[prev_pos])
                              tempstatus.append(tr_status[prev_pos])
                              
                         else:
                              if len(tempfbl)>=FBL_LEN:
                                   FBLs.append(tempfbl)
                                   status.append(tempstatus)
                              break;
                         
                         prev_pos=get_index(prev_id,build_id[prev_pos])
          if len(FBLs)<1:#check if no fbls, continue
               continue

          #print "complete this branch"
          ##FBLs contain a list of fbls in a branch; then start to get the info for fbls;
          for m in range(len(FBLs)):
               fbl=FBLs[m]
               t_status=status[m]
               
               NumEtries=get_num_e(t_status,'errored')#errored tries
               NumFtries=get_num_e(t_status,'failed')#failed tries
               NumTtries=len(fbl)#total tries
               testchurn=[]#test line changed
               filechurn=[]#number of files changed
               testok=[]
               testfail=[]
               testrun=[]
               corecommit=[]
               comments=[]
               testduration=[]
               jobcount=[]
               Tcommits=0
               Numispr=0
               teamsize=0
               teamsizeff=0
               timespan=asdays(0)
               ghlang=0
               Numispr=[]
               ispassed=0
               commitid=''
               for x2 in range(NumTtries):
                    sql='select '+sqlBase+'\
                         from travistorrent_11_1_2017 where tr_build_id=\''+str(fbl[x2])+'\' limit 1'
                    cursor.execute(sql)
                    templist=cursor.fetchone()
                    testchurn.append(templist[0])
                    filechurn.append(sum(templist[1:4]))
                    corecommit.append(templist[6])
                    comments.append(templist[8])
                    Numispr.append(templist[11])
                    

                    #print x,NumTtries
                    sql='select tr_log_num_tests_run,tr_log_num_tests_ok,tr_log_num_tests_failed,tr_log_testduration\
                         from travistorrent_11_1_2017 where tr_build_id=\''+str(fbl[x2])+'\' '
                    
                    cursor.execute(sql)
                    testinfo=cursor.fetchall()
                    testrun.append(mean([y[0]for y in testinfo]))
                    testok.append(mean([y[1]for y in testinfo]))
                    testfail.append(mean([y[2]for y in testinfo]))
                    tomeanlist([y[3]for y in testinfo],testduration)
                    jobcount.append(len(testinfo))

                    if x2 ==0:
                         if templist[9]=='ruby':
                              ghlang=1
                         s_date=templist[10]
                         teamsize=templist[7]
                         
                    elif x2==NumTtries-1:
                         e_date=templist[10]
                         sql='select git_trigger_commit from travistorrent_11_1_2017 where tr_build_id=\''+str(fbl[x2])+'\' limit 1'
                         cursor.execute(sql)
                         temp=cursor.fetchone()
                         commitid=temp[0]
                         if status[m][x2]=='passed':
                              ispassed=1
                         

               timespan=tosec(e_date-s_date)
               avgTimefp=timespan/float(NumTtries)
               sql='select count(*) from pg_build_date where \
                    gh_project_name=\''+str(pglist[i])+'\' and gh_build_started_at<=\''+str(e_date)+'\'\
                    and gh_build_started_at>=\''+str(s_date)+'\''
               cursor.execute(sql)
               Tcommits=cursor.fetchone()[0]

               if m <0.8*len(FBLs):
               
                    fileout.write(str(ispassed)+','+str(commitid)+','+unicode(branchlist[x])+','+str(pglist[i])+',')
                    fileout.write(str(avgTimefp)+','+str(NumTtries)+','+str(NumEtries)+','+str(NumFtries)+',')
                    fileout.write(str(mean(testchurn))+','+str(mean(filechurn))+','+str(percentTest(testok,testrun))+',')
                    fileout.write(str(percentTest(testfail,testrun))+','+str(sum(corecommit)/float(NumTtries+1))+',')
                    fileout.write(str(teamsize)+','+str(Tcommits)+','+str(sum(comments))+','+str(tomeanlist(Numispr))+','+str(mean(testduration))+',')
                    fileout.write(str(timespan)+','+str(mean(jobcount))+','+str(ghlang)+'\n')
               else:
                    filetest.write(str(ispassed)+','+str(commitid)+','+unicode(branchlist[x])+','+str(pglist[i])+',')
                    filetest.write(str(avgTimefp)+','+str(NumTtries)+','+str(NumEtries)+','+str(NumFtries)+',')
                    filetest.write(str(mean(testchurn))+','+str(mean(filechurn))+','+str(percentTest(testok,testrun))+',')
                    filetest.write(str(percentTest(testfail,testrun))+','+str(sum(corecommit)/float(NumTtries+1))+',')
                    filetest.write(str(teamsize)+','+str(Tcommits)+','+str(sum(comments))+','+str(tomeanlist(Numispr))+','+str(mean(testduration))+',')
                    filetest.write(str(timespan)+','+str(mean(jobcount))+','+str(ghlang)+'\n')
     
cursor.close()
fileout.close()
filetest.close()

#if __name__ == "__main__":
#     main()
