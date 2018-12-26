import MySQLdb
import time
import sys
from socket import socket

def TableName(ti):
	time_s = time.mktime(time.strptime(ti, "%Y-%m-%d %H:%M:%S"))
	temp = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_s)))[0:10]
	tn = 'log'+temp[0:4]+temp[5:7]+temp[8:10]
	return(str(tn))

def sleeptime(hour,min,sec):
	return hour*3600+min*60+sec;

config = {
    'host':'localhost',
	'port':3306,
	'user':'root',
	'passwd':'root',
	'db':'DBtest'
}

db = MySQLdb.connect(**config)
cursor = db.cursor()

system_list = {'00':'N/A','01':'WE','02':'NU','04':'CI','08':'ZC','10':'VOBC','40':'ATS','20':'DSU'}


sll = 1
s2 = sleeptime(0,0,0)
second = sleeptime(0,5,0)
s = round(sleeptime(24,0,0)/second)
time_second = time.time();
time_h = ''
time_t = ''
o = 1
st = 0

CARBON_SERVER = '127.0.0.1'
CARBON_PORT = 2003
sock = socket()
try:
	sock.connect((CARBON_SERVER, CARBON_PORT))
except:
	print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % {'server': CARBON_SERVER, 'port':CARBON_PORT}
	sys.exit(1)
	
f = open('starttime.conf')
contents = f.read()
time_h = str(contents)
f.close()

f = open('endtime.conf')
contents = f.read()
time_end = str(contents)
f.close()

#time_h = str(raw_input("start_date"))
print time_h
if len(time_h)==0:
	time_h = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))[0:10]+" 00:00:00"
time_t = str(time_h)

print(time_end)
print(time_h)

#time_end = str(raw_input("end_time"))
while True:
	if o==1:
		table_name = TableName(time_t)
		print table_name
		temp = time.localtime(time_second)
		time_second = time.mktime(time.strptime(time_t, "%Y-%m-%d %H:%M:%S"))
		o = 2
		st = 0
		time.sleep(sll)
		continue
	else:
		if st == s:
			o = 1
			continue
		else:
			st += 1
			time_second += second
			time_h = time_t
			time_t = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_second)))
			time_now = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
			day2 = str(time.localtime(time_second)[2])

			if len(time_end)!=0:
				if time_t>time_end:
					break

			while time_now<=time_t:
				print 'time wait',time_t,time_now
				time.sleep(second)
				time_now = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))

			sql = "show tables like '"+table_name+"';"
			cursor.execute(sql)
			results = cursor.fetchall()

			if len(results)==0:
				print 'no table',time_t, time_now
				continue
			else:
				sql = "SELECT source_type,dest_type,count(*),sum(size) FROM " + table_name + " where time>'" + time_h + "' AND time <'" + time_t + "' group by source_type,dest_type;"
				print sql
				try:
					cursor.execute(sql)
					results = cursor.fetchall()
					time_stamp = int(time_second) + s2
					lines = []
					for row in results:
						lines.append("CBTC.counts.%s.%s.ct %s %d" % (
						system_list[row[0]], system_list[row[1]], row[2], time_stamp))
						lines.append("CBTC.sizesum.%s.%s.ct %s %d" % (
						system_list[row[0]], system_list[row[1]], str(row[3]), time_stamp))
					message = '\n'.join(lines) + '\n'
					print "sending message\n"
					print '-' * 80
					print message
					print
					sock.sendall(message)
				# o+=1
				# if o>3:
				# break
				except:
					print"Error: unable tp fecth data"
				db.commit()
cursor.close()
db.close()
