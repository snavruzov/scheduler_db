#!/usr/bin/env python
__author__ = 'snavruzov'

import psycopg2
import sys

from apscheduler.scheduler import Scheduler

sched = Scheduler()
flag = True


def string_gen(ver):
    result = ''.join(["(" + "'" + row[1] + "'" + ")," for row in ver])
    return result[:-1]


@sched.interval_schedule(minutes=5)
def update_base():
    print 'This job is updating table every 5 minutes.'
    con = None
    try:
        con = psycopg2.connect(host="localhost", database='sms', user='postgres')
        cur = con.cursor()
        #Extract last 5 minutes before updated records
        cur.execute("SELECT * FROM table_2 WHERE "
                    "dateadd>=now() - interval '5 minute' order by 1 desc")
        ver = cur.fetchall()
        upd_query = "INSERT INTO table_1 (datas) values "
        upd_query += string_gen(ver)
        print upd_query
        if ver:
            cur.execute(upd_query)
            con.commit()

    except Exception, e:
        if con:
            con.rollback()
        global flag
        flag = False
        print 'Oops! Error %s' % e
        sys.exit(1)

    finally:
        if con:
            con.close()


sched.start()

while flag:
    pass
