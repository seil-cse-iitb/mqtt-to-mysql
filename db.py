from datetime import datetime
from MySQLdb import *

def insert_to_db(room_name,ts,node_id,temp,humidity,volt):

        con = connect("10.129.23.161","writer","datapool","cooling")
        cur = con.cursor()

        sql  = "insert into temperature_analysis(room_name,timestamp,node_id,temperature,humidity,voltage)  values('%s','%s','%d','%f','%f','%f')" %(room_name,ts,node_id,temp,humidity,volt)
        # print sql
        try:
                # print "Executing sql"
                cur.execute(sql)
                # print "Executed sql"
                con.commit()
                # print "Insertion done -- Done"
        except:
                con.rollback()
                print "Execution failed -- Rollback in progress.."
        con.close()
