#coding=utf8
import sys
sys.path.append("../common_tools")
from statutil import *

def getHash(value):
    h = 0;  
    if (h == 0 and len(value) > 0):
        for i in range(len(value)):
            h = 31 * h + ord(value[i])
        hash = h 
    return h


if __name__ == '__main__':
    conn = get_config_db('athena',0)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = "select cid, name, parent_cid, first_id, first_name, second_id, second_name, third_id, third_name, level from category where status = 1 and is_parent = 0"
    cursor.execute(sql)
    rows=cursor.fetchall()
    for row in rows:
        first_name = row["first_name"]
        if(first_name == ""):
            first_name = "-"
        second_name = row["second_name"]
        if(second_name == ""):
            second_name = "-"
        third_name = row["third_name"]
        if(third_name == ""):
            third_name = "-"
        first_name = first_name.lower()
        second_name = second_name.lower()
        third_name = third_name.lower()

        #print "%s\t%s\t%s"%(first_name, second_name, third_name)
        print "%s\t%s\t%s"%(first_name.encode("utf8"), second_name.encode("utf8"), third_name.encode("utf8"))
    conn.close()

     
    
