#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb


def show_table(name,sql='-1'):
    """The default way to use this function is show_table(tablename)
       ,then the data in thie table will be showed.You can also choose
       to show_table(tablename,sql_sentence),then you sentense will be executed
       ,the related results will be showed"""
    if sql == '-1':
        sql = """SELECT * from %s"""%name
    aa=cursor.execute(sql)
    if aa==0:
        print "%s is a empty table"%name
    elif aa>0:
        results = cursor.fetchmany(aa)
        for row in results:
            print row
    else:
        print "Wrong values of the execute function return number"
    return

def clear_table(name):
    #clear the table
    sql="""delete from %s"""%name
    cursor.execute(sql)
    return

def add_bw(port,bw):
    #add bandwidth to 'bw_left' in the wanted port
    sql1 = """select bw_left_mbit from CONNECTION where port_num='%s'""" % port
    cursor.execute(sql1)
    old_bw = cursor.fetchone()[0]
    new_bw = old_bw + bw
    sql2 = """update CONNECTION set bw_left_mbit=%f where port_num='%s'
            """ % (new_bw, port)
    try:
        cursor.execute(sql2)
    except:
        print "Something Wrong"
    return

def substract_bw(port,bw):
    #add bandwidth to 'bw_left' in the wanted port
    sql1 = """select bw_left_mbit from CONNECTION where port_num='%s'""" % port
    cursor.execute(sql1)
    old_bw = cursor.fetchone()[0]
    new_bw = old_bw - bw
    sql2 = """update CONNECTION set bw_left_mbit=%f where port_num='%s'
            """ % (new_bw, port)
    cursor.execute(sql2)
    return

def init_table():
    "Initail SERVICE CONNECTION PORT tables"
    #SERVICE table
    sql1="""create table SERVICE(id int(8),sou_ip char(20),des_ip char(20)
            ,udp_port char(16),bw char(16))"""
    cursor.execute((sql1))
    #CONNECTION table
    sql2 = """create table CONNECTION(sou_switch_num char(20),port_num char(20),
                des_switch_num char(20),bw_mbit float,bw_left_mbit float)"""
    cursor.execute((sql2))
    #PORT table
    sql3 = """create table PORT(service_id int(8),port_num char(20))"""
    cursor.execute((sql3))
    return

def create_connection(port_dict,link_list):
    "create CONNECTION table by port info and link info"
    #initialize the CONNECTION table without des_switch_num
    for key in port_dict:
        snum = key[1]
        bw = bw_left = float(port_dict[key][0])
        sql = """insert into CONNECTION(sou_switch_num,port_num,bw_mbit,
                     bw_left_mbit) values('%s','%s',%f,%f)""" % (snum, key, bw, bw_left)
        cursor.execute(sql)
    #initialize the CONNECTION table with des_switch_num
    for temp in link_list:
        (port1, port2) = temp
        des1 = port2[1]
        des2 = port1[1]
        sql1 = """update CONNECTION set des_switch_num='%s' where port_num='%s'""" % (des1, port1)
        sql2 = """update CONNECTION set des_switch_num='%s' where port_num='%s'""" % (des2, port2)
        cursor.execute(sql1)
        cursor.execute(sql2)
    return

def GetTopo():
    #abstract neighbor matrix from CONNECTION table
    sql1 = """select * from CONNECTION"""
    aa = cursor.execute(sql1)
    if aa < 1:
        print "The %s table is empty!" %table_name
        return
    results = cursor.fetchall()
    size = 1
    for t in results:
        c1 = int(t[0])
        if size < c1:
            size = c1
    matrix = [[-1 for i in range(size)] for j in range(size)]
    for t in results:
        snum, dnum, bw_left = (int(t[0]), int(t[2]), t[4])
        matrix[(snum - 1)][(dnum - 1)] = bw_left
    return matrix

def OutPort(list):
    #According the route switch num,get the ports
    route_port = []
    for t in list:
        snum1, snum2 = t
        sql1 = """select port_num from CONNECTION where sou_switch_num='%s' and
               des_switch_num='%s'""" % (snum1, snum2)
        sql2 = """select port_num from CONNECTION where sou_switch_num='%s' and
               des_switch_num='%s'""" % (snum2, snum1)
        cursor.execute(sql1)
        port1 = cursor.fetchone()[0]
        cursor.execute(sql2)
        port2 = cursor.fetchone()[0]
        route_port.append((port1, port2))
    #return the port list
    return route_port

def delete_service(id):
    "delete used port bandwidth,add the related left bandwidth"
    deta=0.1
    #Get the bandwidth occupied by the service
    sql = """select bw from SERVICE where id=%d""" % id
    cursor.execute(sql)
    bw = cursor.fetchone()[0]
    p=bw.find('m')
    deta=float(bw[0:p])
    #Change bw_left_mbit parameter in CONNECTION
    sql = """select port_num from PORT where service_id=%d""" % id
    cursor.execute(sql)
    result = cursor.fetchall()
    for t in result:
        port = t[0]
        add_bw(port, deta)
    #delete port information in Port
    sql = """delete from PORT where service_id=%d""" % id
    cursor.execute(sql)
    #delete service infomation in SERVICE
    sql = """delete from SERVICE where id=%d""" % id
    cursor.execute(sql)
    return

def add_service(service_dict,port_dict):
    """change port value when adding new service"""
    deta = 0.0
    #add record in SERVICE table
    for key in service_dict:
        id = key
        sou_ip, des_ip, udp_port, bw = service_dict[key]
        sql = """insert into SERVICE values(%d,'%s','%s','%s','%s')""" % (id, sou_ip, des_ip, udp_port, bw)
        cursor.execute(sql)
        p=bw.find('m')
        deta=float(bw[0:p])
    # add record in PORT table
    for key in port_dict:
        id = key
        for t in port_dict[id]:
            for port in t:
                sql = """insert into PORT values(%d,'%s')""" % (id, port)
                cursor.execute(sql)
    #Change bw_left_mbit parameter in CONNECTION
    sql = """select port_num from PORT where service_id=%d""" % id
    cursor.execute(sql)
    result = cursor.fetchall()
    for t in result:
        port = t[0]
        substract_bw(port,deta)
    return

def check_service(sou_ip,des_ip,udp_port):
    "check if the string is in the SERVICE table"
    sql = """select id from SERVICE where sou_ip='%s' and des_ip='%s' and
            udp_port='%s'""" % (sou_ip, des_ip, udp_port)
    a = cursor.execute(sql)
    if a > 0:
        return True
    elif a == 0:
        return False
    else:
        print "Warning:wrong with the SERVICE table"
        return False


# 打开数据库连接
db = MySQLdb.connect("localhost","root","890iop","TESTDB" )#数据库需要提前创建
# 使用cursor()方法获取操作游标 
cursor = db.cursor()

########################测试数据
#拓扑构建
port_basic = {'s1-eth2':'2mbit','s2-eth1':'2mbit','s2-eth2':'2mbit',
            's3-eth1':'2mbit'}
link = [('s1-eth2','s2-eth1'),('s2-eth2','s3-eth1')]
#路由端口请求信息
route=[('1','2'),('2','3')]
#路由端口返回信息（由程序得到）
route_port=[('s1-eth2', 's1-eth2'), ('s2-eth2', 's2-eth2')]
#业务信息以及业务涉及到的端口
service_dict1 = {1:['10.0.0.1','10.0.0.2','1234','0.5mbit']}
service_dict2 = {2:['10.0.0.1','10.0.0.2','1234','1.1mbit']}
port_dict1 = {1:[('s1-eth2', 's2-eth1'), ('s2-eth2', 's3-eth1')]}
port_dict2 = {2:[('s1-eth2', 's2-eth1'), ('s2-eth2', 's3-eth1')]}
#ip、端口测试信息
ip1,ip2,ip3,port=("10.0.0.1","10.0.0.2",'10.0.0.3','1234')

######################## 初始化
#定义三个表
#init_table()

clear_table('SERVICE')
clear_table('CONNECTION')
clear_table('PORT')
show_table('SERVICE')
show_table('CONNECTION')
show_table('PORT')
print "Cut Line"
##############################无畏虹部分（填写连接表）
create_connection(port_basic,link)

#############################李婕妤部分（连接矩阵、由路由信息得到路由经过的端口）
#返回拓扑
matrix=GetTopo()
print matrix
#由路由信息得到端口
route_port=OutPort(route)
print route_port

##############################无畏虹部分（添加业务、检测流是否在业务之中、删除业务）
#记录开通的第一个业务，填写服务表和已用端口表
add_service(service_dict1,port_dict1)
show_table('SERVICE')
show_table('CONNECTION')
show_table('PORT')
print "Cut Line"
#记录开通的第二个业务
add_service(service_dict2,port_dict2)
show_table('SERVICE')
show_table('CONNECTION')
show_table('PORT')
print "Cut Line"
#检测流是否在服务之中
print check_service(ip1,ip2,port)
print check_service(ip1,ip3,port)
#删除第一个业务
delete_service(1)
show_table('SERVICE')
show_table('CONNECTION')
show_table('PORT')
print "Cut Line"
#删除第二个业务
delete_service(2)
show_table('SERVICE')
show_table('CONNECTION')
show_table('PORT')





# 数据库接口处理
db.commit()
cursor.close()
db.close()