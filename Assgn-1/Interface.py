#!/usr/bin/python2.7
#Interface for the assignement

import psycopg2
DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    ratings_data = open(ratingsfilepath, 'r')
    cur = openconnection.cursor()

    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (ratingstablename,))
    if not cur.fetchone()[0]:
        cur.execute("CREATE TABLE %s (userid integer, movieid integer, rating float);" %(ratingstablename,))

    for data_sample in ratings_data:
        sample = data_sample.split('::')
        cur.execute("insert into %s VALUES (%s, %s, %s);" %(ratingstablename, sample[0], sample[1], sample[2]))
    
    openconnection.commit()
    ratings_data.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    partition_range = 5/float(numberofpartitions)
    rating_start = 0
    rating_end = 0
    cur = openconnection.cursor()

    for i in range(0, numberofpartitions):
        rating_end += partition_range
        cur.execute("create table %s (userid integer, movieid integer, rating float)" %('range_part'+str(i), ))
        if rating_start == 0:
            cur.execute("insert into %s select * from %s where rating>=%s and rating<=%s" %('range_part'+str(i), ratingstablename, rating_start, rating_end))
        else:
            cur.execute("insert into %s select * from %s where rating>%s and rating<=%s" %('range_part'+str(i), ratingstablename, rating_start, rating_end))
        rating_start = rating_end

    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ("metadata",))
    if not cur.fetchone()[0]:
        cur.execute("create table metadata (attribute_name varchar, attribute_value integer);")
    cur.execute("insert into metadata (attribute_name, attribute_value) VALUES ('%s', %s);" %('range_partitions', numberofpartitions))
    
    openconnection.commit()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur.execute("select * from %s" %(ratingstablename))
    rows = cur.fetchall()
    row_num = 0

    for i in range(numberofpartitions):
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)",("rrobin_part" + str(i),))
        if not cur.fetchone()[0]:
            cur.execute("create table %s (userid integer, movieid integer, rating float);" % ("rrobin_part" + str(i)))

    for row in rows:
        partition_number = row_num % numberofpartitions
        cur.execute("insert into %s (userid, movieid, rating) VALUES (%s, %s, %s)" %("rrobin_part"+str(partition_number), row[0], row[1], row[2]))
        row_num += 1
    
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ("metadata",))
    if not cur.fetchone()[0]:
        cur.execute("create table metadata (attribute_name varchar, attribute_value integer);")
    cur.execute("insert into metadata (attribute_name, attribute_value) VALUES ('%s', %s);" % ('rrobin_partitions', numberofpartitions))
    
    openconnection.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    
    cur.execute("select count(*) from %s" %(ratingstablename))
    tableSize = cur.fetchone()[0]
    
    cur.execute("select attribute_value from metadata where attribute_name='rrobin_partitions';")
    num_of_partitions = cur.fetchone()[0]

    partition_number = (tableSize)%num_of_partitions

    cur.execute("insert into %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (ratingstablename, userid, itemid, rating))
    cur.execute("insert into %s (userid, movieid, rating) VALUES (%s, %s, %s)" %("rrobin_part"+str(partition_number) ,userid, itemid, rating))
    
    openconnection.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("select attribute_value from metadata where attribute_name='range_partitions';")
    num_of_partitions = cur.fetchone()[0]
    
    if num_of_partitions == 0:
        return
    partition_range = 5/float(num_of_partitions)

    partition_number = rating/partition_range
    if partition_number != 0 and partition_number % 1 == 0:
        partition_number = partition_number - 1

    partition_number = int(partition_number)

    cur.execute("insert into %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (ratingstablename, userid, itemid, rating))
    cur.execute("insert into %s (userid, movieid, rating) values (%s, %s, %s)" %('range_part'+str(partition_number),userid, itemid, rating))
    
    openconnection.commit()

def create_db(dbname):
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        print 'A database named {0} already exists'.format(dbname)

    cur.close()
    con.close()
