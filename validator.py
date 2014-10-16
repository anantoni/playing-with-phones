#!/usr/bin/python

import pymysql as db
import settings
import sys
import pprint
def insertRecord():
    return

def deleteRecord():
    return

def connection():
    ''' User this function to create your connections '''
    con = db.connect(
            settings.mysql_host, 
            settings.mysql_user, 
            settings.mysql_passwd, 
    settings.mysql_schema)
    
    return con


def query(phone):
    con = connection()
    cur = con.cursor()

    sql = "select * from Client as c where c.phone = %s;"

    cur.execute(sql, (long(phone),))

    for i in cur:
        print i

    cur.close()
    con.close()

def delete(phone):
    con = connection()
    cur = con.cursor()

    sql = "select * from Client where phone = %s;"
    cur.execute(sql, (long(phone),))

    row = cur.fetchone()
    if not row:
        print "Phone number" + str(phone) + " not found"
        return

    sql = "delete from Client where phone = %s;"

    cur.execute(sql, (long(phone),))
    con.commit()
    print "Phone number " + str(phone) + " deleted sucessfully"
    cur.close()
    con.close()

def insert(phone, lastname, firstname, town, invoice):
    # prepare a cursor object using cursor() method
    # Create a new connection
    con = connection()

    # Create a cursor on the connection
    cur = con.cursor()
    
    # Prepare SQL query to INSERT a record into the database.
    sql = "INSERT INTO Client (PHONE, LAST_NAME, FIRST_NAME, TOWN, INVOICE) VALUES (%s, %s, %s, %s, %s);"
    try:
        # Execute the SQL command
        cur.execute(sql, (phone, lastname, firstname, town, invoice))  # MySQL Settings
        
        # Commit your changes in the database
        con.commit()
    except:
        print "Record not added: Duplicate phone number exists."
        return

    print "Phone number " + str(phone) +" added successfully."
    cur.close()
    con.close()

def countPopulation(town):
    con = connection()
    cur = con.cursor()

    sql = "SELECT town, count(*) FROM Client WHERE town=%s GROUP BY town;"

    cur.execute(sql, (town,))
    
    rows = cur.fetchall()    
    if not rows:
        print "No records found for town."
        return

    for row in rows:
        print row[0] + ": " + str(row[1])

    cur.close()
    con.close()

def topPopulatedTowns(k):
    con = connection()
    cur = con.cursor()

    sql = "SELECT town, count(*) FROM Client GROUP BY town ORDER BY count(*) desc LIMIT %s;"

    cur.execute(sql, (int(k),))
    
    rows = cur.fetchall()
    if not rows:
        print "No records found."
        return

    for row in rows:
        if (row == (None, None)):
            print "No records found."
            return

        print row[0] + " " +  str(row[1])
        
    cur.close()
    con.close()

def findMin():
    con = connection()
    cur = con.cursor()

    sql = "SELECT min(phone) FROM Client;"

    cur.execute(sql)

    for i in cur:
        if i == (None,):
            print 0
            return        
        print long(i[0])

    cur.close()
    con.close()

def findMax():
    con = connection()
    cur = con.cursor()

    sql = "SELECT max(phone) FROM Client" 
    
    cur.execute(sql)
    
    for i in cur:
        if i == (None,):
            print 0
            return
        print long(i[0])

    cur.close()
    con.close()

def sumOfTown(town):
    con = connection()
    cur = con.cursor()

    sql = "select town, sum(invoice) from Client where town = %s;"
    
    cur.execute(sql, (town,))
    
    rows = cur.fetchall()

    for row in rows:
        if row == (None, None):
            print "No records found for given town."
            return

        print row[0] + ": " + str(row[1])

    
    cur.close()
    con.close()

def findTopSpenders():
    con = connection()
    cur = con.cursor()

    sql = "SELECT town, phone, invoice FROM Client as c1 where c1.invoice >= all(Select c2.invoice from Client as c2 where c1.town = c2.town) order by invoice DESC;"
    
    cur.execute(sql, )

    rows = cur.fetchall()
    if not rows:
        print "No records found."
        return

    for row in rows:
        print row[0] + " " + str(long(row[1])) + " " + str(row[2])

    cur.close()
    con.close()

def findTopSpendersInTown(town, l):
    con = connection()
    cur = con.cursor()
    
    sql = "select phone, invoice from Client where town = %s order by invoice DESC limit %s;"

    cur.execute(sql, (town, int(l)))

    rows = cur.fetchall()
    if not rows:
        print "No records found for given town."
        return

    for row in rows:
        print str(long(row[0])) + " " + str(row[1])

    cur.close()
    con.close()

def loadDataFile(dataFile):
    # prepare a cursor object using cursor() method
    # Create a new connection
    con = connection()

    # Create a cursor on the connection
    cur = con.cursor()
    try:
        with open(dataFile, 'r') as f:
            for line in f:
                line = line.split(' ')
            
                # Prepare SQL query to INSERT a record into the database.
                sql = "INSERT INTO Client (PHONE, LAST_NAME, FIRST_NAME, TOWN, INVOICE) VALUES (%s, %s, %s, %s, %s);"
                try:
                    # Execute the SQL command
                    cur.execute(sql, (line[0], line[1], line[2], line[3], line[4]))  # MySQL Settings

                    # Commit your changes in the database
                    con.commit()
                except:
                    print sys.exc_info()[0]

    except:
        print "Datafile " + dataFile
    cur.close()
    con.close()

def deleteData():
    con = connection()
    cur = con.cursor()

    sql = "DELETE FROM Client;"

    cur.execute(sql)

    con.commit()

    cur.close()
    con.close()

if __name__ == "__main__":
    # Open database connection
    # loadDataFile(db, dataFile)
    deleteData()
    file = open("operationsfile.txt", 'r')

    for line in file:
        line = line.strip()
        print "\nmyphones> " + line
        elements = line.split(' ')
        if (elements[0] == 'i'):
            insert(elements[1], elements[2], elements[3], elements[4], elements[5])

        elif (elements[0] == 'd'):
            delete(elements[1])

        elif (elements[0] == 'q'):
            query(elements[1])

        elif (elements[0] == 't'):
            topPopulatedTowns(elements[1])

        elif (elements[0] == 's'):
            sumOfTown(elements[1])

        elif (elements[0] == 'p'):
            countPopulation(elements[1])

        elif (elements[0] == 'ft'):
            findTopSpendersInTown(elements[1], elements[2])

        elif (elements[0] == 'fts'):
            findTopSpenders()

        elif (elements[0] == 'min'):
            findMin()

        elif (elements[0] == 'max'):
            findMax()

        elif (elements[0] == 'l'):
            loadDataFile(elements[1])

        else:
            print "invalid command"
