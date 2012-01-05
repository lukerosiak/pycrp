"""This python utility imports bulk data from the Center for Responsive Politics into a MySQL database, sparing the user the repetitious work of importing, naming fields and properly configuring tables. It is based in part on code from the Sunlight Foundation's Jeremy Carbaugh.

It includes a few auxillary tables and fields not part of CRP's official bulk download, but does not harness Personal Financial Disclosures. When you run this script repeatedly, it will check the updated dates and only re-download if the data has been modified. Even so, you are encouraged to download bulk files, which can be quite large, at non-peak-traffic times. Because some table schemas have changed over cycles, the utility has been tested only for recent cycles: 2008 and 2010. 

Register for a 'MyOpenSecrets' account at opensecrets.org and supply your login info below. Create a mysql database on your computer and provide the host, user, password and database name. Also set the 'cycles' list for the two-digit representation of the election cycle you want to download. Then run python grab-crp.py. 

Windows users can connect to this database in Microsoft Access if you prefer by setting up an ODBC connection. (Start-Control Panel-Administrative Tools-Data Sources (ODBC)). After you've set up an ODBC connection using the MySQL ODBC Connector, go to the External Data tab in Access, click 'other' and 'ODBC,' and connect to the tables. 

Luke Rosiak
"""

import MySQLdb
import sys
import csv
import datetime
import logging
import os
import re
import urllib, urllib2


from credentials import *


class LobbyDownloader(object):
    
    def __init__(self):
        
        self.db = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD,db=MYSQL_DB)
        self.cursor = self.db.cursor()
        self.dest_path = '/home/luke/research/pycrp/raw'

    def createtables(self):
        queries = [
                """CREATE TABLE IF NOT EXISTS crp_lobbying(
	            uniqid varchar(56) NOT NULL,
	            registrant_raw varchar(95) NULL,
	            registrant varchar(40) NULL,
	            isfirm char(1) NULL,
	            client_raw varchar(95) NULL,
	            client varchar(40) NULL,
	            ultorg varchar(40) NULL,
	            amount float NULL,
	            catcode char(5) NULL,
	            source char (5) NULL,
	            self char(1) NULL,
	            IncludeNSFS char(1) NULL,
	            usethis char(1) NULL,
	            ind char(1) NULL,
	            year char(4) NULL,
	            type char(4) NULL,
	            typelong varchar(50) NULL,
	            orgID char(10) NULL,
	            affiliate char(1) NULL,
                PRIMARY KEY (uniqid)
                );""",
                """CREATE TABLE IF NOT EXISTS crp_lobbyist(
	            uniqID varchar(56) NOT NULL,
	            lobbyist varchar(50) NULL,
	            lobbyist_raw varchar(50) NULL,
	            lobbyist_id char(15) NULL,
	            year varchar(5) NULL,
	            Offic_position varchar(100) NULL,
	            cid char (12) NULL,
	            formercongmem char(1) NULL,
                INDEX u (uniqID)
                );""",
                """CREATE TABLE IF NOT EXISTS crp_lob_indus(
	            client varchar(40) NULL,
	            sub varchar(40) NULL,
	            total float NULL,
	            year char(4) NULL,
	            catcode char(5) NULL
                );""",
                """CREATE TABLE IF NOT EXISTS crp_lob_agency(
	            uniqID varchar(56) NOT NULL,
	            agencyID char(4) NOT NULL,
	            Agency varchar(80) NULL,
                INDEX u (uniqID)
                );""",
                """CREATE TABLE  IF NOT EXISTS crp_lob_issue(
	            SI_ID int NOT NULL,
	            uniqID varchar(56) NOT NULL,
	            issueID char(3) NOT NULL,
	            issue varchar(50) NULL,
	            SpecificIssue varchar(255) NULL,
	            year char (4) NULL
                );""",
                """CREATE TABLE  IF NOT EXISTS crp_lob_bills(
	            B_ID int NULL,
	            si_id int NULL,
	            CongNo char(3) NULL,
                Bill_Name varchar(15) NOT NULL
                );""",
                """CREATE TABLE IF NOT EXISTS crp_lob_rpt(
	            TypeLong varchar (50) NOT NULL,
	            Typecode char(4) NOT NULL
                );"""
        ]
          
        for query in queries:
            self.cursor.execute(query)



    def populatetables(self):

        def writerowsfromcsv(file, table):
            def linereader(path):
                infile = open(path, 'rU')
                for line in infile:
                    line = unicode(line, 'ascii', 'ignore').replace('\n', '')
                    yield line
                infile.close()
            
            detailReader =  csv.reader(linereader(file), quotechar='|')
            writerows(detailReader, table)

        def writerows(rows, table):

            def reformatdate(date):
                return date[6:] + '-' + date[:2] + '-' + date[3:5]

            logging.info("Writing " + table)
            for row in rows:
                if len(row)>0:
                    if table=='lobbyagency':
                        row[1] = row[1][:3]

                    sql = "INSERT INTO crp_" + table + " VALUES ("
                    for f in row:
                        f = f.decode('iso8859-1').encode('utf-8','ignore').strip()
                        sql = sql+' %s,'
                    sql = sql[:-1]+");"
                    try:
                        self.cursor.execute(sql,row) 
                    except:
                        logging.info( "This FAILED:" + sql + str(row) )
                        pass


        ext = ".txt"
        self.cursor.execute("DELETE FROM crp_lobbying")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_lobbying" + ext), "lobbying"))
        self.cursor.execute("DELETE FROM crp_lobbyist")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_lobbyist" + ext), "lobbyist"))
        self.cursor.execute("DELETE FROM crp_lob_indus")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_indus" + ext), "lob_indus"))
        self.cursor.execute("DELETE FROM crp_lob_agency")
        writerowsfromcsv( os.path.join(self.dest_path, "lob_agency" + ext), "lob_agency")
        self.cursor.execute("DELETE FROM crp_lob_issue")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_issue" + ext), "lob_issue"))
        self.cursor.execute("DELETE FROM crp_lob_bills")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_bills" + ext), "lob_bills"))
        self.cursor.execute("DELETE FROM crp_lob_rpt")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_rpt" + ext), "lob_rpt"))

if __name__ == '__main__':
    dl = LobbyDownloader()

    dl.createtables()
    dl.populatetables()

  
