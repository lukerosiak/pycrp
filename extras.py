"""This python utility imports bulk data from the Center for Responsive Politics into a MySQL database, sparing the user the repetitious work of importing, naming fields and properly configuring tables. It is based in part on code from the Sunlight Foundation's Jeremy Carbaugh.

It includes a few auxillary tables and fields not part of CRP's official bulk download, but does not harness Personal Financial Disclosures. When you run this script repeatedly, it will check the updated dates and only re-download if the data has been modified. Even so, you are encouraged to download bulk files, which can be quite large, at non-peak-traffic times. Because some table schemas have changed over cycles, the utility has been tested only for recent cycles: 2008 and 2010. 

Register for a 'MyOpenSecrets' account at opensecrets.org and supply your login info below. Create a mysql database on your computer and provide the host, user, password and database name. Also set the 'cycles' list for the two-digit representation of the election cycle you want to download. Then run python grab-crp.py. 

Windows users can connect to this database in Microsoft Access if you prefer by setting up an ODBC connection. (Start-Control Panel-Administrative Tools-Data Sources (ODBC)). After you've set up an ODBC connection using the MySQL ODBC Connector, go to the External Data tab in Access, click 'other' and 'ODBC,' and connect to the tables. 

Luke Rosiak
"""

import MySQLdb
import pyExcelerator
import cookielib
import csv
import datetime
import logging
import os
import re
import sys
import urllib, urllib2
from BeautifulSoup import BeautifulSoup

CYCLES = ["12",]

from credentials import *



class ExtrasDownloader(object):
    
    def __init__(self, cycles, path=None):
        
        self.path = path or os.path.abspath(os.path.dirname(__file__))
        self.cycles = cycles
        self.db = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD,db=MYSQL_DB)
        self.cursor = self.db.cursor()
        self.dest_path = 'raw'
        
 
    #these tables all come from the multi-paned Excel worksheet: categories, members, congcmtes, congcmte_posts
    def createtables(self):
        queries = [
            "DROP TABLE IF EXISTS crp_categories;",
                """CREATE TABLE crp_categories(
	            catcode varchar (5) NOT NULL,
	            catname varchar (50) NOT NULL,
	            catorder varchar (3) NOT NULL,
	            industry varchar (20) NOT NULL,
	            sector varchar (20) NOT NULL,
	            sectorlong varchar (200) NOT NULL,
                PRIMARY KEY (catcode)
                );""",
                """CREATE TABLE IF NOT EXISTS crp_members(
	            congno INT NOT NULL,
	            cid varchar (9) NOT NULL,
	            CRPName varchar (50) NOT NULL,
	            party varchar (1) NOT NULL,
	            office varchar (4) NOT NULL,
                PRIMARY KEY (congno, cid)
                );""",
                "DROP TABLE IF EXISTS crp_congcmtes;",
                """CREATE TABLE crp_congcmtes(
	            code varchar(5) NOT NULL,
	            title varchar (70) NOT NULL,
	            INDEX (code)
                );""",
                """CREATE TABLE IF NOT EXISTS crp_congcmte_posts(
	            cid varchar(9) NOT NULL,
	            congno INT NOT NULL,
	            code varchar(5) NOT NULL,
	            position varchar (20) NOT NULL
                );""", 
                """CREATE TABLE IF NOT EXISTS crp_leadpacs(	            
                cycle int NOT NULL,
                cid varchar(10) NOT NULL,
	            cmteid varchar(10) NOT NULL
	            );""",
        ]
        

 
        cursor = self.db.cursor()
        for query in queries:
            cursor.execute(query)



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
            cursor = self.db.cursor()
            for row in rows:
                if len(row)>0:
                    sql = "INSERT INTO crp_" + table + " VALUES ("
                    for f in row:
                        f = f.decode('iso8859-1').encode('utf-8','ignore').strip()
                        sql = sql+' %s,'
                    sql = sql[:-1]+");"
                    try:
                        cursor.execute(sql,row) 
                    except:
                        print( "This FAILED:" + sql + str(row) ) 
                        logging.info( "This FAILED:" + sql + str(row) )
                        pass

        def parseExcelIDs(f):
            def sheetToRows(values):
                matrix = [[]]
                for row_idx, col_idx in sorted(values.keys()):
                    v = values[(row_idx, col_idx)]
                    if isinstance(v, unicode):
                        v = v.encode('cp866', 'backslashreplace')
                    else:
                        v = str(v)
                    last_row, last_col = len(matrix), len(matrix[-1])
                    while last_row < row_idx:
                        matrix.extend([[]])
                        last_row = len(matrix)

                    while last_col < col_idx:
                        matrix[-1].extend([''])
                        last_col = len(matrix[-1])

                    matrix[-1].extend([v])
                return matrix

            grabsheets = [('Members', 'members', [0,2,3,4]), ('CRP Industry Codes', 'categories', [0,1,2,3,4,5]), 
                ('Congressional Cmte Codes', 'congcmtes',[0,1]), ('Congressional Cmte Assignments', 'congcmte_posts', [0,2,3,4])] 

            #members: 0,2,4,3 for 2012

            for sheet_name, values in pyExcelerator.parse_xls(f): 
                matrix = [[]]
                sheet_title = sheet_name.encode('cp866', 'backslashreplace')
                for sheet_info in grabsheets:
                    if sheet_title.startswith(sheet_info[0]):
                        matrix = sheetToRows(values)
                        newmatrix = []
                        prefix = None #special case-make this the first value for all records in worksheet
                        if sheet_title.startswith('Members'):
                            prefix = sheet_title[-5:-2]
                        for row in matrix:
                            if len(row)>0 and not row[1].startswith("This information is being made available"):
                                newrow = []
                                if prefix:
                                    newrow.append(prefix)
                                for i in sheet_info[2]:
                                    if sheet_info[1]=='congcmte_posts' and i==4 and len(row)<5:
                                        thisval = ''
                                    else:
                                        thisval = row[i]
                                    try:                        
                                        newrow.append( thisval )
                                    except:
                                        logging.info( str(row) + " failed" )
                                newmatrix.append(newrow)
                        #get rid of headers
                        if sheet_info[1] in ['members', 'categories', 'congcmtes', 'congcmte_posts']:
                            newmatrix = newmatrix[1:]
                        writerows(newmatrix,sheet_info[1])


        parseExcelIDs(os.path.join(DEST_PATH,"CRP_IDs.xls"))

        leadpacs = []
        r = re.compile( r'strID=C(\d+)">(.{5,50})</a>\s*</td>\s*<td>\s*<a href="/politicians/summary.php\?cid=N(\d{8})')
        for year in CYCLES:
            html = urllib2.urlopen("http://www.opensecrets.org/pacs/industry.php?txt=Q03&cycle=20"+year).read()
            table = BeautifulSoup(html).findAll('table')[2]
            rows = table.findAll('tr')
            for row in rows[1:]:
                cells = row.findAll('td')
                cmteid = cells[0].a['href'][-9:]
                if cells[1].a:
                    cid = cells[1].a['href'][len('/politicians/summary.php?cid='):][:9]
                    pair = ["20"+year, cid, cmteid]
                    if pair not in leadpacs:
                        leadpacs.append(pair)
        writerows(leadpacs,"leadpacs")    

        
 


if __name__ == '__main__':
    cycles = sys.argv[1:]
    dl = ExtrasDownloader(cycles)
    dl.createtables()
    dl.populatetables()
