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
import urllib, urllib2

from credentials import *

LOGIN_URL = "http://www.opensecrets.org/MyOS/index.php"
MYOSHOME_URL = "http://www.opensecrets.org/MyOS/home.php"
BULKDATA_URL = "http://www.opensecrets.org/MyOS/bulk.php"
DOWNLOAD_URL = "http://www.opensecrets.org/MyOS/download.php?f=%s"

REQUEST_HEADERS = {
    "User-Agent": "CRPPYDWNLDR v1.0 ~ CRP Python Downloader",
}

META_FIELDS = ['filename','ext','description','filesize','updated','url']


class CRPDownloader(object):
    
    def __init__(self, email, password, path=None, cycles=CYCLES):
        
        self.email = email
        self.password = password
        self.path = path or os.path.abspath(os.path.dirname(__file__))
        self.cycles = cycles

        alwaysupdate = ['CRP_PFDRangeData.xls', 'CRP_Categories.txt', 'CRP_IDs.xls']
        self.to_update = alwaysupdate #don't redownload files that haven't changed, or drop and re-populate their tables
        
        # setup opener
        self.opener = urllib2.build_opener(
            urllib2.HTTPCookieProcessor(
                cookielib.LWPCookieJar()
            )
        )
        
        self.meta = { }
        meta_path = os.path.join(self.path, 'meta.csv')
        if os.path.exists(meta_path):
            reader = csv.DictReader(open(meta_path, 'r'), fieldnames=META_FIELDS)
            for record in reader:
                self.meta[record['url']] = record
    
    
    def bulk_download(self, resources):
        
        meta_file = open(os.path.join(self.path, 'meta.csv'), 'w+')
        meta = csv.DictWriter(meta_file, fieldnames=META_FIELDS)
        
        for res in resources:
            if res['url'] in self.meta:
                if res['updated'] == self.meta[res['url']]['updated']:
                    self.to_update.append(res['filename']) ##
                    logging.info('ignoring %s.%s, local file is up to date' % (res['filename'], res['ext']))
                    meta.writerow(self.meta[res['url']])
                    continue
            
            file_path = os.path.join(self.path, "%s.%s" % (res['filename'], res['ext']))
            
            logging.info('downloading %s.%s' % (res['filename'], res['ext']))
            
            filename = res['filename'] + '.' + res['ext']
            self.to_update.append(filename)
            r = self.opener.open(res['url'])
            
            CHUNK = 16 * 1024
            with open(file_path, 'wb') as outfile:
              while True:
                chunk = r.read(CHUNK)
                if not chunk: break
                outfile.write(chunk)
                            
            outfile.close()
            
            res['filesize'] = "%iMB" % (os.path.getsize(file_path) / 1024 / 1024)
            
            meta.writerow(res)
        
        


        
    def get_resources(self):
        
        now = datetime.datetime.now()
        updated = now.date().isoformat()
        
        resources = []
        
        # "visit" myos page and authenticate
        r = self.opener.open(LOGIN_URL)
        params = urllib.urlencode({'email': self.email, 'password': self.password, 'Submit': 'Log In'})
        r = self.opener.open(LOGIN_URL, params)

        # get bulk download url
        r = self.opener.open(BULKDATA_URL)

        #TODO: use new XML file instead http://www.opensecrets.org/myos/odata_meta.xml
        DL_RE = re.compile(r'<li>\s*<a href="download.php\?f=(?P<filename>\w+)\.(?P<ext>\w{3})">(?P<description>.+?)</a>\s*(?P<filesize>\d{1,3}MB) -- Last updated: (?P<updated>\d{1,2}/\d{1,2}/\d{2})\s*</li>', re.I | re.M)

        for m in DL_RE.findall(r.read()):
            res = dict(zip(['filename','ext','description','filesize','updated'], m))
            res['url'] = DOWNLOAD_URL % "%s.%s" % (res['filename'], res['ext'])

            #ignore PDF and 527, for now at least, as well as cycles we don't want
            if res['filename'][-2:] in self.cycles or res['filename']=='Lobby': 
                resources.append(res)  
        
        #these reference files don't have 'last updated' dates, so we have to assume they're new
        # PFD data range spreadsheet       
        resources.append({
            'filename': 'CRP_PFDRangeData',
            'ext': 'xls',
            'description': 'PFD Range Data',
            'filesize': None,
            'updated': updated,
            'url': 'http://www.opensecrets.org/downloads/crp/CRP_PFDRangeData.xls',
        })    
        
        # CRP category codes        
        resources.append({
            'filename': 'CRP_Categories',
            'ext': 'txt',
            'description': 'CRP Category Codes',
            'filesize': None,
            'updated': updated,
            'url': 'http://www.opensecrets.org/downloads/crp/CRP_Categories.txt',
        })    
        
        # a whole host of CRP IDs        
        resources.append({
            'filename': 'CRP_IDs',
            'ext': 'xls',
            'description': 'CRP ID spreadsheet',
            'filesize': None,
            'updated': updated,
            'url': 'http://www.opensecrets.org/downloads/crp/CRP_IDs.xls',
        })
        
        return resources


    def extract(self, src_path, dest_path):
        
        #for f in os.listdir(src_path):
        for f in self.to_update:
            
            fpath = os.path.join(src_path, f)
        
            if f.endswith('.zip'):
                cmd = 'unzip -u %s -d %s' % (fpath, dest_path)
            else:
                cmd = 'cp %s %s' % (fpath, dest_path)
                
            logging.info( cmd )
            os.system(cmd)


    def createtables(self):
        queries = {
            'campaignfin': ["DROP TABLE IF EXISTS crp_cmtes;",
                """CREATE TABLE crp_cmtes (
	            Cycle char(4) NOT NULL,
	            CmteID char(9) NOT NULL,
	            PACShort varchar(40) NULL,
	            Affiliate varchar(40) NULL,
	            UltOrg varchar(40) NULL,
	            RecipID char(9) NULL,
	            RecipCode char(2) NULL,
	            FECCandID char(9) NULL,
	            Party char(1) NULL,
	            PrimCode char(5) NULL,
	            Src char(10) NULL,
                Sens char(1) NULL,
	            Frgn int NOT NULL,
	            Actve int NULL,
                PRIMARY KEY (Cycle, CmteID)
                );""",
                "DROP TABLE IF EXISTS crp_cands;",
                """CREATE TABLE crp_cands(
	            Cycle char(4) NOT NULL,
	            FECCandID char(9) NOT NULL,
	            CID char(9) NOT NULL,
	            FirstLastP varchar(40) NULL,
	            Party char(1) NULL,
	            DistIDRunFor char(4) NULL,
	            DistIDCurr char(4) NULL,
	            CurrCand char(1) NULL,
	            CycleCand char(1) NULL,
	            CRPICO char(1) NULL,
	            RecipCode char(2) NULL,
	            NoPacs char(1) NULL,
                PRIMARY KEY (Cycle, FECCandID),
                INDEX (CID)
                );""", 
                "DROP TABLE IF EXISTS crp_indivs;",    
                """CREATE TABLE crp_indivs(
	            Cycle char(4) NOT NULL,
	            FECTransID char(7) NOT NULL,
	            ContribID char(12) NULL,
	            Contrib varchar(34) NULL,
                RecipID char(9) NULL,
	            Orgname varchar(40) NULL,
	            UltOrg varchar(40) NULL,
	            RealCode char(5) NULL,
	            Date date NOT NULL,
	            Amount int NULL,
                street varchar(20) NULL,
	            City varchar (18) NULL,
	            State char (2) NULL,
                Zip char (5) NULL,
	            Recipcode char (2) NULL,
	            Type char(3) NULL,
	            CmteID char(9) NULL,
	            OtherID char(9) NULL,
	            Gender char(1) NULL,
	            FECOccEmp varchar(35) NULL,
                Microfilm varchar(11) NULL,
	            Occ_EF varchar(38) NULL,
	            Emp_EF varchar(38) NULL,
                Src char(5) NULL,
                lastname varchar(20),
                first varchar(10),
                first3 varchar(3),
                INDEX (Orgname),
                PRIMARY KEY (Cycle, FECTransID)
                );""",
                "DROP TABLE IF EXISTS crp_pacs;",
                """CREATE TABLE crp_pacs (
	            Cycle char(4) NOT NULL,
	            FECRecNo char(7)  NOT NULL,
                PACID char(9)  NOT NULL,
	            CID char(9)  NULL,
	            Amount int,
	            Date datetime NULL,
	            RealCode char(5)  NULL,
	            Type char(3)  NULL,
	            DI char(1)  NOT NULL,
	            FECCandID char(9)  NULL,
                INDEX (Cycle, PACID)
                );""",
                "DROP TABLE IF EXISTS crp_pac_other;",
                """CREATE TABLE crp_pac_other (
	            Cycle char(4) NOT NULL,
	            FECRecNo char(7)  NOT NULL,
	            FilerID char(9)  NOT NULL,
	            DonorCmte varchar(40)  NULL,
	            ContribLendTrans varchar(40)  NULL,
	            City varchar(18)  NULL,
	            State char(2)  NULL,
	            Zip char(5)  NULL,
	            FECOccEmp varchar(35)  NULL,
	            PrimCode char(5)  NULL,
	            Date datetime NULL,
	            Amount float NULL,
	            RecipID char(9)  NULL,
	            Party char(1)  NULL,
	            OtherID char(9)  NULL,
	            RecipCode char(2)  NULL,
	            RecipPrimcode char(5)  NULL,
	            Amend char(1)  NULL,
	            Report char(3)  NULL,
	            PG char(1)  NULL,
	            Microfilm char(11)  NULL,
	            Type char(3)  NULL,
	            Realcode char(5)  NULL,
	            Source char(5)  NULL
                );""",],
            'expends': ["DROP TABLE IF EXISTS crp_expends;",
                """CREATE TABLE crp_expends(
	            Cycle char(4) NOT NULL,
                recordnum INT NULL,
	            TransID char(20) ,
	            CRPFilerid char(9) ,
	            recipcode char(2) ,
	            pacshort varchar(40) ,
	            CRPRecipName varchar(90) ,
	            ExpCode char(3) ,
	            Amount decimal(12, 0) NOT NULL,
	            Date datetime NULL,
	            City varchar(18) ,
	            State char(2) ,
	            Zip char(9) ,
	            CmteID_EF char(9) ,
	            CandID char(9) ,
	            Type char(3) ,
	            Descrip varchar(100) ,
	            PG char(5) ,
	            ElecOther varchar(20) ,
	            EntType char(3) ,
	            Source char(5) 
                );""", ],
            'lobby': ["DROP TABLE IF EXISTS crp_lobbying;",
                """CREATE TABLE crp_lobbying(
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
                "DROP TABLE IF EXISTS crp_lobbyists;",
                """CREATE TABLE crp_lobbyists(
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
                "DROP TABLE IF EXISTS crp_lobbyindus;",
                """CREATE TABLE crp_lobbyindus(
	            client varchar(40) NULL,
	            sub varchar(40) NULL,
	            total float NULL,
	            year char(4) NULL,
	            catcode char(5) NULL
                );""",
                "DROP TABLE IF EXISTS crp_lobbyagency;",
                """CREATE TABLE crp_lobbyagency(
	            uniqID varchar(56) NOT NULL,
	            agencyID char(4) NOT NULL,
	            Agency varchar(80) NULL,
                INDEX u (uniqID)
                );""",
                "DROP TABLE IF EXISTS crp_lobbyissue;",
                """CREATE TABLE crp_lobbyissue(
	            SI_ID int NOT NULL,
	            uniqID varchar(56) NOT NULL,
	            issueID char(3) NOT NULL,
	            issue varchar(50) NULL,
	            SpecificIssue varchar(255) NULL,
	            year char (4) NULL
                );""",
                "DROP TABLE IF EXISTS crp_lob_bills;",
                """CREATE TABLE crp_lob_bills(
	            B_ID int NULL,
	            si_id int NULL,
	            CongNo char(3) NULL,
                Bill_Name varchar(15) NOT NULL
                );""",
                "DROP TABLE IF EXISTS crp_lob_rpt;",
                """CREATE TABLE crp_lob_rpt(
	            TypeLong varchar (50) NOT NULL,
	            Typecode char(4) NOT NULL
                );""",],
            'CRP_IDs': #these tables all come from the multi-paned Excel worksheet: categories, members, congcmtes, congcmte_posts
                ["DROP TABLE IF EXISTS crp_categories;",
                """CREATE TABLE crp_categories(
	            catcode varchar (5) NOT NULL,
	            catname varchar (50) NOT NULL,
	            catorder varchar (3) NOT NULL,
	            industry varchar (20) NOT NULL,
	            sector varchar (20) NOT NULL,
	            sectorlong varchar (200) NOT NULL,
                PRIMARY KEY (catcode)
                );""",
                "DROP TABLE IF EXISTS crp_members;",
                """CREATE TABLE crp_members(
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
                "DROP TABLE IF EXISTS crp_congcmte_posts;",
                """CREATE TABLE crp_congcmte_posts(
	            cid varchar(9) NOT NULL,
	            congno INT NOT NULL,
	            code varchar(5) NOT NULL,
	            position varchar (20) NOT NULL
                );""", ],
            'expendcodes': ["DROP TABLE IF EXISTS crp_expendcodes;",
                """CREATE TABLE crp_expendcodes(
	            expcode varchar(3) NOT NULL,
	            descrip_short varchar(20) NOT NULL,
	            descrip varchar(50) NOT NULL,
	            sector varchar(1) NOT NULL,
	            sectorname varchar(50) NOT NULL,
                PRIMARY KEY (expcode)
                );""",],
            'leadpacs': ["DROP TABLE IF EXISTS crp_leadpacs;",
                """CREATE TABLE crp_leadpacs(
	            cid varchar(10) NOT NULL,
	            cmteid varchar(10) NOT NULL
	            );""",]
        }


        expendcodes = """0	not yet coded	not yet coded	0	Uncoded
    A00	Admin-Misc	Miscellaneous Administrative	A	Administrative
    A10	Admin-Travel	Travel	A	Administrative
    A20	Admin-Salaries	Salaries & Benefits	A	Administrative
    A30	Admin-Postage	Postage/Shipping	A	Administrative
    A50	Admin-Consultants	Administrative Consultants	A	Administrative
    A60	Admin-Rent/Utilities	Rent/Utilities	A	Administrative
    A70	Admin- Food/Meetings	Food/Meetings	A	Administrative
    A80	Admin-Supplies/Equip	Supplies, Equipment & Furniture	A	Administrative
    C00	Misc Campaign	Miscellaneous Campaign 	C	Campaign Expenses
    C10	Campaign Materials	Materials	C	Campaign Expenses
    C20	Campaign Polling	Polling/Surveys/Research	C	Campaign Expenses
    C30	GOTV Campaign	GOTV	C	Campaign Expenses
    C40	Campaign Events	Campaign Events	C	Campaign Expenses
    C50	Campaign Consultants	Political Consultants	C	Campaign Expenses
    C60	Campaign Direct Mail	Campaign Direct Mail	C	Campaign Expenses
    F00	Misc Fundraising	Miscellaneous Fundraising	F	Fundraising
    F40	Fundraising Events	Fundraising Events	F	Fundraising
    F50	Fundraising Consult	Fundraising Consultants	F	Fundraising
    F60	Direct Mail/TeleMkt	Fundr Direct Mail/Telemarketing	F	Fundraising
    H00	Misc-Other	Miscellaneous	H	Other
    H10	Misc-Donations	Charitable Donations	H	Other
    H20	Misc-Loan Payments	Loan Payments	H	Other
    M00	Misc Media	Miscellaneous Media	M	Media
    M10	Broadcast Media	Broadcast Media	M	Media
    M20	Print Media	Print Media	M	Media
    M30	Internet Media	Internet Media	M	Media
    M50	Media Consultants	Media Consultants	M	Media
    N99	Non-Expenditure	Non-Expenditure	N	Non-Expenditure
    R00	Misc Contribs	Miscellaneous Contributions	R	Contributions
    R10	Party Contrib	Parties (Fed & Non-federal)	R	Contributions
    R20	Candidate Contrib	Candidates (Fed & Non-federal)	R	Contributions
    R30	Committee Contrib	Committees (Fed & Non-Federal)	R	Contributions
    R90	Contrib Refunds	Contrib Refunds	R	Contributions
    T00	Misc Transfer	Miscellaneous Transfer	T	Transfers
    T10	Federal Transfer	Federal Transfer	T	Transfers
    T20	Non-Federal Transfer	Non-Federal Transfer	T	Transfers
    T30	Natl Party Transfer	National Party Transfer	T	Transfers
    T60	St/Loc Pty Transfer	State/Local Party Transfer	T	Transfers
    U10	Insufficient Info	Insufficient Info	U	Unknown
    U20	Unknown	Unknown	U	Unknown"""
        recs = expendcodes.split("\n")
        for rec in recs:
            fields = rec.strip().split("\t")
            query = "INSERT INTO crp_expendcodes VALUES ('"+fields[0]+"', '"+fields[1]+"', '"+fields[2]+"', '"+fields[3]+"', '"+fields[4]+"');"
            queries['expendcodes'].append(query)
     
        cursor = db.cursor()
        for key in queries.keys():
            querylist = queries[key] 
            for query in querylist:
                #try:
                cursor.execute(query)
                #except:
                #    logging.info( "FAILED: " + query )


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
            cursor = db.cursor()
            for row in rows:
                if len(row)>0:
                    if table=='indivs':
                        #split contrib and fam?
                        lastname = row[3].split(', ')[0]
                        first = row[3][len(lastname)+2:]
                        row.append(lastname)
                        row.append(first)
                        row.append(first[:3])
                        row[8] = reformatdate(row[8])
                    if table=='pacs':
                        row[5] = reformatdate(row[5])
                    if table=='pac_other':
                        row[10] = reformatdate(row[10])
                    if table=='expends':
                        row[9] = reformatdate(row[9])
                    if table=='lobbyagency':
                        row[1] = row[1][:3]


                    sql = "INSERT INTO crp_" + table + " VALUES ("
                    for f in row:
                        f = f.decode('iso8859-1').encode('utf-8','ignore').strip()
                        sql = sql+' %s,'
                    sql = sql[:-1]+");"
                    try:
                        cursor.execute(sql,row) 
                    except:
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


        ext = ".txt"
        
        print CYCLES
        print self.to_update

        for year in CYCLES:
            if True: #'CampFin' in self.to_update:
                writerowsfromcsv( os.path.join(dest_path, "cmtes" + year + ext), "cmtes")
                writerowsfromcsv( os.path.join(dest_path, "cands" + year + ext), "cands")
                writerowsfromcsv( os.path.join(dest_path, "indivs" + year + ext), "indivs")
                writerowsfromcsv( os.path.join(dest_path, "pacs" + year + ext), "pacs")
                writerowsfromcsv( os.path.join(dest_path, "pac_other" + year + ext), "pac_other")
            if 'Expends' in self.to_update:
                writerowsfromcsv( os.path.join(dest_path, "expends" + year + ext), "expends")

        #lobbying records aren't split up by year or cycle
        if True: #'Lobby' in self.to_update:
            writerowsfromcsv( os.path.join(dest_path, "lob_lobbying" + ext), "lobbying") 
            writerowsfromcsv( os.path.join(dest_path, "lob_lobbyist" + ext), "lobbyists") 
            writerowsfromcsv( os.path.join(dest_path, "lob_indus" + ext), "lobbyindus")
            writerowsfromcsv( os.path.join(dest_path, "lob_agency" + ext), "lobbyagency")
            writerowsfromcsv( os.path.join(dest_path, "lob_issue" + ext), "lobbyissue") 
            writerowsfromcsv( os.path.join(dest_path, "lob_bills" + ext), "lob_bills")
            writerowsfromcsv( os.path.join(dest_path, "lob_rpt" + ext), "lob_rpt")

        parseExcelIDs(os.path.join(dest_path,"CRP_IDs.xls"))
        

        leadpacs = []
        for year in CYCLES:
            f = urllib.urlopen("http://www.opensecrets.org/pacs/industry.php?txt=Q03&cycle=20"+year)
            l = f.read().replace('\n','').replace('\r','').replace('\t','')
            r = r'strID=C(\d*)">(.{5,50})</a></td><td><a href="/politicians/summary.php\?cid=N(\d{8})'
            matches = re.findall(r, l)
            for m in matches:
                pair = ["N"+m[0], "C"+m[2]]
                if pair not in leadpacs:
                    leadpacs.append(pair)
        writerows(leadpacs,"leadpacs")    

        
 


if __name__ == '__main__':

    src_path = 'download'
    dest_path = 'raw'
    if not os.path.exists(dest_path):
        os.mkdir(dest_path)
    if not os.path.exists(src_path):
        os.mkdir(src_path)

    logging.basicConfig(level=logging.DEBUG)

    db = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD,db=MYSQL_DB)
    
    dl = CRPDownloader(CRP_EMAIL, CRP_PASSWORD, cycles=CYCLES, path=src_path)
    dl.bulk_download( dl.get_resources() )

    dl.extract(src_path, dest_path)
    dl.createtables()
    dl.populatetables()

  
