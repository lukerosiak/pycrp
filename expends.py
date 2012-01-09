import sys
import MySQLdb
import logging
import os


class ExpendsDownloader(object):
    
    def __init__(self,cursor,path,cycles):
        
        self.cursor = cursor
        self.dest_path = path
        self.cycles = cycles
        

    def createtables(self):

        query = """CREATE TABLE IF NOT EXISTS crp_expendcodes(
	            expcode varchar(3) NOT NULL,
	            descrip_short varchar(20) NOT NULL,
	            descrip varchar(50) NOT NULL,
	            sector varchar(1) NOT NULL,
	            sectorname varchar(50) NOT NULL,
                PRIMARY KEY (expcode)
                );"""
        self.cursor.execute(query)

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
        query = "SELECT count(*) from crp_expendcodes"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        if row[0]==0:
    
            recs = expendcodes.split("\n")
            for rec in recs:
                fields = rec.strip().split("\t")
                query = "INSERT INTO crp_expendcodes VALUES ('"+fields[0]+"', '"+fields[1]+"', '"+fields[2]+"', '"+fields[3]+"', '"+fields[4]+"');"
                self.cursor.execute(query)
 
        
        query = """CREATE TABLE IF NOT EXISTS crp_expends(
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
                );"""
        self.cursor.execute(query)



    def populatetables(self):
 
        ext = ".txt"
        for year in self.CYCLES:
            query = "DELETE FROM crp_expends WHERE cycle='20%s';" %year
            self.cursor.execute(query)
            self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_expends  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|' (Cycle,recordnum,TransID,CRPFilerid,recipcode,pacshort,CRPRecipName,ExpCode,Amount,@Date_orig,City,State,Zip,CmteID_EF,CandID,Type,Descrip ,PG,ElecOther,EntType,Source) SET Date = STR_TO_DATE(@Date_orig, '%m/%d/%Y')" % ( os.path.join(self.dest_path, "expends" + year + ext)))


    def go(self):
        self.createtables()
        self.populatetables()
