"""
Import OpenSecrets.org's lobbying tables to MySQL
"""

import MySQLdb
import sys
import logging
import os
import re



class LobbyDownloader(object):
    
    def __init__(self,cursor,path):
        
        self.cursor = cursor
        self.dest_path = path

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

        ext = ".txt"
        self.cursor.execute("DELETE FROM crp_lobbying")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_lobbying" + ext)))
        
        self.cursor.execute("DELETE FROM crp_lobbyist")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_lobbyist" + ext)))
        
        self.cursor.execute("DELETE FROM crp_lob_indus")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_indus" + ext)))
        
        self.cursor.execute("DELETE FROM crp_lob_agency")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_agency" + ext)))
        
        self.cursor.execute("DELETE FROM crp_lob_issue")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_issue" + ext)))
        
        self.cursor.execute("DELETE FROM crp_lob_bills")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_bills" + ext)))
        
        self.cursor.execute("DELETE FROM crp_lob_rpt")
        self.cursor.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE crp_%s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '|'" % ( os.path.join(self.dest_path, "lob_rpt" + ext)))


    def go(self):
        self.createtables()
        self.populatetables()
        


  
