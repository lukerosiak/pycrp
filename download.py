import cookielib
import csv
import datetime
import logging
import os
import re
import sys
import urllib, urllib2
import MySQLdb
from optparse import make_option

from credentials import *

from campfin import CampFinDownloader
from expends import ExpendsDownloader
from lobby import LobbyDownloader
from extras import ExtrasDownloader


POSSIBLE_SECTIONS = ['campfin','expend','lobby','extras']
cycle_re = re.compile(r"(20)?(\d{2})")

LOGIN_URL = "http://www.opensecrets.org/MyOS/index.php"
MYOSHOME_URL = "http://www.opensecrets.org/MyOS/home.php"
BULKDATA_URL = "http://www.opensecrets.org/MyOS/bulk.php"
DOWNLOAD_URL = "http://www.opensecrets.org/MyOS/download.php?f=%s"

REQUEST_HEADERS = {
    "User-Agent": "CRPPYDWNLDR v1.0 ~ CRP Python Downloader",
}


META_FIELDS = ['filename','ext','description','filesize','updated','url']

class CRPDownloader(object):
    
    def __init__(self,cycles,sections):
        
        self.email = CRP_EMAIL
        self.password = CRP_PASSWORD
        self.path = SRC_PATH
        self.cycles = cycles
        
        self.sections = sections
        
        # setup opener
        self.opener = urllib2.build_opener(
            urllib2.HTTPCookieProcessor(
                cookielib.LWPCookieJar()
            )
        )
        
        if not os.path.exists(self.path):
            os.system("mkdir %s" % self.path)
        if not os.path.exists(DEST_PATH):
            os.system("mkdir %s" % DEST_PATH)
        
        self.meta = { }
        meta_path = os.path.join(self.path, 'meta.csv')
        print meta_path
        if os.path.exists(meta_path):
            print 'exists'
            meta_file = open(meta_path, 'r').read()
            reader = csv.DictReader(meta_file, fieldnames=META_FIELDS)
            print meta_file
            for record in reader:
                self.meta[record['url']] = record
                
        else:
            logging.info("no existing meta file at %s" % meta_path)

    
    def go(self, sections, redownload=False):
        resources = self.get_resources()
        logging.info(resources)
        self._bulk_download(resources, sections, redownload)
    
    def _bulk_download(self, resources, sections, redownload=False):
        
        meta_file = open(os.path.join(self.path, 'meta.csv'), 'w+')
        meta = csv.DictWriter(meta_file, fieldnames=META_FIELDS)
        
        logging.info(self.meta)
        
        for res in resources:
            
            if not redownload and res['url'] in self.meta:
                if res['updated'] == self.meta[res['url']]['updated']:
                    logging.info('ignoring %s.%s, local file is up to date' % (res['filename'], res['ext']))
                    meta.writerow(self.meta[res['url']])
                    continue
                elif (res['filename']=='Lobby' and 'lobby' not in sections) or \
                    (res['filename'].startswith('CampaignFin') and 'campfin' not in sections) or \
                    (res['filename'].startswith('Expend') and 'expend' not in sections):
                    logging.info('ignoring %s.%s, not specified for download' % (res['filename'], res['ext']))
                    meta.writerow(self.meta[res['url']])
                    continue
            
            file_path = os.path.join(self.path, "%s.%s" % (res['filename'], res['ext']))
            
            logging.info('downloading %s.%s' % (res['filename'], res['ext']))
            
            r = self.opener.open(res['url'])
                            
            outfile = open(file_path, 'w')
            outfile.write(r.read())
            outfile.close()
            
            res['filesize'] = "%iMB" % (os.path.getsize(file_path) / 1024 / 1024)
            
            meta.writerow(res)
        
        
            self.extract(file_path, DEST_PATH)
                    
        meta_file.close()
        
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

        DL_RE = re.compile(r'<li>\s*<a href="download.php\?f=(?P<filename>\w+)\.(?P<ext>\w{3})">(?P<description>.+?)</a>\s*(?P<filesize>\d{1,3}MB) -- Last updated: (?P<updated>\d{1,2}/\d{1,2}/\d{2})\s*</li>', re.I | re.M)

        for m in DL_RE.findall(r.read()):
            res = dict(zip(['filename','ext','description','filesize','updated'], m))
            res['url'] = DOWNLOAD_URL % "%s.%s" % (res['filename'], res['ext'])
            
            if res['filename'][-2:] in self.cycles or res['filename']=='Lobby':
                resources.append(res)  
        
        # PFD data range spreadsheet
        
        """resources.append({
            'filename': 'CRP_PFDRangeData',
            'ext': 'xls',
            'description': 'PFD Range Data',
            'filesize': None,
            'updated': updated,
            'url': 'http://www.opensecrets.org/downloads/crp/CRP_PFDRangeData.xls',
        })    """
        
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
    
    
    def extract(self, filename, dest_path):
        
        (path,f) = os.path.split(filename)
        if f.endswith('.zip'):
            cmd = 'unzip -u %s -d %s' % (filename, dest_path)
        else:
            cmd = 'cp %s %s' % (filename, os.path.join(dest_path,f))
                
        logging.info( cmd )
        os.system(cmd)




if __name__ == '__main__':
    cycles = []
    sections = []

    args = sys.argv[1:]
    for arg in args:
        arg = arg.lower()
        if cycle_re.match(arg):
            year = cycle_re.match(arg).groups()[1]
            if year not in cycles: cycles.append(year)
        elif arg in POSSIBLE_SECTIONS:
            if arg not in sections: sections.append(arg)
        
    if not len(cycles): cycles = DEFAULT_CYCLES
    if not len(sections): sections = POSSIBLE_SECTIONS
    
    logging.basicConfig(level=logging.DEBUG)
    
    #dl = CRPDownloader(cycles,sections)
    #dl.go(sections)
    
    db = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD,db=MYSQL_DB)
    cursor = db.cursor()
    
    if 'campfin' in sections:
        CampFinDownloader(cursor,DEST_PATH,cycles).go()
    if 'expend' in sections:
        ExpendsDownloader(cursor,DEST_PATH,cycles).go()
    if 'lobby' in sections:
        LobbyDownloader(cursor,DEST_PATH).go()
    if 'extras' in sections:
        ExtrasDownloader(cursor,DEST_PATH,cycles).go()

