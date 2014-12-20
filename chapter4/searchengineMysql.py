import urllib2
from BeautifulSoup import *
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite
import MySQLdb as mdb
import sys
import nn


# Create a list of words to ignore
ignorewords = {'the': 1, 'of': 1, 'to': 1, 'and': 1, 'a': 1, 'in': 1, 'is': 1, 'it': 1}


class crawler:
    # Initialize the crawler with the name of database
    def __init__(self, dbname):
        self.con = mdb.connect('localhost', 'testuser', 'testtest', dbname)
        self.cur = self.con.cursor()

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    # Auxilliary function for getting an entry id and adding
    # it if it's not present
    def getentryid(self, table, field, value, createnew=True):
        try:
            self.cur.execute(
                "select id from %s where %s='%s'" % (table, field, value))
            res = self.cur.fetchone()
            if res == None:
                self.cur.execute(
                    "insert into %s (%s) values ('%s')" % (table, field, value))
                return self.con.insert_id()
            else:
                return res[0]
        except:
            print "Unexpected error1:", sys.exc_info()


            # Index an individual page

    def addtoindex(self, url, soup):
        if self.isindexed(url): return
        print 'Indexing ' + url

        # Get the individual words
        text = self.gettextonly(soup)
        words = self.separatewords(text)

        # Get the URL id
        urlid = self.getentryid('urllist', 'url', url)

        # Link each word to this url
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords: continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.cur.execute("insert into wordlocation(urlid,wordid,location) values (%d,%d,%d)" % (urlid, wordid, i))


    # Extract the text from an HTML page (no tags)
    def gettextonly(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext + '\n'

            return resulttext
        else:
            return v.strip()

    # Return true if this url is already indexed
    def isindexed(self, url):
        return False

    # Add a link between two pages
    def addlinkref(self, urlFrom, urlTo, linkText):
        words = self.separatewords(linkText)
        fromid = self.getentryid('urllist', 'url', urlFrom)
        toid = self.getentryid('urllist', 'url', urlTo)
        if fromid == toid: return
        self.cur.execute("insert into link(fromid,toid) values (%d,%d)" % (fromid, toid))
        linkid = self.con.insert_id()
        for word in words:
            if word in ignorewords: continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.cur.execute("insert into linkwords(linkid,wordid) values (%d,%d)" % (linkid, wordid))

    # Seperate the words by any non-whitespace character
    def separatewords(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']


    # Starting with a list of pages, do a breadth
    # first search to the given depth, indexing pages
    # as we go
    def crawl(self, pages, depth=1):
        for i in range(depth):
            newpages = {}
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Could not open %s" % page
                    continue
                try:
                    soup = BeautifulSoup(c.read())
                    self.addtoindex(page, soup)

                    links = soup('a')
                    for link in links:
                        if ('href' in dict(link.attrs)):
                            url = urljoin(page, link['href'])
                            if url.find("'") != -1: continue
                            url = url.split('#')[0]  # remove location portion
                            if url[0:4] == 'http' and not self.isindexed(url):
                                newpages[url] = 1
                            linkText = self.gettextonly(link)
                            self.addlinkref(page, url, linkText)

                    self.dbcommit()
                except:
                    print "Unexpected error:", sys.exc_info()
                    print "Could not parse page %s" % page

            pages = newpages


    # Create the database tables
    def createindextables(self):

        self.cur.execute('create table urllist(id INTEGER  AUTO_INCREMENT, url VARCHAR(500), PRIMARY KEY (id))')
        self.cur.execute('create table wordlist(id INTEGER  AUTO_INCREMENT, word VARCHAR(500), PRIMARY KEY (id))')
        self.cur.execute('create table wordlocation(id INTEGER  AUTO_INCREMENT, urlid integer,wordid integer,location VARCHAR(500), PRIMARY KEY (id))')
        self.cur.execute('create table link(id INTEGER  AUTO_INCREMENT, fromid integer,toid integer, PRIMARY KEY (id))')
        self.cur.execute('create table linkwords(id INTEGER  AUTO_INCREMENT, wordid VARCHAR(500),linkid VARCHAR(500), PRIMARY KEY (id))')
        self.cur.execute('create index wordidx on wordlist(word)')
        self.cur.execute('create index urlidx on urllist(url)')
        self.cur.execute('create index wordurlidx on wordlocation(wordid)')
        self.cur.execute('create index urltoidx on link(toid)')
        self.cur.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()

    def calculatepagerank(self, iterations=20):
        # clear out the current page rank tables
        self.cur.execute('drop table if exists pagerank')
        self.cur.execute('create table pagerank(urlid primary key,score)')

        # initialize every url with a page rank of 1
        for (urlid,) in self.cur.execute('select id from urllist'):
            self.cur.execute('insert into pagerank(urlid,score) values (%d,1.0)' % urlid)
        self.dbcommit()

        for i in range(iterations):
            print "Iteration %d" % (i)
            for (urlid,) in self.cur.execute('select id from urllist'):
                pr = 0.15

                # Loop through all the pages that link to this one
                for (linker,) in self.cur.execute(
                                'select distinct fromid from link where toid=%d' % urlid):
                    # Get the page rank of the linker
                    linkingpr = self.cur.execute(
                        'select score from pagerank where urlid=%d' % linker).fetchone()[0]

                    # Get the total number of links from the linker
                    linkingcount = self.cur.execute(
                        'select count(*) from link where fromid=%d' % linker).fetchone()[0]
                    pr += 0.85 * (linkingpr / linkingcount)
                self.cur.execute(
                    'update pagerank set score=%f where urlid=%d' % (pr, urlid))
            self.dbcommit()


class searcher:
    def __init__(self, dbname):
        self.cur = sqlite.connect(dbname)

    def __del__(self):
        self.cur.close()

    def getmatchrows(self, q):
        # Strings to build the query
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        # Split the words by spaces
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            # Get the word ID
            wordrow = self.cur.execute(
                "select id from wordlist where word='%s'" % word).fetch()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid=w%d.urlid and ' % (tablenumber - 1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1

        # Create the query from the separate parts
        fullquery = 'select %s from %s where %s' % (fieldlist, tablelist, clauselist)
        print fullquery
        self.cur.execute(fullquery)
        rows = [row for row in self.cur.fetchall()]

        return rows, wordids

    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0], 0) for row in rows])

        # This is where we'll put our scoring functions
        weights = [(1.0, self.locationscore(rows)),
                   (1.0, self.frequencyscore(rows)),
                   (1.0, self.pagerankscore(rows)),
                   (1.0, self.linktextscore(rows, wordids)),
                   (5.0, self.nnscore(rows, wordids))]
        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]

        return totalscores

    def geturlname(self, id):
        return self.cur.execute(
            "select url from urllist where id=%d" % id).fetchone()[0]

    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, wordids)
        rankedscores = [(score, url) for (url, score) in scores.items()]
        rankedscores.sort()
        rankedscores.reverse()
        for (score, urlid) in rankedscores[0:10]:
            print '%f\t%s' % (score, self.geturlname(urlid))
        return wordids, [r[1] for r in rankedscores[0:10]]

    def normalizescores(self, scores, smallIsBetter=0):
        vsmall = 0.00001  # Avoid division by zero errors
        if smallIsBetter:
            minscore = min(scores.values())
            return dict([(u, float(minscore) / max(vsmall, l)) for (u, l) in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore == 0: maxscore = vsmall
            return dict([(u, float(c) / maxscore) for (u, c) in scores.items()])

    def frequencyscore(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows: counts[row[0]] += 1
        return self.normalizescores(counts)

    def locationscore(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]: locations[row[0]] = loc

        return self.normalizescores(locations, smallIsBetter=1)

    def distancescore(self, rows):
        # If there's only one word, everyone wins!
        if len(rows[0]) <= 2: return dict([(row[0], 1.0) for row in rows])

        # Initialize the dictionary with large values
        mindistance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i] - row[i - 1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]: mindistance[row[0]] = dist
        return self.normalizescores(mindistance, smallIsBetter=1)

    def inboundlinkscore(self, rows):
        uniqueurls = dict([(row[0], 1) for row in rows])
        inboundcount = dict(
            [(u, self.cur.execute('select count(*) from link where toid=%d' % u).fetchone()[0]) for u in uniqueurls])
        return self.normalizescores(inboundcount)

    def linktextscore(self, rows, wordids):
        linkscores = dict([(row[0], 0) for row in rows])
        for wordid in wordids:
            self.cur.execute(
                'select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.id' % wordid)
            for (fromid, toid) in self.cur.fatchall():
                if toid in linkscores:
                    pr = self.cur.execute('select score from pagerank where urlid=%d' % fromid).fetchone()[0]
                    linkscores[toid] += pr
        maxscore = max(linkscores.values())
        normalizedscores = dict([(u, float(l) / maxscore) for (u, l) in linkscores.items()])
        return normalizedscores

    def pagerankscore(self, rows):
        pageranks = dict(
            [(row[0], self.cur.execute('select score from pagerank where urlid=%d' % row[0]).fetchone()[0]) for row in
             rows])
        maxrank = max(pageranks.values())
        normalizedscores = dict([(u, float(l) / maxrank) for (u, l) in pageranks.items()])
        return normalizedscores

    def nnscore(self, rows, wordids):
        # Get unique URL IDs as an ordered list
        #urlids = [urlid for urlid in dict([(row[0], 1) for row in rows])]
        #nnres = mynet.getresult(wordids, urlids)
        #scores = dict([(urlids[i], nnres[i]) for i in range(len(urlids))])
        #return self.normalizescores(scores)
        return;
