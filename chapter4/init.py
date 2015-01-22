import sys
import searchengineMysql as searchengine
import nnMysql as nn

reload(searchengine)
crawler = searchengine.crawler('test')
# crawler.createindextables()
# page = ['http://localhost:4000/test.html']
# crawler.crawl(page)
#
engine = searchengine.searcher('test')
engine.query('test link')

reload(nn)
mynet = nn.searchnet('test')
# mynet.maketables()
wWorld, wRiver, wBank = 101, 102, 103
uWorldBank, uRiver, uEarth = 201, 202, 203
mynet.generatehiddennode([wWorld, wBank], [uWorldBank, uRiver, uEarth])
mynet.cur.execute('select * from wordhidden')
for c in mynet.cur.fetchall(): print c
mynet.trainquery([wWorld, wBank], [uWorldBank, uRiver, uEarth], uWorldBank)
print mynet.getresult([wWorld, wBank], [uWorldBank, uRiver, uEarth])