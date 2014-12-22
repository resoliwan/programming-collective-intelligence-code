import sys
import searchengineMysql as searchengine


reload(searchengine)
crawler =searchengine.crawler('test')
crawler.createindextables()
page = ['http://localhost:4000/test.html']
crawler.crawl(page)

engine = searchengine.searcher('test')
engine.query('test link')



