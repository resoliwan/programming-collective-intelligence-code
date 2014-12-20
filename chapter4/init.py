
import searchengine as searchengine

reload(searchengine)
crawler =searchengine.crawler('testserver')
#crawler.createindextables()
page = ['http://localhost:4000/test.html']
crawler.crawl(page)

