
#my server 로 검색할 경우 양쪽 단어가 다있는 url  검사
SELECT 
	A.id,
    A.urlid,
    A.wordid,
    (SELECT word FROM  wordlist WHERE A.wordid = id) as word1,
    A.location,
    B.wordid,
    (SELECT word FROM  wordlist WHERE B.wordid = id) as word2,
    B.location
FROM 
	wordlocation A, 
    wordlocation B    
WHERE 
	A.urlid = B.urlid
AND A.wordid = 16
AND B.wordid = 10;

SELECT A.id, A.urlid, A.wordid, B.word, A.location FROM wordlocation A, wordlist B WHERE A.wordid = B.id AND A.wordid = 16 ORDER BY A.id;


SELECT * FROM link;
SELECT * FROM linkwords;
SELECT * FROM urllist;
SELECT * FROM wordlinklinklist ORDER BY id;
SELECT * FROM wordlocation;


