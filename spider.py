import sqlite3
import urllib
import ssl
from urlparse import urljoin
from urlparse import urlparse
from BeautifulSoup import *

# Deal with SSL certificate anomalies Python > 2.7
scontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
scontext = None

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

# Creating the PAGES table
cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')

# Creating the LINKS table
cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

# //////////////////////////////////////////////////////START FRESH CRAWLING OR RESUME PREVIOUS///////////////////////////////////////////////////////"""
# Check to see if we are already in progress...
cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1') #Random is not required any unretrieved page would do
row = cur.fetchone()
if row is not None:
    print "Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl."
else :
    starturl = raw_input('Enter web url or enter: ')
    # If nothing is entered then we set starturl to DEFAULT
    if ( len(starturl) < 1 ) : starturl = 'http://python-data.dr-chuck.net/'
    # Removing '/' if there is any at the end of our url
    if ( starturl.endswith('/') ) : starturl = starturl[:-1]
    web = starturl
    # Going to the main page if it ends with a .htm or .html
    if ( starturl.endswith('.htm') or starturl.endswith('.html') ) :
        pos = starturl.rfind('/')   # rfind() returns last match position in the string else returns -1
        web = starturl[:pos]

    # Now we add the WEB(This is the stripped version of STARTURL) to our WEBS table and add STARTURL to PAGES
    if ( len(web) > 1 ) :
        cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', ( web, ) )
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( starturl, ) )
        conn.commit()
# //////////////////////////////////////////////////////START FRESH CRAWLING OR RESUME PREVIOUS///////////////////////////////////////////////////////"""


# Get the current webs
cur.execute('''SELECT url FROM Webs''')
# We create a list and store all the URL from WEBS table
webs = list()
for row in cur:
    webs.append(str(row[0]))

print webs



#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<SELECT A RANDOM UNRETRIEVED PAGE FROM PAGES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"""
many = 0
while True:
    if ( many < 1 ) :
        sval = raw_input('How many pages:')
        if ( len(sval) < 1 ) : break
        many = int(sval)
    many = many - 1
    # Grab a random unretrieved page from PAGES table
    cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        row = cur.fetchone()
        print 'ROW',row
        fromid = row[0]
        url = row[1]
    except: # cur.fetchone exception
        print 'No unretrieved HTML pages found'
        many = 0
        break   # no unretrieved pages left in PAGES table

    print fromid, url,   # our randomly selected unretrieved page from PAGES table

#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<SELECT A RANDOM UNRETRIEVED PAGE FROM PAGES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"""



# ***********************************************  OPEN THE UNRETRIEVED PAGE AND CHECK IF ITS OK FOR OUR PURPOSE *********************************************** """

    # This is an UNRETRIEVED page, there should be no links from it...NOTE that we only get this URL from PAGES because it hasn't been visited
    # This is because if we have previously retrieved this page then, maybe while crawling it we may get links that are
    # new and some pre existing link from this url may not even exist anymore(Then we could circle back to the same page also)
    # HAVE TO THINK OF A MORE EFFICIENT WAY.
    cur.execute('DELETE from Links WHERE from_id=?', (fromid, ) )
    try:
        # Deal with SSL certificate anomalies Python > 2.7
        scontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        document = urllib.urlopen(url, context=scontext)

        # Normal Unless you encounter certificate problems
        #document = urllib.urlopen(url)


        """There are 3 possible way we can mark a page error from document.read()
        1. We get document code other than 200
        2. We get different type other than HTML/TEXT
        3. The above two is false but Beautiful Soup cant parse HTML
        """
        html = document.read()
        if document.getcode() != 200 :      # anything except code 200 is ERROR
            print "Error on page: ",document.getcode()
            cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url) )

        if 'text/html' != document.info().gettype() :
            print "Ignore non text/html page"
            cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
            conn.commit()
            continue

        print '('+str(len(html))+')',

        soup = BeautifulSoup(html)
    except KeyboardInterrupt:
        print ''
        print 'Program interrupted by user...'
        break
    except:
        print "Unable to retrieve or parse page"
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
        conn.commit()
        continue

    # Reaching here means we passed all the above 3 checks i.e. the page is good
    # If URL in table then, We UPDATE since we picked this link from PAGES table
    #cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) )
    cur.execute('UPDATE Pages SET html=? WHERE url=?', (buffer(html), url ) )
    conn.commit()
# ***********************************************  OPEN THE UNRETRIEVED PAGE AND CHECK IF ITS OK FOR OUR PURPOSE  ***********************************************'''


# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Getting links from the URL we selected and storing them in PAGES and LINKS $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    # Retrieve all of the anchor tags
    tags = soup('a')
    count = 0
    for tag in tags:
        #Get href attribute
        href = tag.get('href', None) # Returns href attribute value or None if there is no href
        if ( href is None ) : continue
        # Resolve relative references like href="/contact"
        up = urlparse(href) # Parses the URL into SCHEME, PATH etc etc
        if ( len(up.scheme) < 1 ) : #We are interested in links that are in the CURRENT DOMAIN so we dont want anything in SCHEME
            href = urljoin(url, href)
        # Handling Bookmark tags
        # Note that BOOKMARK tags are tags to the same page.
        # For example https://www.w3.org/TR/html4/struct/links.html#h-12.1.3
        # IF A PAGE HAS LINK TO ITSELF IT SHOULD NOT BE COUNTED
        ipos = href.find('#')
        if ( ipos > 1 ) : href = href[:ipos]

        # not interested in non text/html page
        if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue
        if ( href.endswith('/') ) : href = href[:-1]
        # print href
        if ( len(href) < 1 ) : continue

        # Check if the URL is in any of the webs
        # We are not interested in links that are leaving the CURRENT DOMAIN
        # We already did this check in the previous section but just to be safe
        found = False
        for web in webs:
            if ( href.startswith(web) ) :
                found = True
                break
        if not found : continue

        # Now we have a valid ANCHOR TAG that we are interested in
        # We add it to our PAGES table
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( href, ) )
        count = count + 1
        conn.commit()

        # We add this information with from_id(which is the url we opened ) and to_id(which is the current anchor tag having our href of interest) in LINKS table
        # Both from_id of the URL and to_id of the HREF are available from PAGES table
        cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
        try:
            row = cur.fetchone()
            toid = row[0]
        except:
            print 'Could not retrieve id'
            continue
        # print fromid, toid
        cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) )
        conn.commit()

    print count

cur.close()
