from distutils.version import StrictVersion
import hashlib
import os
import seesaw
import shutil
import time
import datetime
import urllib2
import gzip
import fnmatch
import re
from seesaw.config import NumberConfigValue, realize
from seesaw.externalprocess import WgetDownload
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import (GetItemFromTracker, SendDoneToTracker,
    PrepareStatsForTracker, UploadWithTracker)
from seesaw.util import find_executable
from cStringIO import StringIO
from urlparse import urljoin
from urllib import quote
from json import loads
from lxml import etree

# check the seesaw version
if StrictVersion(seesaw.__version__) < StrictVersion("0.1.4"):
    raise Exception("This pipeline needs seesaw version 0.1.4 or higher.")


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
WGET_LUA = find_executable(
    "Wget+Lua",
    ["GNU Wget 1.14.lua.20130523-9a5c"],
    [
        "./wget-lua",
        "./wget-lua-warrior",
        "./wget-lua-local",
        "../wget-lua",
        "../../wget-lua",
        "/home/warrior/wget-lua",
        "/usr/bin/wget-lua"
    ]
)

if not WGET_LUA:
    raise Exception("No usable Wget+Lua found.")


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20140215.02"
USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27'
TRACKER_ID = 'myopera'
HEADERS = {
        'User-Agent': USER_AGENT,
        'Accept-encoding': 'gzip'
}
TRACKER_HOST = 'tracker.archiveteam.org'
#TRACKER_HOST = 'localhost:9080'

def sleep(seconds=0.75):
    sleep_time = seconds * random.uniform(0.5, 2.0)
    time.sleep(sleep_time)

def download_url(url, headers):
    while True:
        try:
            request = urllib2.Request(url, headers=headers)
            response = urllib2.urlopen(request)
            content = response.read()
            # Is it compressed with gzip?
            if content.startswith("\x1f\x8b\x08"):
                content = StringIO(content)
                content = gzip.GzipFile(fileobj=content)
                content = content.read()
        except urllib2.HTTPError as error:
            if error.code == 404:
                raise error
            elif error.code == 503 or error.code == 500 or error.code == 403:
                sleep_time = 10
                print 'My Opera threw an error ( code', error.code, ') Sleep for', sleep_time, 'seconds.'
                time.sleep(sleep_time)
                continue # retry
            elif error.code != 200 and error.code != 404:
                print 'Unexpected error. ( code', error.code, ') Retrying.'
                sleep(seconds=5)
                continue # retry
        return content

def blogpostlist(useruri):
    ''' Copied from MyOBackup (https://github.com/karlcow/myobackup)
        Copyright (c) 2013 Karl Dubost, MIT License
        Modified to work with pipeline.py.
    '''
    # return a list of blog posts URI for a given username
    # useruri = http://my.opera.com/$USERNAME/archive/
    postlist = []
    myparser = etree.HTMLParser(encoding="utf-8")
    archivehtml = download_url(useruri, HEADERS)
    tree = etree.HTML(archivehtml, parser=myparser)
    # Check for both types of MyOpera archive
    navlinks = tree.xpath('(//p[@class="pagenav"] | //div[@class="month"]//li)//a/@href')
    # Remove the last item of the list which is the next link
    if navlinks:
	    navlinks.pop()
    # create a sublist of archives links
    archlinks = fnmatch.filter(navlinks, '?startidx=*')
    # Insert the first page of the archive at the beginning
    archlinks.insert(0, useruri)
    # making full URI
    archlinks = [urljoin(useruri, archivelink) for archivelink in archlinks]
    # we go through all the list
    for archivelink in archlinks:
        archtml = download_url(archivelink, HEADERS)
        tree = etree.HTML(archtml, parser=myparser)
        links = tree.xpath('//div[@id="arc"]//li//a/@href')
        # Getting the links for all the archive page only!
        for link in links:
            postlist.append(urljoin(useruri, link))
    print "Downloading {0} blog posts".format(str(len(postlist)))
    return postlist

def photolist(useruri):
    # Return a list of all photos
    # useruri = http://my.opera.com/$USERNAME/albums/
    piclist = []
    albumlist = []
    # First, get list of all albums
    myparser = etree.HTMLParser(encoding="utf-8")
    archivehtml = download_url(urljoin(useruri, "index.dml?page=1&perscreen=36"),\
     HEADERS)
    tree = etree.HTML(archivehtml, parser=myparser)
    # Find total number of albums
    maxAlbums = tree.xpath('//p[@class="pagenav-info"]/text()')
    if not maxAlbums:
        # <=1 albums
        maxAlbums = 1
    else:
        maxAlbums = int(re.search(r"\d+ of (\d+)", maxAlbums[0], re.DOTALL).group(1))
    # Find albums links on the first page
    albumlist.extend([urljoin("http://my.opera.com", link.values()[0])\
     for link in tree.xpath('//div[@class="albuminfo"]/a[@href]')])
    # Now grab the rest of the albums
    if maxAlbums > 36:
        # Skip albums we've already found
        archivehtml = download_url(urljoin(useruri,\
         "index.dml?perscreen={0}&skip=36".format(maxAlbums-36)), HEADERS)
        tree = etree.HTML(archivehtml, parser=myparser)
        albumlist.extend([urljoin("http://my.opera.com", link.values()[0])\
         for link in tree.xpath('//div[@class="albuminfo"]/a[@href]')])
    # Use OEmbed to get image links
    # As Opera allows only 100 images per page on the album pages,
    # this is much easier and potentially faster than crawling.
    for album in albumlist:
        picsjson = download_url('http://my.opera.com/service/oembed/?url={0}'.format(\
         quote(album)), HEADERS)
        #print "Album {0} has {1}".format(album, str(len(loads(picsjson)['images']['image'])))
        piclist.extend(loads(picsjson)['images']['image'])
    print "Downloading {0} pictures".format(str(len(piclist)))
    return piclist

###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, "PrepareDirectories")
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item["item_name"]

        # We expect a list of urls (no http:// prefix ok)
        item['url_list'] = item_name.split(',')

        for url in item['url_list']:
            item.log_output('URL: ' + url)

        # Be safe about max filename length
        truncated_item_name = hashlib.sha1(item_name).hexdigest()
        dirname = "/".join((item["data_dir"], truncated_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item["item_dir"] = dirname
        item["warc_file_base"] = "%s-%s-%s" % (self.warc_prefix,
            truncated_item_name,
            time.strftime("%Y%m%d-%H%M%S"))

        open("%(item_dir)s/%(warc_file_base)s.warc.gz" % item, "w").close()
        open("%(item_dir)s/%(warc_file_base)s_links.txt" % item, "w").close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % item,
              "%(data_dir)s/%(warc_file_base)s.warc.gz" % item)

        shutil.rmtree("%(item_dir)s" % item)


wget_args = [
    WGET_LUA,
    "-U", USER_AGENT,
    "-nv",
    "-o", ItemInterpolation("%(item_dir)s/wget.log"),
    "--lua-script", "myopera.lua",
    "--no-check-certificate",
    "--output-document", ItemInterpolation("%(item_dir)s/wget.tmp"),
    "--truncate-output",
    "-e", "robots=off",
    "--rotate-dns",
    "--page-requisites",
    "--timeout", "60",
    "--tries", "inf",
    "--waitretry", "120",
    "--no-parent",
    "--span-hosts",
    "--domains", "my.opera.com,files.myopera.com,static.myopera.com",
    "--exclude-domains", "blogs.opera.com",
    "--warc-file", ItemInterpolation("%(item_dir)s/%(warc_file_base)s"),
    "--warc-header", "operator: Archive Team",
    "--warc-header", "myopera-dld-script-version: " + VERSION,
    "--warc-header", ItemInterpolation("myopera-user: %(item_name)s"),
    "-i", ItemInterpolation("%(item_dir)s/%(warc_file_base)s_links.txt")
]

if 'bind_address' in globals():
    wget_args.extend(['--bind-address', globals()['bind_address']])
    print('')
    print('*** Wget will bind address at {0} ***'.format(globals()['bind_address']))
    print('')


class WgetArgFactory(object):
    ''' Grab a list of all blog posts and photos.
        Faster/More reliable to get the links directly, rather
        than crawl around forever.
        Can be done in Lua, but much, much easier
        to do it here.
    '''
    def realize(self, item):
        baseURL = "http://my.opera.com/{0}".format(item['url_list'][0])
        allLinks = blogpostlist(baseURL+"/archive/") + photolist(baseURL+"/albums/") +\
                   [baseURL + "/about/"] + [baseURL + "/links/"] + [baseURL + "/favorites/"]
        # Put all links into one file for wget
        open("%(item_dir)s/%(warc_file_base)s_links.txt" % item, "w").write(\
         '\n'.join(allLinks))
        return realize(wget_args, item)
         

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="My Opera",
    project_html="""
    <img class="project-logo" alt="" src="http://i.imgur.com/S5Ubz6x.png" height="50" />
    <h2>My Opera <span class="links"><a href="http://my.opera.com/">Website</a> &middot; <a href="http://%s/%s/">Leaderboard</a></span></h2>
    <p><b>Opera</b> closes its social network.</p>
    """ % (TRACKER_HOST, TRACKER_ID)
    , utc_deadline=datetime.datetime(2014, 03, 01, 00, 00, 1)
)

pipeline = Pipeline(
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader,
        VERSION),
    PrepareDirectories(warc_prefix="myopera"),
    WgetDownload(
        WgetArgFactory(),
        max_tries=5,
        accept_on_exit_code=[0, 8],
    ),
    PrepareStatsForTracker(
        defaults={ "downloader": downloader, "version": VERSION },
        file_groups={
            "data": [ ItemInterpolation("%(item_dir)s/%(warc_file_base)s.warc.gz") ]
            }
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=4, default="1",
        name="shared:rsync_threads", title="Rsync threads",
        description="The maximum number of concurrent uploads."),
        UploadWithTracker(
            "http://'tracker.archiveteam.org'/%s" % TRACKER_ID,
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation("%(data_dir)s/%(warc_file_base)s.warc.gz")
                ],
            rsync_target_source_path=ItemInterpolation("%(data_dir)s/"),
            rsync_extra_args=[
                "--recursive",
                "--partial",
                "--partial-dir", ".rsync-tmp"
            ]
            ),
    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)
