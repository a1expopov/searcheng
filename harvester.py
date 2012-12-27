#!/usr/bin/env python

import urllib2
import sys
import threading as th, Queue
from lxml import etree
import pickle

CAP_RUNS_PER_HARVESTER = 25
CAP_HARVESTERS = 10


class Harvester(th.Thread):

    def __init__(self, queue, output_queue):
        th.Thread.__init__(self)
        self.queue = queue
        self.output_queue = output_queue
        self.runs = 0
        
    def run(self):
        while 1:
            url = self.queue.get()
            if url is None or self.runs >= CAP_RUNS_PER_HARVESTER:
                self.queue.task_done()
                break
            self.harvest(url)
            self.queue.task_done()
    
    def harvest(self, url):
        
        request = urllib2.Request(url)
        request.add_header(
            'User-Agent',
            'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:17.0) Gecko/20100101 Firefox/17.0')
        
        try:
            response = urllib2.urlopen(request)
        except:
            return
            
        try:
            tree = etree.parse(response, etree.HTMLParser())
        except:
            return
            
        links = tree.xpath('//a')
        for l in links:
            try:
                href = l.attrib['href']
                if 'http' in href:
                    self.output_queue.put(href)
                    self.queue.put(href)
            except:
                continue

        self.runs += 1

        
def harvest(starting_url):
    
    input_queue = Queue.Queue()
    input_queue.put(starting_url)
    
    output_queue = Queue.Queue()
    
    harvesters = []
    for i in range(CAP_HARVESTERS):
        harvester = harvester(input_queue, output_queue)
        harvesters.append(harvester)
        harvester.start()
        
    for harvester in harvesters:
        harvester.join()
    
    seen_links = {}
    while 1:
        try:
            url = output_queue.get_nowait()
            seen_links[url] = seen_links.get(url, 0) + 1
        except:
            break
            
    return seen_links
    
if __name__ == '__main__':
    
    url = sys.argv[1]
    
    with open('/home/alex/harvester/harvested.db', 'wb') as fp:
        pickle.dump(harvest(url), fp)
        
