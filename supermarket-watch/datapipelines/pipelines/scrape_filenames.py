import logging
import re
import time
import sys
import os

import requests
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

link_re = re.compile('PriceFull([0-9]+)[-_]([0-9]+)[-_]([0-9]+)\.(?:gz|zip)')
page_re = re.compile('page=([0-9]+)')

CEREBRUS_FORM_DATA="sEcho=1&iColumns=5&sColumns=%2C%2C%2C%2C&iDisplayStart=0&iDisplayLength=9999&mDataProp_0=fname&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=type&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=false&mDataProp_2=size&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=ftime&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=false&sSearch=&bRegex=false&iSortingCols=0&cd=%2F"

def process_resource(sources):

    count = 0

    for src in sources:
        logging.info('%r', src)
        if src['kind'] == 'cerebrus':
            sess = requests.Session()
            page = pq(sess.get(src['url'], verify = False).text)
            csrftoken = pq(page.find('#csrftoken')).attr('value')
            _ = sess.post(src['url']+'login/user',
                          data={'csrftoken': csrftoken,
                                'username': src['username'],
                                'password': src['password'],
                                'Submit': 'Sign in'
                                }).text
            data = sess.post(src['url']+'file/ajax_dir?'+CEREBRUS_FORM_DATA).json()
            data = data['aaData']
            logging.info('DATA %r', len(data))
            for datum in data:
                if datum['type'] != 'file':
                    continue
                href = datum['fname']
                match = link_re.search(href)
                if match:
                    rec = dict(zip(
                        ['chain', 'branch', 'timestamp'], match.groups()
                    ))
                    rec.update({
                        'origin_url': src['url']+'file/d/',
                        'url': href,
                        'cookies': list(sess.cookies.items()),
                        'chain_name': src['name']
                    })
                    yield rec

        elif src['kind'] == 'web-folder':
            tried = set()
            urls = [src['url']]
            while len(urls) > 0:
                url = urls.pop(0)
                if url in tried:
                    continue
                tried.add(url)
                logging.info('Trying URL %s', url)
                page = pq(requests.get(url, verify=False).text)
                anchors = page.find('a')
                for anchor in anchors:
                    href = pq(anchor).attr('href')
                    if href.startswith('/') or \
                            href.startswith('?') or \
                            href.startswith('#') or \
                            href.startswith('..') or \
                            '://' in href:
                        continue
                    match = link_re.search(href)
                    if match:
                        if href in tried:
                            continue
                        tried.add(href)
                        rec = dict(zip(
                            ['chain', 'branch', 'timestamp'], match.groups()
                        ))
                        rec.update({
                            'origin_url': url,
                            'url': href,
                            'chain_name': src['name']
                        })
                        # logging.info('%r', rec)
                        yield rec
                    else:
                        urls.append(os.path.join(url, href))

        elif src['kind'] == 'web-table':
            page_num = 1
            max_page = 1
            while page_num <= max_page:
                params = {}
                if page_num > 1:
                    params['page'] = page_num
                page = pq(requests.get(src['url'], params=params, verify=False).text)
                links = page.find('a')
                got = False
                for link_ in links:
                    link = pq(link_)
                    href = link.attr('href')
                    if href is None:
                        continue
                    match = page_re.search(href)
                    if match:
                        max_page = max(max_page, int(match.groups()[0]))
                        continue
                    match = link_re.search(href)
                    if not match:
                        continue
                    rec = dict(zip(
                        ['chain', 'branch', 'timestamp'], match.groups()
                    ))
                    rec.update({
                        'origin_url': src['url'],
                        'url': href,
                        'chain_name': src['name']
                    })
                    yield rec
                    count += 1
                    sys.stdout.flush() 
                    got = True
                if got:
                    time.sleep(60)
                page_num += 1



datapackage['resources'][0]['schema']['fields'] = [
    { 'name': 'origin_url', 'type': 'string'},
    { 'name': 'url', 'type': 'string'},
    { 'name': 'cookies', 'type': 'array'},
    { 'name': 'chain_name', 'type': 'string'},
    { 'name': 'chain', 'type': 'string'},
    { 'name': 'branch', 'type': 'string'},
    { 'name': 'timestamp', 'type': 'date', 'format': 'fmt:%Y%m%d%H%M'},
]

spew(datapackage, [process_resource(next(res_iter))])
