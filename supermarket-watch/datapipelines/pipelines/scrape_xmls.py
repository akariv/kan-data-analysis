import logging
import copy
import gzip
import time
from io import BytesIO
import xml.etree.ElementTree as ET

import zipfile
import requests

from urllib.parse import urljoin

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

def get_data(url):
  while True:
    try:
      data = requests.get(url, verify=False).content
      return data
    except Exception as e:
      logging.error('Error in request %s: %s', url, e)
      time.sleep(60)


def process_resource(sources):

    for src in sources:
        orig_url = src['origin_url']
        url = src['url']
        url = urljoin(orig_url, url)
        if '.gz' in url:
            data = get_data(url)
            try:
              data = gzip.decompress(data)
            except OSError as e:
              logging.error('%s %r', e, data[:1000])
              continue
            if b'windows-1255' in data[:1000]:
                data = data.decode('windows-1255')
            else:
                data = data.decode('utf-8-sig')
        else:
            data = get_data(url)
            data = zipfile.ZipFile(BytesIO(data))
            data = data.open(data.namelist()[0]).read()
            data = data.decode('windows-1255')

        root = ET.fromstring(data)
        items = list(root.iter('Item'))
        assert len(items) > 0
        for item in items:
            rec = copy.deepcopy(src)
            rec.update(dict(
                (c.tag, c.text)
                for c in item
                if c.tag in [
                    'ItemCode',
                    'ItemName',
                    'ManufactureName',
                    'ItemPrice',
                    'ManufactureCountry',
                    'ManufactureItemDescription',
                    'UnitOfMeasure',
                ]
            ))
            yield rec


datapackage['resources'][0]['schema']['fields'].extend([
    { 'name': 'url', 'type': 'string'},
    { 'name': 'ItemName', 'type': 'string'},
    { 'name': 'ItemCode', 'type': 'string'},
    { 'name': 'ManufactureName', 'type': 'string'},
    { 'name': 'ManufactureCountry', 'type': 'string'},
    { 'name': 'ManufactureItemDescription', 'type': 'string'},
    { 'name': 'UnitOfMeasure', 'type': 'string'},
    { 'name': 'ItemPrice', 'type': 'number'},
])

spew(datapackage, [process_resource(next(res_iter))])
