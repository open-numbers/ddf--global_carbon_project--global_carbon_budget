# check if there are new version

import os

import requests
import lxml.html
from lxml import etree

import pandas as pd

from ddf_utils.factory.common import download


VERSION = 2019
PAGE = 'https://www.icos-cp.eu/global-carbon-budget-2019'
ARCHIVE_PAGE = 'https://www.globalcarbonproject.org/carbonbudget/archive.htm'
SOURCE_FILE_DIR = '../source/'


def get_source_file_links(html):
    res = dict()
    all_links = html.xpath('//a')
    for elem in all_links:
        img = elem.find('img')
        if img is not None:
            text = etree.tostring(elem).decode('utf-8')
            if 'Global Budget' in text:
                res['global'] = elem.attrib['href']
            if 'National Emissions' in text:
                res['nation'] = elem.attrib['href']
    return res


def new_version_p(html):
    table = pd.read_html(html)[-1]
    cols = table.iloc[0]
    table = table.iloc[1:]
    table.columns = cols
    return str(VERSION) in table['Year'].values


def main():
    # check new version
    html = requests.get(ARCHIVE_PAGE).content
    if new_version_p(html):
        raise ValueError('new version detected! please check and update the scripts.')

    # download source files
    html = lxml.html.fromstring(requests.get(PAGE).content)
    source_links = get_source_file_links(html)

    print('source links:')
    print(source_links)
    assert ('global' in source_links and
            'nation' in source_links), 'source links not correct, please check log'

    download(source_links['global'], os.path.join(SOURCE_FILE_DIR, 'global.xlsx'))
    download(source_links['nation'], os.path.join(SOURCE_FILE_DIR, 'nation.xlsx'))
    print('updated source files')


if __name__ == '__main__':
    main()
