#!/usr/bin/env python3
# crawler.py
#http://docs.aiohttp.org/en/stable/client_advanced.html#proxy-support
#https://realpython.com/async-io-python/
"""Asynchronously get links embedded in multiple pages' HMTL."""
luminati_proxy = 'http://lum-customer-hl_6dfb912f-zone-static:q6a5892b84p9@zproxy.lum-superproxy.io:22225'

import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
from bs4 import BeautifulSoup
import aiofiles
import aiohttp
from aiohttp import ClientSession
import json
import time

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True

HREF_RE = re.compile(r'href="(.*?)"')

def get_chunks(my_list, chunk_size):
    for i in range(0, len(my_list), chunk_size):
        yield my_list[i:i+chunk_size]


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.

    kwargs are passed to `session.request()`.
    """

    resp = await session.request(method="GET", url=url, proxy = luminati_proxy, **kwargs)
    resp.raise_for_status()
    logger.info("Got response [%s] for URL: %s", resp.status, url)
    html = await resp.text()
    return resp.status, html

async def parse(url: str, session: ClientSession, **kwargs) -> set:
    """Find HREFs in the HTML of `url`."""
    found = set()
    try:
        status, html = await fetch_html(url=url, session=session, **kwargs)
    except (
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return found
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return found
    else:
        return status, html

async def write_one(file: IO, url: str, **kwargs) -> None:
    """Write the found HREFs from `url` to `file`."""
    res = await parse(url=url, **kwargs)
    if not res:
        return None
    res_dict = {
            'url':url,
            'status':res[0],
            'html':str(res[1])}
    async with aiofiles.open(file, "a") as f:
        await f.write(json.dumps(res_dict, indent=4)+',\n')
        logger.info("Wrote results for source URL: %s", url)

async def bulk_crawl_and_write(file: IO, urls: list, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(
                write_one(file=file, url=url, session=session, **kwargs)
            )
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent

    with open(here.joinpath("mobi_urls.txt")) as infile:
        urls = list(map(str.strip, infile))
    
    outpath = here.joinpath("mobi_all_content.json")
    with open(outpath, "w") as outfile:
        outfile.write("{\n\"items\" : [\n")
    start_time = time.time()
    for i, chunk in enumerate(get_chunks(urls, 25)):
        print(f"chunk {i}")
        asyncio.run(bulk_crawl_and_write(file=outpath, urls=chunk))
    duration = time.time() - start_time
    print(f"downloaded and wrote {len(urls)} in {duration} seconds")
    with open(outpath, "a") as outfile:
        outfile.write("]\n}")
