import os
import asyncio
import logging
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# external packages
import aiohttp
from tabula import read_pdf
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
springer_home = 'https://link.springer.com'


async def download_book_async(session, executor, name, link, output):
    logging.info(f'downloading {name}...')

    try:
        async with session.get(link, verify_ssl=False) as resp:
            soup = BeautifulSoup(await resp.text(), 'html.parser')
            path = soup.find('a', {'class': 'test-bookpdf-link'})['href']

        async with session.get(f'{springer_home}{path}', verify_ssl=False) as resp:
            loop = asyncio.get_running_loop()
            with open(f'{output / name}.pdf', 'wb') as fd:
                content = await resp.read()
                await loop.run_in_executor(executor, fd.write, content)

    except Exception as e:
        logging.error(e)


async def bound_download(sem, *fetch_args):
    async with sem:
        await download_book_async(*fetch_args)


async def main():
    parser = argparse.ArgumentParser(
        prog='springer.py', description='Download springer books')
    parser.add_argument(
        'input_path', help='Path to the PDF file containing links', type=str)
    parser.add_argument('output_path',
                        help='Path to the output directory for storing the files', type=str)

    args = parser.parse_args()

    inp_dir = Path(args.input_path)
    out_dir = Path(args.output_path)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    table = read_pdf(inp_dir, pages='all', multiple_tables=False)
    links = table[0]['Unnamed: 4'].tolist()[1:]
    names = table[0]['Unnamed: 1'].tolist()[1:]

    sem = asyncio.Semaphore(50)
    executor = ThreadPoolExecutor(50)

    async with aiohttp.ClientSession(read_timeout=600) as session:
        aws = [
            bound_download(sem, session, executor, name, link, out_dir) for name, link in zip(names, links)
            if isinstance(name, str)
        ]

        await asyncio.gather(*aws, return_exceptions=False)

    logging.info('DONE.')


if __name__ == '__main__':
    asyncio.run(main())
