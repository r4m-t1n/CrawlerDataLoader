import json
from io import BytesIO
from PIL import Image
import aiohttp
from bs4 import BeautifulSoup

async def search_image_bing(
        label: str, headers: dict, session: aiohttp.ClientSession,
        first_img: int = 0, batch_size: int = 1):

    url = f"https://www.bing.com/images/async?q={label}&first={first_img}&count={batch_size}"
    async with session.get(url, headers=headers) as response:
        soup = BeautifulSoup(await response.text(), "html.parser")

    for a in soup.find_all("a", class_="iusc"):
        try:
            m = json.loads(a["m"])
            if "murl" in m:
                return m["murl"], first_img
        except:
            continue
    return None, first_img + batch_size

async def get_image_shape(
        url: str, headers: dict, session: aiohttp.ClientSession):
    try:
        async with session.get(url, headers=headers) as response:
            partial_data = b""
            async for chunk in response.content.iter_chunked(1024):
                partial_data += chunk
                if len(partial_data) >= 16 * 1024:
                    break
            img = Image.open(BytesIO(partial_data))
            return img.size
    except:
        return None

async def get_image_size(
        url: str, headers: dict, session: aiohttp.ClientSession):
    try:
        head = await session.head(url, headers=headers)
        size = int(head.headers.get("Content-Length", 0))
        return size
    except:
        return None

async def is_image_valid(
        url: str, headers: dict, session: aiohttp.ClientSession,
        min_shape=None, max_shape=None,
        min_size=None, max_size=None):

    if min_shape is not None or max_shape is not None:
        img_shape = await get_image_shape(url, headers, session)
        if not img_shape:
            return False
        w, h = img_shape

        if min_shape is not None and min(w, h) < min_shape[0]:
            return False
        if max_shape is not None and max(w, h) > max_shape[0]:
            return False

    if min_size is not None or max_size is not None:
        img_size = await get_image_size(url, headers, session)
        if not img_size:
            return False
        size_in_kb = img_size / 1024
        if min_size is not None and size_in_kb < min_size:
            return False
        if max_size is not None and size_in_kb > max_size:
            return False

    return True

async def download_url(
        url: str, headers: dict, session: aiohttp.ClientSession):
    try:
        async with session.get(url, headers=headers) as response:
            return await response.read()
    except:
        return None

async def download_batch(
        label: str, session: aiohttp.ClientSession,
        seen_urls: set,
        min_shape=None, max_shape=None,
        min_size=None, max_size=None,
        first_img=0, batch_size=1):

    headers = {"User-Agent": "Mozilla/5.0"}
    counter = 0
    while (counter < batch_size):
        url, first_img = await search_image_bing(label, headers, session, first_img, batch_size=1)
        if url is None:
            continue

        if url in seen_urls:
            first_img += 1
            continue

        valid = await is_image_valid(url, headers, session, min_shape, max_shape, min_size, max_size)
        if not valid:
            first_img += 1
            continue
        data = await download_url(url, headers, session)
        if data:
            counter += 1
            yield data, first_img, url
            first_img += 1