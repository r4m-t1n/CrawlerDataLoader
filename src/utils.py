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
                if len(partial_data) >= 64 * 1024:
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
    
    cond_shape = True
    if (min_shape or max_shape):
        img_shape = await get_image_shape(url, headers, session)
        if img_shape and (min_shape or max_shape):
            w, h = img_shape
            cond_shape = ((min_shape is None or min(w,h) >= min_shape) and
                        (max_shape is None or max(w,h) <= max_shape))

    cond_size = True
    if (min_size or max_size):
        img_size = await get_image_size(url, headers, session)
        if not (img_shape and img_size):
            return False

        size_in_kb = img_size / 1024
        cond_size = min_size <= size_in_kb <= max_size


    return cond_size and cond_shape

async def download_url(
        url: str, headers: dict, session: aiohttp.ClientSession):
    try:
        async with session.get(url, headers=headers) as response:
            return await response.read()
    except:
        return None

async def download_batch(
        label: str, session: aiohttp.ClientSession,
        min_shape=None, max_shape=None,
        min_size=None, max_size=None,
        first_img=0, batch_size=1):

    headers = {"User-Agent": "Mozilla/5.0"}
    counter = 0
    while (counter < batch_size):
        url, first_img = await search_image_bing(label, headers, session, first_img, batch_size=1)
        if url is None:
            continue
        valid = await is_image_valid(url, headers, session, min_shape, max_shape, min_size, max_size)
        if not valid:
            first_img += 1
            continue
        data = await download_url(url, headers, session)
        if data:
            yield data, first_img
            first_img += 1
