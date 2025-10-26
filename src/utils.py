import aiohttp
from pexels_api import API

async def search_images_pexels(api_key: str, query: str, page: int, per_page: int):
    api = API(api_key)
    try:
        api.search(query, page=page, results_per_page=per_page)

        photos = api.get_entries()
        if photos:
            return [photo.original for photo in photos]
        return []
    except Exception as e:
        print(f"An error occurred with Pexels API: {e}")
        return []

async def download_url(url: str, headers: dict, session: aiohttp.ClientSession):
    try:
        async with session.get(url, headers=headers, timeout=30) as response:
            response.raise_for_status()
            return await response.read()
    except aiohttp.ClientError as e:
        print(f"Failed to download {url}: {e}")
        return None

async def download_batch_pexels(
        api_key: str,
        label: str,
        session: aiohttp.ClientSession,
        seen_urls: set,
        page: int = 1):
    headers = {"User-Agent": "Mozilla/5.0"}

    image_urls = await search_images_pexels(api_key, label, page=page, per_page=5)
    
    if not image_urls:
        yield None, None
        return

    for url in image_urls:
        if url in seen_urls:
            continue
        
        data = await download_url(url, headers, session)
        if data:
            yield data, url
            return