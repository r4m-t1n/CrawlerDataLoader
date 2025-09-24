from typing import Union, List
import aiohttp
import random
import os
import aiofiles
from utils import download_batch

class Dataset:
    def __init__(
            self, labels: List[str],
            batch_size: int = 16, num_samples: int = None,
            transform = None, random_state: int = None,
            min_shape: Union[List[int], tuple[int]] = None,
            max_shape: Union[List[int], tuple[int]] = None,
            min_size: int = None, max_size: int = None,
            save_path: str = None):

        self.labels = labels
        self.num_samples = num_samples
        self.batch_size = batch_size

        if random_state is not None:
            random.seed(random_state)

        self.transform = transform
        self.min_shape = min_shape
        self.max_shape = max_shape
        self.min_size = min_size
        self.max_size = max_size

        self.labels_first_img = {l:0 for l in labels}
        self.downloaded_count = 0
        self.seen_urls = set()

        self.save_path = save_path
        if self.save_path:
            os.makedirs(self.save_path, exist_ok=True)
            self.save_counters = {label: 0 for label in self.labels}


    async def load_batch(self):
        async with aiohttp.ClientSession() as session:
            while self.num_samples is None or self.downloaded_count < self.num_samples:
                if self.num_samples is not None and self.downloaded_count >= self.num_samples:
                    break

                label = random.choice(self.labels)
                async for image, first_img, url in download_batch(
                        label, session,
                        seen_urls=self.seen_urls,
                        min_shape=self.min_shape, max_shape=self.max_shape,
                        min_size=self.min_size, max_size=self.max_size,
                        first_img=self.labels_first_img[label],
                        batch_size=1
                    ):
                    
                    self.labels_first_img[label] = first_img
                    self.seen_urls.add(url)

                    if self.save_path:
                        file_counter = self.save_counters[label]
                        file_name = f"{label}_{file_counter}.jpg"
                        file_path = os.path.join(self.save_path, file_name)
                        
                        async with aiofiles.open(file_path, 'wb') as f:
                            await f.write(image)
                        
                        self.save_counters[label] += 1

                    if self.transform:
                        image = self.transform(image)
                    
                    self.downloaded_count += 1
                    yield image, label
                    break