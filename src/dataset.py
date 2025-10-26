from typing import List
import aiohttp
import random
import os
import aiofiles
from utils import download_batch_pexels
from PIL import Image
from io import BytesIO

class Dataset:
    def __init__(
            self, labels: List[str],
            api_key: str,
            num_samples: int,
            transform=None, random_state: int = None,
            save_path: str = None):

        self.labels = labels
        self.api_key = api_key
        self.num_samples = num_samples
        self.transform = transform
        self.save_path = save_path

        if random_state is not None:
            random.seed(random_state)

        if self.num_samples % len(self.labels) != 0:
            raise ValueError("num_samples should be divisible by the number of labels for a balanced dataset.")

        self.samples_per_label = self.num_samples // len(self.labels)

        self.download_queue = []
        for label in self.labels:
            self.download_queue.extend([label] * self.samples_per_label)

        random.shuffle(self.download_queue)

        self.labels_page_number = {l: 1 for l in labels}
        self.seen_urls = set()
        if self.save_path:
            os.makedirs(self.save_path, exist_ok=True)
            self.save_counters = {label: 0 for label in self.labels}

    async def load_batch(self):
        async with aiohttp.ClientSession() as session:
            for label in self.download_queue:
                image_downloaded = False
                while not image_downloaded:
                    async for image_data, url in download_batch_pexels(
                            self.api_key, label, session,
                            seen_urls=self.seen_urls,
                            page=self.labels_page_number[label]
                        ):

                        if image_data is None:
                            break

                        self.seen_urls.add(url)

                        if self.save_path:
                            file_counter = self.save_counters[label]
                            file_name = f"{label}_{file_counter}.jpg"
                            file_path = os.path.join(self.save_path, file_name)

                            async with aiofiles.open(file_path, 'wb') as f:
                                await f.write(image_data)

                            self.save_counters[label] += 1

                        image_pil = Image.open(BytesIO(image_data)).convert("RGB")

                        image_tensor = image_pil
                        if self.transform:
                            image_tensor = self.transform(image_pil)

                        yield image_tensor, label
                        image_downloaded = True
                        break

                    self.labels_page_number[label] += 1