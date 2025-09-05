from typing import Union, List
import aiohttp
import random
from utils import download_batch

class Dataset:
    def __init__(
            self, labels: List[str],
            num_samples: int, batch_size: int = 16,
            transform = None, random_state: int = None,
            min_shape: Union[List[int], tuple[int]] = None,
            max_shape: Union[List[int], tuple[int]] = None,
            min_size: int = 64, max_size: int = 128):

        self.labels = labels
        #self.num_samples = num_samples for now
        self.batch_size = batch_size

        if random_state is not None:
            random.seed(random_state)

        self.transform = transform

        self.min_shape = min_shape
        self.max_shape = max_shape

        self.min_size = min_size
        self.max_size = max_size

        self.labels_first_img = {l:0 for l in labels}

    async def load_batch(self):
        async with aiohttp.ClientSession() as session:
            for _ in range(self.batch_size):
                label = random.choice(self.labels)
                async for image, first_img in download_batch(
                        label, session,
                        self.min_shape, self.max_shape,
                        self.min_size, self.max_size,
                        self.labels_first_img[label],
                        batch_size=1
                    ):
                    self.labels_first_img[label] = first_img
                    if self.transform:
                        image = self.transform(image)
                    yield image, label
                    break