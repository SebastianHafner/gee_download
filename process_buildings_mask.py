from pathlib import Path
from data_processing.utils import read_tif, write_tif
import numpy as np
from tqdm import tqdm

DATASET_PATH = Path('C:/Users/shafner/urban_extraction/data/urban_extraction_dataset_version4')
PATCH_SIZE = 256

if __name__ == '__main__':
    buildings_path = DATASET_PATH / 'buildings'
    buildings_files = [f for f in buildings_path.glob('**/*')]
    for buildings_file in tqdm(buildings_files):
        arr, transform, crs = read_tif(buildings_file)
        mask = np.empty(arr.shape, dtype=np.uint8)
        m, n, _ = arr.shape
        for i in range(0, m, PATCH_SIZE):
            for j in range(0, n, PATCH_SIZE):
                n_building_pixels = np.sum(arr[i:i+PATCH_SIZE, j:j+PATCH_SIZE, ])
                mask_value = 0 if n_building_pixels == 0 else 1
                mask[i:i + PATCH_SIZE, j:j + PATCH_SIZE, ] = mask_value

        mask_path = DATASET_PATH / 'masks'
        mask_path.mkdir(exist_ok=True)
        roi_id = buildings_file.stem.split('_')[1]
        mask_file = mask_path / f'mask_{roi_id}.tif'
        write_tif(mask_file, mask, transform, crs)
