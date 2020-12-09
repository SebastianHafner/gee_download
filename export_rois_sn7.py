from pathlib import Path
from download_manager import args
from download_manager.config import config
from data_processing import satellite_data
from data_processing import utils
import data_processing.building_footprints as bf
import json
import ee
import utm
import pandas as pd

SPACENET7_PATH = Path('C:/Users/shafner/urban_extraction/data/spacenet7/train')


def bounding_box(aoi_id: str):
    root_path = SPACENET7_PATH / aoi_id
    img_folder = root_path / 'images'
    all_img_files = list(img_folder.glob('**/*.tif'))
    img_file = all_img_files[0]
    arr, transform, crs = utils.read_tif(img_file)
    y_pixels, x_pixels, _ = arr.shape

    x_pixel_spacing = transform[0]
    x_min = transform[2]
    x_max = x_min + x_pixels * x_pixel_spacing

    y_pixel_spacing = transform[4]
    y_max = transform[5]
    y_min = y_max + y_pixels * y_pixel_spacing

    bbox = ee.Geometry.Rectangle([x_min, y_min, x_max, y_max], proj=str(crs)).transform('EPSG:4326')
    return bbox

if __name__ == '__main__':

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    ee.Initialize()

    # getting metadata from csv file
    metadata_file = SPACENET7_PATH.parent / 'sn7_metadata_urban_dataset.csv'
    metadata = pd.read_csv(metadata_file)

    collection = []
    for index, row in metadata.iterrows():
        aoi_id = str(row['aoi_id'])
        properties = {
            'aoi_id': aoi_id,
            'year': int(row['year']),
            'month:': int(row['month']),
            'country': str(row['country']),
            'group': int(row['group']),
        }
        # getting bounding box of area of interest
        bbox = bounding_box(aoi_id)

        collection.append(ee.Feature(bbox.centroid(), properties))

    collection = ee.FeatureCollection(collection)

    dl_task = ee.batch.Export.table.toDrive(
        collection=collection,
        description='sites_points_sn7',
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix='sites_points_sn7',
        fileFormat=cfg.DOWNLOAD.TABLE_FORMAT
    )

    dl_task.start()
