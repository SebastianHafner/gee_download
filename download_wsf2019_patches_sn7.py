from pathlib import Path
from download_manager import args
from download_manager.config import config
from data_processing import utils
import json
import ee
import utm
import pandas as pd

SPACENET7_PATH = Path('C:/Users/shafner/datasets/spacenet7/train')


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


def epsg_utm(bbox):
    center_point = bbox.centroid()
    coords = center_point.getInfo()['coordinates']
    lon, lat = coords
    easting, northing, zone_number, zone_letter = utm.from_latlon(lat, lon)
    return f'EPSG:326{zone_number}' if lat > 0 else f'EPSG:327{zone_number}'


if __name__ == '__main__':

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    ee.Initialize()

    wsf2019 = ee.ImageCollection(f'users/{cfg.GEE_USERNAME}/urban_extraction_app/wsf2019')

    # getting metadata from csv file
    metadata_file = SPACENET7_PATH.parent / 'sn7_metadata_urban_dataset.csv'
    metadata = pd.read_csv(metadata_file)

    for index, row in metadata.iterrows():
        aoi_id = str(row['aoi_id'])
        year = int(row['year'])
        month = int(row['month'])
        quality = int(row['quality'])

        # getting bounding box of area of interest
        bbox = bounding_box(aoi_id)
        epsg = epsg_utm(bbox)

        img = wsf2019.filterBounds(bbox).mosaic().rename('wsf2019')

        img_name = f'wsf2019_{aoi_id}'

        dl_desc = f'{aoi_id}WSF2019Download'

        dl_task = ee.batch.Export.image.toDrive(
            image=img,
            region=bbox.getInfo()['coordinates'],
            description=dl_desc,
            folder=f'spacenet7_wsf2019',
            fileNamePrefix=f'wsf2019_{aoi_id}',
            scale=cfg.PIXEL_SPACING,
            crs=epsg,
            maxPixels=1e12,
            fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
        )
        dl_task.start()