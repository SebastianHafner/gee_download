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

SPACENET7_PATH = Path('C:/Users/shafner/datasets/spacenet7')


def bounding_box(split: str, aoi_id: str, year: int, month: int):
    folder = SPACENET7_PATH / split / aoi_id / 'images_masked'
    arr, transform, crs = utils.read_tif(folder / f'global_monthly_{year}_{month:02d}_mosaic_{aoi_id}.tif')
    y_pixels, x_pixels, _ = arr.shape

    x_pixel_spacing = transform[0]
    x_min = transform[2]
    x_max = x_min + x_pixels * x_pixel_spacing

    y_pixel_spacing = transform[4]
    y_max = transform[5]
    y_min = y_max + y_pixels * y_pixel_spacing

    bbox = ee.Geometry.Rectangle([x_min, y_min, x_max, y_max], proj=str(crs)).transform('EPSG:4326')
    return bbox


def resolution(split: str, aoi_id: str, year: int, month: int):
    folder = SPACENET7_PATH / split / aoi_id / 'images_masked'
    _, transform, _ = utils.read_tif(folder / f'global_monthly_{year}_{month:02d}_mosaic_{aoi_id}.tif')
    return transform[0]


def epsg(split: str, aoi_id: str, year: int, month: int):
    folder = SPACENET7_PATH / split / aoi_id / 'images_masked'
    _, _, crs = utils.read_tif(folder / f'global_monthly_{year}_{month:02d}_mosaic_{aoi_id}.tif')
    return str(crs)


def epsg_utm(bbox):
    center_point = bbox.centroid()
    coords = center_point.getInfo()['coordinates']
    lon, lat = coords
    easting, northing, zone_number, zone_letter = utm.from_latlon(lat, lon)
    return f'EPSG:326{zone_number}' if lat > 0 else f'EPSG:327{zone_number}'


if __name__ == '__main__':

    split = 'test'

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/spacenet7_s1s2')

    # extracting parameters from config
    records = cfg.SATELLITE_DATA.RECORDS

    ee.Initialize()

    # getting metadata from json file
    metadata = utils.read_json(SPACENET7_PATH / f'metadata_{split}.json')

    # getting orbit numbers if defined
    orbit_numbers = utils.read_json(SPACENET7_PATH / 's1_orbit_numbers.json') if cfg.SINGLE_S1_ORBIT else None

    aoi_tracker = []

    aoi_ids = list(metadata.keys())
    for aoi_id in aoi_ids:

        # missing
        if aoi_id in cfg.MISSING_AOIS:
            continue
        # subset
        if cfg.AOI_SUBSET and aoi_id not in cfg.AOI_SUBSET:
            continue

        # if aoi_id in aoi_tracker:
        #     continue
        timestamps = metadata[aoi_id]
        for timestamp in timestamps:
            year, month, mask = int(timestamp['year']), int(timestamp['month']), bool(timestamp['mask'])
            print(f'{aoi_id} - {year} - {month:02d}')

            # getting bounding box of area of interest, crs and orbit
            bbox = bounding_box(split, aoi_id, year, month)
            res = resolution(split, aoi_id, year, month)
            crs = epsg(split, aoi_id, year, month)
            orbit_number = orbit_numbers[aoi_id] if cfg.SINGLE_S1_ORBIT else None

            # download satellite data
            for record in records:
                sensor = record['SENSOR']
                processing_level = record['PROCESSING_LEVEL']
                product = record['PRODUCT']

                start_date = f'{year}-{month:02d}-01'
                end_year, end_month = utils.offset_months(year, month, 1)
                end_date = f'{end_year}-{end_month:02d}-01'
                date_range = ee.DateRange(start_date, end_date)

                # downloading satellite data according to properties specified in record
                img = satellite_data.get_satellite_data(record, bbox, date_range, orbit_number)
                if img is None:
                    continue
                img_name = f'{sensor}_{aoi_id}_{year}_{month:02d}'

                dl_desc = f'{aoi_id}{year}{month:02d}{sensor.capitalize()}Download'

                dl_task = ee.batch.Export.image.toDrive(
                    image=img,
                    region=bbox.getInfo()['coordinates'],
                    description=dl_desc,
                    folder=f'{split}_s1_dummy',
                    fileNamePrefix=img_name,
                    scale=res,
                    crs=crs,
                    maxPixels=1e7,
                    fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
                )

                dl_task.start()
                aoi_tracker.append(aoi_id)

