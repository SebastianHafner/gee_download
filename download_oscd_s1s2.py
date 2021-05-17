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

OSCD_DATASET_PATH = Path('C:/Users/shafner/continuous_urban_change_detection/OSCD_dataset')
EPSG = 'epsg:4326'

if __name__ == '__main__':

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/oscd_s1s2')

    # extracting parameters from config
    records = cfg.SATELLITE_DATA.RECORDS

    ee.Initialize()

    # getting metadata from GEE
    metadata = ee.FeatureCollection('users/hafnersailing/oscd_dataset/oscd_dataset_metadata')
    aoi_ids = metadata.aggregate_array('name').getInfo()

    for i, aoi_id in enumerate(aoi_ids):

        print(aoi_id)
        # missing
        if aoi_id in cfg.MISSING_AOIS:
            continue
        # subset
        if cfg.AOI_SUBSET and aoi_id not in cfg.AOI_SUBSET:
            continue

        feature = metadata.filterMetadata('name', 'equals', aoi_id).first()

        bbox = feature.geometry()

        start_date = ee.Date(feature.get('t1'))
        start_year = start_date.get('year').getInfo()
        start_month = start_date.get('month').getInfo()
        print(f'Start date: {start_year}-{start_month}-01')

        end_date = ee.Date(feature.get('t2'))
        end_year = end_date.get('year').getInfo()
        end_month = end_date.get('month').getInfo()
        print(f'End date: {end_year}-{end_month}-01')

        from_date = ee.Date(f'{start_year}-{start_month:02d}-01')

        while True:

            year = from_date.get('year').getInfo()
            month = from_date.get('month').getInfo()
            print(f'Date: {year}-{month:02d}')
            to_date = from_date.advance(1, 'month')

            for record in records:
                sensor = record['SENSOR']
                processing_level = record['PROCESSING_LEVEL']
                product = record['PRODUCT']

                date_range = ee.DateRange(from_date, to_date)

                # downloading satellite data according to properties specified in record
                img = satellite_data.get_satellite_data(record, bbox, date_range)
                if img is None:
                    continue
                img_name = f'{sensor}_{aoi_id}_{year}_{month:02d}'

                dl_desc = f'{aoi_id}{year}{month:02d}{sensor.capitalize()}Download'

                dl_task = ee.batch.Export.image.toCloudStorage(
                    image=img,
                    region=bbox.getInfo()['coordinates'],
                    description=dl_desc,
                    bucket=cfg.DOWNLOAD.BUCKET_NAME,
                    fileNamePrefix=f'{aoi_id}/{sensor}/{img_name}',
                    scale=cfg.PIXEL_SPACING,
                    crs=EPSG,
                    maxPixels=1e7,
                    fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
                )

                dl_task.start()

            from_date = to_date

            if from_date.get('year').getInfo() == end_year and from_date.get('month').getInfo() == end_month:
                break

        change = ee.Image(f'users/{cfg.GEE_USERNAME}/oscd_dataset/change_{aoi_id}').unmask().uint8()
        img_name = f'change_{aoi_id}'
        dl_desc = f'Change{aoi_id}Download'
        dl_task = ee.batch.Export.image.toCloudStorage(
            image=change,
            region=bbox.getInfo()['coordinates'],
            description=dl_desc,
            bucket=cfg.DOWNLOAD.BUCKET_NAME,
            fileNamePrefix=f'{aoi_id}/change/{img_name}',
            scale=cfg.PIXEL_SPACING,
            crs=EPSG,
            maxPixels=1e7,
            fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
        )
        dl_task.start()
