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
SPACENET7_PATH = Path('/storage/shafner/spacenet7/train')


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
    cfg = config.setup(args, 'configs/spacenet7_s1s2')

    # extracting parameters from config
    records = cfg.SATELLITE_DATA.RECORDS

    ee.Initialize()

    # getting metadata from csv file
    metadata_file = Path(cfg.METADATA_FILE)
    metadata = pd.read_csv(metadata_file)

    # getting orbit numbers if defined
    if cfg.SINGLE_S1_ORBIT:
        orbits_file = Path(cfg.S1_ORBITS_FILE)
        orbit_numbers = utils.read_json(orbits_file)
    else:
        orbit_numbers = None

    aoi_tracker = []

    for index, row in metadata.iterrows():

        aoi_id = str(row['aoi_id'])

        # missing
        if aoi_id in cfg.MISSING_AOIS:
            continue
        # subset
        if cfg.AOI_SUBSET and aoi_id not in cfg.AOI_SUBSET:
            continue

        year = int(row['year'])
        month = int(row['month'])
        mask = int(row['mask'])
        print(f'{aoi_id} - {year} - {month:02d}')

        # getting bounding box of area of interest, crs and orbit
        bbox = bounding_box(aoi_id)
        epsg = epsg_utm(bbox)
        orbit_number = orbit_numbers[aoi_id] if cfg.SINGLE_S1_ORBIT else None

        # download satellite data
        for record in records:
            sensor = record['SENSOR']
            processing_level = record['PROCESSING_LEVEL']
            product = record['PRODUCT']

            if sensor == 'sentinel1':
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

                dl_task = ee.batch.Export.image.toCloudStorage(
                    image=img,
                    region=bbox.getInfo()['coordinates'],
                    description=dl_desc,
                    bucket=cfg.DOWNLOAD.BUCKET_NAME,
                    fileNamePrefix=f'{aoi_id}/{sensor}/{img_name}',
                    scale=cfg.PIXEL_SPACING,
                    crs=epsg,
                    maxPixels=1e6,
                    fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
                )

                dl_task.start()

        # building_footprints = ee.FeatureCollection(f'users/{cfg.GEE_USERNAME}/spacenet7/buildings_{aoi_id}')
        # building_footprints = building_footprints \
        #     .filterMetadata('year', 'equals', year) \
        #     .filterMetadata('month', 'equals', month)
        # print(f'n buildings: {building_footprints.size().getInfo()}')
        # buildings = bf.rasterize(building_footprints, 'buildings')
        # building_percentage = buildings \
        #     .reproject(crs=epsg, scale=1) \
        #     .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1000) \
        #     .reproject(crs=epsg, scale=cfg.PIXEL_SPACING) \
        #     .rename('buildingPercentage')
        #
        # img_name = f'buildings_{aoi_id}_{year}_{month:02d}'
        #
        # dl_desc = f'{aoi_id}BuildingsDownload'
        #
        # dl_task = ee.batch.Export.image.toCloudStorage(
        #     image=building_percentage,
        #     region=bbox.getInfo()['coordinates'],
        #     description=dl_desc,
        #     bucket=cfg.DOWNLOAD.BUCKET_NAME,
        #     fileNamePrefix=f'{aoi_id}/buildings/{img_name}',
        #     scale=cfg.PIXEL_SPACING,
        #     crs=epsg,
        #     maxPixels=1e6,
        #     fileFormat='GeoTIFF'
        # )
        # dl_task.start()

        # if aoi_id not in aoi_tracker and mask:
        #     masks = ee.Image(f'users/{cfg.GEE_USERNAME}/spacenet7/masks_{aoi_id}').unmask().uint8()
        #     img_name = f'masks_{aoi_id}'
        #     dl_desc = f'MasksDownload{aoi_id}'
        #     dl_task = ee.batch.Export.image.toCloudStorage(
        #         image=masks,
        #         region=bbox.getInfo()['coordinates'],
        #         description=dl_desc,
        #         bucket=cfg.DOWNLOAD.BUCKET_NAME,
        #         fileNamePrefix=f'{aoi_id}/{img_name}',
        #         scale=cfg.PIXEL_SPACING,
        #         crs=epsg,
        #         maxPixels=1e6,
        #         fileFormat='GeoTIFF'
        #     )
        #     dl_task.start()
        #     aoi_tracker.append(aoi_id)
