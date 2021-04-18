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


def epsg_utm(bbox):
    center_point = bbox.centroid()
    coords = center_point.getInfo()['coordinates']
    lon, lat = coords
    easting, northing, zone_number, zone_letter = utm.from_latlon(lat, lon)
    return f'EPSG:326{zone_number}' if lat > 0 else f'EPSG:327{zone_number}'


def building_footprint_features(aoi_id: str, year: int, month: int):
    root_path = SPACENET7_PATH / aoi_id
    label_folder = root_path / 'labels_match'
    label_file = label_folder / f'global_monthly_{year}_{month:02d}_mosaic_{aoi_id}_Buildings.geojson'
    label_data = utils.load_json(label_file)

    features = label_data['features']
    new_features = []
    for feature in features:
        coords = feature['geometry']['coordinates']
        geom = ee.Geometry.Polygon(coords, proj='EPSG:3857').transform('EPSG:4326')
        new_feature = ee.Feature(geom)
        new_features.append(new_feature)
    return new_features


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

    for index, row in metadata.iterrows():

        aoi_id = str(row['aoi_id'])
        if aoi_id == 'L15-1848E-0793N_7394_5018_13' or aoi_id == 'L15-0457E-1135N_1831_3648_13':
            continue
        year = int(row['year'])
        month = int(row['month'])
        mask = int(row['mask'])
        print(f'{aoi_id} - {year} - {month:02d}')

        # getting bounding box of area of interest
        bbox = bounding_box(aoi_id)
        epsg = epsg_utm(bbox)

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
                crs=epsg,
                maxPixels=1e6,
                fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
            )

            dl_task.start()

        building_footprints = ee.FeatureCollection(f'users/{cfg.GEE_USERNAME}/spacenet7/buildings_{aoi_id}')
        building_footprints = building_footprints \
            .filterMetadata('year', 'equals', year) \
            .filterMetadata('month', 'equals', month)
        print(f'n buildings: {building_footprints.size().getInfo()}')
        buildings = bf.rasterize(building_footprints, 'buildings')
        building_percentage = buildings \
            .reproject(crs=epsg, scale=1) \
            .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1000) \
            .reproject(crs=epsg, scale=cfg.PIXEL_SPACING) \
            .rename('buildingPercentage')

        img_name = f'buildings_{aoi_id}_{year}_{month:02d}'

        dl_desc = f'{aoi_id}BuildingsDownload'

        dl_task = ee.batch.Export.image.toCloudStorage(
            image=building_percentage,
            region=bbox.getInfo()['coordinates'],
            description=dl_desc,
            bucket=cfg.DOWNLOAD.BUCKET_NAME,
            fileNamePrefix=f'{aoi_id}/buildings/{img_name}',
            scale=cfg.PIXEL_SPACING,
            crs=epsg,
            maxPixels=1e6,
            fileFormat='GeoTIFF'
        )
        dl_task.start()
