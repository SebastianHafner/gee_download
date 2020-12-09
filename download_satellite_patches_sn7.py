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
    cfg = config.setup(args, 'configs/urban_extraction')

    # extracting parameters from config
    records = cfg.SATELLITE_DATA.RECORDS

    ee.Initialize()

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

        if quality != 3:

            print(index, f'aoi_id: {aoi_id} - year: {year} - month: {month:02d} - quality: {quality}')

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
                img_name = f'{sensor}_{aoi_id}'

                dl_desc = f'{aoi_id}{sensor.capitalize()}Download'

                dl_task = ee.batch.Export.image.toCloudStorage(
                    image=img,
                    region=bbox.getInfo()['coordinates'],
                    description=dl_desc,
                    bucket=cfg.DOWNLOAD.BUCKET_NAME,
                    fileNamePrefix=f'sn7/{sensor}/{img_name}',
                    scale=cfg.PIXEL_SPACING,
                    crs=epsg,
                    maxPixels=1e6,
                    fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
                )

                # dl_task.start()

            building_footprints = ee.FeatureCollection(f'users/{cfg.GEE_USERNAME}/SN7/sn7_buildings')
            building_footprints = building_footprints.filterBounds(bbox)
            buildings = bf.rasterize(building_footprints, 'buildings')
            building_percentage = buildings \
                .reproject(crs=epsg, scale=1) \
                .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1000) \
                .reproject(crs=epsg, scale=cfg.PIXEL_SPACING) \
                .rename('buildingPercentage')

            img_name = f'buildings_{aoi_id}'

            dl_desc = f'{aoi_id}BuildingsDownload'

            dl_task = ee.batch.Export.image.toCloudStorage(
                image=building_percentage,
                region=bbox.getInfo()['coordinates'],
                description=dl_desc,
                bucket=cfg.DOWNLOAD.BUCKET_NAME,
                fileNamePrefix=f'sn7/buildings/{img_name}',
                scale=cfg.PIXEL_SPACING,
                crs=epsg,
                maxPixels=1e6,
                fileFormat='GeoTIFF'
            )
            # dl_task.start()

            # building_footprints = building_footprints.filterMetadata('stable', 'equals', 1)
            building_footprints = ee.FeatureCollection(f'users/{cfg.GEE_USERNAME}/SN7/sn7_reference_buildings')
            building_footprints = building_footprints.filterBounds(bbox)
            buildings = bf.rasterize(building_footprints, 'buildings')
            building_percentage = buildings \
                .reproject(crs=epsg, scale=1) \
                .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1000) \
                .reproject(crs=epsg, scale=cfg.PIXEL_SPACING) \
                .rename('buildingPercentage')

            img_name = f'buildings_{aoi_id}'

            dl_desc = f'{aoi_id}BuildingsDownload'

            dl_task = ee.batch.Export.image.toCloudStorage(
                image=building_percentage,
                region=bbox.getInfo()['coordinates'],
                description=dl_desc,
                bucket=cfg.DOWNLOAD.BUCKET_NAME,
                fileNamePrefix=f'sn7/reference_buildings/{img_name}',
                scale=cfg.PIXEL_SPACING,
                crs=epsg,
                maxPixels=1e6,
                fileFormat='GeoTIFF'
            )
            # dl_task.start()

            dsm = ee.Image("JAXA/ALOS/AW3D30/V2_2").select(['AVE_DSM'], ['Elevation']).float()
            dsm = dsm.unitScale(-1000, 9000).clamp(0, 1).unmask().float()

            img_name = f'dsm_{aoi_id}'

            dl_desc = f'{aoi_id}DSMDownload'

            dl_task = ee.batch.Export.image.toCloudStorage(
                image=dsm,
                region=bbox.getInfo()['coordinates'],
                description=dl_desc,
                bucket=cfg.DOWNLOAD.BUCKET_NAME,
                fileNamePrefix=f'sn7/dsm/{img_name}',
                scale=cfg.PIXEL_SPACING,
                crs=epsg,
                maxPixels=1e6,
                fileFormat='GeoTIFF'
            )
            # dl_task.start()