from pathlib import Path
from download_manager import args
from download_manager.config import config
from data_processing import utils, sentinel1, sentinel2toa
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

    # extracting parameters from config
    records = cfg.SATELLITE_DATA.RECORDS

    ee.Initialize()

    # getting metadata from csv file
    metadata_file = SPACENET7_PATH.parent / 'sn7_metadata_urban_dataset.csv'
    metadata = pd.read_csv(metadata_file)

    data = {}
    total_s1, total_s2 = 0, 0
    for index, row in metadata.iterrows():
        aoi_id = str(row['aoi_id'])
        year = int(row['year'])
        month = int(row['month'])
        quality = int(row['quality'])

        print(aoi_id)
        data[aoi_id] = {}

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

            # unpacking
            sensor = record['SENSOR']
            date_range = ee.DateRange(*record['DATE_RANGE'])
            if sensor == 'sentinel1':
                n = sentinel1.single_orbit_mean_scene_number(bbox, date_range)
                total_s1 += n
            else:
                n = sentinel2toa.custom_composite_scene_number(bbox, date_range)
                total_s2 += n
            data[aoi_id][sensor] = n
            print(f'{sensor}: {n}')

    data['total_sentinel1'] = total_s1
    data['total_sentinel2'] = total_s2

    out_file = Path('C:/Users/shafner/urban_extraction/revision/data') / 'test_scene_numbers.json'
    utils.write_json(out_file, data)