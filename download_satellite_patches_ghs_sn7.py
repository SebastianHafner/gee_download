from pathlib import Path
from download_manager import args
from download_manager.config import config
from data_processing import satellite_data, sentinel2toa, utils
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

tricky_rois = [
    'L15-0632E-0892N_2528_4620_13',
    'L15-1049E-1370N_4196_2710_13',
    'L15-1690E-1211N_6763_3346_13'
]


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
        if True:
            year = int(row['year'])
            month = int(row['month'])
            quality = int(row['quality'])

            # getting bounding box of area of interest
            bbox = bounding_box(aoi_id)
            epsg = epsg_utm(bbox)

            print(index, f'aoi_id: {aoi_id} - year: {year} - month: {month:02d} - quality: {quality}')

            # download satellite data
            start_date = f'{year}-{month:02d}-01'
            end_year, end_month = utils.offset_months(year, month, 1)
            end_date = f'{end_year}-{end_month:02d}-01'
            date_range = ee.DateRange(start_date, end_date)

            # downloading satellite data according to properties specified in record
            img = sentinel2toa.ghs_composite(bbox, date_range)
            img_name = f'sentinel2_{aoi_id}'

            dl_desc = f'{aoi_id}Sentinel2Download'

            dl_task = ee.batch.Export.image.toDrive(
                image=img,
                region=bbox.getInfo()['coordinates'],
                description=dl_desc,
                folder=f'spacenet7_ghs_sentinel2_v3',
                fileNamePrefix=f'sentinel2_{aoi_id}',
                scale=cfg.PIXEL_SPACING,
                crs=epsg,
                maxPixels=1e12,
                fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
            )

            dl_task.start()
