import ee

from download_manager import args
from download_manager.config import config

import satellite_data
from data_processing import utils

from tqdm import tqdm

if __name__ == '__main__':

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    # extracting parameters from config
    records = cfg.SATELLITE_DATA.RECORDS
    dl_type = cfg.DOWNLOAD.TYPE

    # getting region of interest and date range of satellite data
    ee.Initialize()
    roi = utils.extract_bbox(cfg)

    for record in records:
        # unpacking
        sensor = record['SENSOR']
        processing_level = record['PROCESSING_LEVEL']
        product = record['PRODUCT']
        date_range = ee.DateRange(*record['DATE_RANGE'])

        print(f'{sensor} {processing_level} {product}')

        # downloading satellite data according to properties specified in record
        img = satellite_data.get_satellite_data(record, roi, date_range)
        img_name = f'{sensor}_{cfg.ROI.ID}'

        dl_desc = f'{cfg.ROI.ID}{dl_type.capitalize()}{sensor.capitalize()}'
        if cfg.DOWNLOAD.TYPE == 'cloud':

            dl_task = ee.batch.Export.image.toCloudStorage(
                image=img,
                region=roi.getInfo()['coordinates'],
                description=dl_desc,
                bucket=cfg.DOWNLOAD.BUCKET_NAME,
                fileNamePrefix=f'{cfg.ROI.ID}/{sensor}/{img_name}_',
                scale=cfg.PIXEL_SPACING,
                crs=cfg.ROI.UTM_EPSG,
                fileDimensions=cfg.SAMPLING.PATCH_SIZE,
                maxPixels=1e12,
                fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
            )

        else:  # drive
            dl_task = ee.batch.Export.image.toDrive(
                image=img,
                region=roi.getInfo()['coordinates'],
                description=dl_desc,
                folder=cfg.DOWNLOAD.DRIVE_FOLDER,
                fileNamePrefix=img_name,
                scale=cfg.PIXEL_SPACING,
                crs=cfg.ROI.UTM_EPSG,
                fileDimensions=cfg.SAMPLING.PATCH_SIZE,
                maxPixels=1e12,
                fileFormat='GeoTIFF'
            )

        dl_task.start()
