import ee

from download_manager import args
from download_manager.config import config

import satellite_data

import utils


if __name__ == '__main__':

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args)

    # extracting parameters from config
    records = cfg.SATELLITE_DATA.RECORDS
    dl_type = cfg.DOWNLOAD.TYPE

    # getting region of interest and date range of satellite data
    ee.Initialize()
    roi = utils.extract_bbox(cfg)
    date_range = utils.extract_date_range(cfg)

    # loading sampling points
    features = utils.load_samples(cfg)
    features = features[-1:]
    print(f'Number of patches: {len(features)}')

    for record in records:
        sensor = record['SENSOR']
        product = record['PRODUCT']
        for i, feature in enumerate(features):
            patch_id = i + 1
            print(f'{sensor} {product} Patch {patch_id}')

            # creating patch from point coordinates
            patch = utils.feature2patch(cfg, feature)

            # downloading satellite data according to properties specified in record
            img = satellite_data.get_satellite_data(record, patch, date_range)
            img_name = f'{sensor}_{cfg.ROI.ID}_patch{i + 1}'

            dl_desc = f'{dl_type.capitalize()}{sensor.capitalize()}DownloadPatch{patch_id}of{len(features)}'
            if cfg.DOWNLOAD.TYPE == 'cloud':

                dl_task = ee.batch.Export.image.toCloudStorage(
                    image=img,
                    region=patch.getInfo()['coordinates'],
                    description='PythonToCloudExport',
                    bucket=cfg.DOWNLOAD.BUCKET_NAME,
                    fileNamePrefix=f'{cfg.ROI.ID}/{sensor}/{img_name}',
                    scale=cfg.PIXEL_SPACING,
                    crs=cfg.ROI.UTM_EPSG,
                    maxPixels=1e6,
                    fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
                )

            else:  # drive

                dl_task = ee.batch.Export.image.toDrive(
                    image=img,
                    region=patch.getInfo()['coordinates'],
                    description='PythonToDriveExport',
                    folder=cfg.DOWNLOAD.DRIVE_FOLDER,
                    fileNamePrefix=img_name,
                    scale=cfg.PIXEL_SPACING,
                    crs=cfg.ROI.UTM_EPSG,
                    maxPixels=1e6,
                    fileFormat='GeoTIFF'
                )

            dl_task.start()
