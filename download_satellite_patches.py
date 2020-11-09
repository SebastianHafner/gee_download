import ee

from download_manager import args
from download_manager.config import config

from data_processing import satellite_data, utils, building_footprints

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

    for roi in tqdm(cfg.ROIS):
        roi_id = roi['ID']
        epsg = roi['UTM_EPSG']
        if roi_id in cfg.ROIS_SUBSET:
            bbox = utils.extract_bbox(roi)

            for record in records:
                # unpacking
                sensor = record['SENSOR']
                processing_level = record['PROCESSING_LEVEL']
                product = record['PRODUCT']
                date_range = ee.DateRange(*record['DATE_RANGE'])

                print(f'{sensor} {processing_level} {product}')

                img = satellite_data.get_satellite_data(record, bbox, date_range)
                img = img.reproject(crs=epsg, scale=cfg.PIXEL_SPACING)
                # mask = building_footprints.get_building_mask(cfg, roi)
                # mask = mask.reproject(crs=epsg, scale=cfg.PIXEL_SPACING)
                # img = img.updateMask(mask)
                img_name = f'{sensor}_{roi_id}_'

                dl_desc = f'{cfg.ROI.ID}{sensor.capitalize()}{dl_type.capitalize()}'
                dl_task = ee.batch.Export.image.toCloudStorage(
                    image=img,
                    region=bbox.getInfo()['coordinates'],
                    description=dl_desc,
                    bucket=cfg.DOWNLOAD.BUCKET_NAME,
                    fileNamePrefix=f'{roi_id}/{sensor}/{img_name}',
                    scale=cfg.PIXEL_SPACING,
                    crs=epsg,
                    fileDimensions=cfg.SAMPLING.PATCH_SIZE,
                    maxPixels=1e12,
                    skipEmptyTiles=True,
                    fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
                )

                dl_task.start()
