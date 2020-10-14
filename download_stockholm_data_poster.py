from download_manager import args
from download_manager.config import config
from data_processing import utils, building_footprints
import ee
import satellite_data


if __name__ == '__main__':

    ee.Initialize()

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    for record in cfg.SATELLITE_DATA.RECORDS:
        # unpacking
        sensor = record['SENSOR']
        processing_level = record['PROCESSING_LEVEL']
        product = record['PRODUCT']
        date_range = ee.DateRange(*record['DATE_RANGE'])

        # downloading satellite data according to properties specified in record
        bbox = utils.extract_bbox(cfg)
        img = satellite_data.get_satellite_data(record, bbox, date_range)
        img_name = f'{sensor}_{cfg.ROI.ID}'

        dl_desc = f'{sensor.capitalize()}{cfg.ROI.ID}Download'

        dl_task = ee.batch.Export.image.toDrive(
            image=img,
            region=bbox.getInfo()['coordinates'],
            description=dl_desc,
            folder=cfg.DOWNLOAD.DRIVE_FOLDER,
            fileNamePrefix=img_name,
            scale=cfg.PIXEL_SPACING,
            crs=cfg.ROI.UTM_EPSG,
            maxPixels=1e12,
            fileFormat='GeoTIFF'
        )
        dl_task.start()

    img = building_footprints.get_building_percentage(cfg)
    img_name = f'buildings_{cfg.ROI.ID}'

    dl_desc = f'Buildings{cfg.ROI.ID}Download'

    dl_task = ee.batch.Export.image.toDrive(
        image=img,
        region=bbox.getInfo()['coordinates'],
        description=dl_desc,
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix=img_name,
        scale=cfg.PIXEL_SPACING,
        crs=cfg.ROI.UTM_EPSG,
        maxPixels=1e12,
        fileFormat='GeoTIFF'
    )
    dl_task.start()
