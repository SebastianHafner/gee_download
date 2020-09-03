import ee

from download_manager import args
from download_manager.config import config

from data_processing import building_footprints, utils

if __name__ == '__main__':
    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    ee.Initialize()

    bbox = utils.extract_bbox(cfg)
    building_density = building_footprints.get_building_density(cfg)

    dl_desc = f'DriveSampleBuildingDensity{cfg.ROI.ID}'
    file_name = f'building_density_{cfg.ROI.ID}'

    dl_task = ee.batch.Export.image.toDrive(
        image=building_density,
        region=bbox.getInfo()['coordinates'],
        description=dl_desc,
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix=file_name,
        scale=cfg.PIXEL_SPACING,
        crs=cfg.ROI.UTM_EPSG,
        maxPixels=1e12,
        fileFormat='GeoTIFF'
    )

    dl_task.start()

