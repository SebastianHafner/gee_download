import ee

from download_manager import args
from download_manager.config import config

from data_processing import utils


if __name__ == '__main__':
    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    ee.Initialize()

    bbox = utils.extract_bbox(cfg)
    bbox = ee.FeatureCollection([ee.Feature(bbox)])

    dl_desc = f'DriveBBox{cfg.ROI.ID}'
    file_name = f'bbox_{cfg.ROI.ID}'

    dl_task = ee.batch.Export.table.toDrive(
        collection=bbox,
        description=dl_desc,
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix=file_name,
        fileFormat=cfg.DOWNLOAD.TABLE_FORMAT
    )

    dl_task.start()


