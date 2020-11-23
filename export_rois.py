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

    features = []
    for roi in cfg.ROIS:
        geom = utils.extract_bbox(roi)
        feature = ee.Feature(geom, {'id': roi['ID'], 'labeled': roi['LABELED']})
        features.append(feature)

    fc = ee.FeatureCollection(features)

    dl_task = ee.batch.Export.table.toDrive(
        collection=fc,
        description='rois',
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix='rois',
        fileFormat=cfg.DOWNLOAD.TABLE_FORMAT
    )