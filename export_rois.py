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

    training = ['atlanta', 'dallas', 'denver', 'lasvegas', 'losangeles', 'seattle']
    validation = ['newyork', 'sanfrancisco']
    unlabeled = ['stockholm', 'daressalam', 'kampala', 'sidney']

    all_sites = training + validation + unlabeled

    features = []
    point_features = []
    for roi in cfg.ROIS:
        site = roi['ID']
        if site in all_sites:
            if site in training:
                dataset = 'training'
            elif site in validation:
                dataset = 'validation'
            else:
                dataset = 'unlabeled'
            properties = {'id': roi['ID'], 'labeled': roi['LABELED'], 'dataset': dataset}


            geom = utils.extract_bbox(roi)
            feature = ee.Feature(geom, properties)
            features.append(feature)

    fc = ee.FeatureCollection(features)

    dl_task = ee.batch.Export.table.toDrive(
        collection=fc,
        description='sites',
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix='sites',
        fileFormat=cfg.DOWNLOAD.TABLE_FORMAT
    )

    dl_task.start()

    fc_points = fc.map(lambda f: ee.Feature(f).centroid())

    dl_task = ee.batch.Export.table.toDrive(
        collection=fc_points,
        description='sites_points',
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix='sites_points',
        fileFormat=cfg.DOWNLOAD.TABLE_FORMAT
    )

    dl_task.start()
