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

    training = ['albuquerque', 'atlanta', 'charlston', 'columbus', 'dallas', 'denver', 'elpaso', 'houston',
                'kansascity', 'lasvegas', 'losangeles', 'miami', 'minneapolis', 'montreal', 'phoenix', 'quebec',
                'saltlakecity', 'sandiego', 'santafe', 'seattle', 'stgeorge', 'toronto', 'tucson', 'winnipeg']
    validation = ['calgary', 'newyork', 'sanfrancisco', 'vancouver']
    unlabeled = ['beijing', 'dakar', 'dubai', 'jakarta', 'kairo', 'kigali', 'lagos', 'mexicocity', 'mumbai',
                 'riodejanairo', 'shanghai', 'buenosaires', 'bogota', 'sanjose', 'santiagodechile', 'kapstadt',
                 'tripoli', 'freetown', 'london', 'madrid', 'kinshasa', 'manila', 'moscow', 'newdehli', 'nursultan',
                 'perth', 'tokio', 'stockholm', 'sidney', 'maputo', 'caracas', 'santacruzdelasierra', 'saopaulo',
                 'asuncion', 'lima', 'paramaribo', 'libreville', 'djibuti', 'beirut', 'baghdad', 'athens', 'islamabad',
                 'hanoi', 'bangkok', 'dhaka', 'bengaluru', 'taipeh', 'berlin', 'nanning', 'wuhan', 'daressalam',
                 'milano', 'zhengzhou', 'hefei', 'xian', 'seoul', 'ibadan', 'benincity', 'abidjan', 'accra',
                 'amsterdam', 'riyadh', 'amman', 'damascus', 'nouakchott', 'prague', 'sanaa', 'dahmar', 'kuwaitcity',
                 'tindouf', 'tehran']

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
            properties = {'id': roi['ID'], 'labeled': roi['LABELED'], 'dataset': dataset, 'crs': roi['UTM_EPSG']}


            geom = utils.extract_bbox(roi)
            feature = ee.Feature(geom, properties)
            features.append(feature)

    fc = ee.FeatureCollection(features)

    dl_task = ee.batch.Export.table.toDrive(
        collection=fc,
        description='sites',
        folder='urban_dataset_sites',
        fileNamePrefix='sites',
        fileFormat=cfg.DOWNLOAD.TABLE_FORMAT
    )

    dl_task.start()

    fc_points = fc.map(lambda f: ee.Feature(f).centroid())

    dl_task = ee.batch.Export.table.toDrive(
        collection=fc_points,
        description='sites_points',
        folder='urban_dataset_sites',
        fileNamePrefix='sites_points',
        fileFormat=cfg.DOWNLOAD.TABLE_FORMAT
    )

    dl_task.start()
