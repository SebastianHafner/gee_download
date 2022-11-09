import ee

from download_manager import args
from download_manager.config import config

from data_processing import utils, sentinel1, sentinel2toa

from tqdm import tqdm
from pathlib import Path

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

    sites = ['albuquerque', 'atlantaeast', 'atlantawest', 'charlston', 'columbus', 'dallas', 'denver', 'elpaso',
             'houston', 'kansascity', 'lasvegas', 'losangeles', 'miami', 'minneapolis', 'montreal', 'phoenix', 'quebec',
             'saltlakecity', 'sandiego', 'santafe', 'seattle', 'stgeorge', 'toronto','tucson', 'winnipeg', 'sidney',
             'perth', 'calgary', 'newyork', 'sanfrancisco', 'vancouver', 'beijing', 'dubai', 'jakarta', 'kairo',
             'kigali', 'lagos', 'mexicocity', 'mumbai', 'riodejanairo', 'shanghai', 'buenosaires', 'bogota', 'sanjose',
             'santiagodechile', 'kapstadt', 'tripoli', 'freetown', 'london', 'madrid', 'kinshasa', 'manila', 'moscow',
             'newdehli', 'nursultan', 'tokio', 'stockholm', 'maputo', 'caracas', 'santacruzdelasierra', 'saopaulo',
             'asuncion', 'lima', 'paramaribo', 'libreville', 'djibuti', 'beirut', 'baghdad', 'athens', 'islamabad',
             'hanoi', 'bangkok', 'dhaka', 'bengaluru', 'taipeh', 'berlin', 'nanning', 'wuhan', 'daressalam', 'milano',
             'zhengzhou', 'hefei', 'xian', 'seoul', 'ibadan', 'benincity', 'abidjan', 'accra', 'amsterdam', 'riyadh',
             'amman', 'damascus', 'nouakchott', 'prague', 'sanaa', 'kuwaitcity', 'tehran']

    data = {}
    total_s1, total_s2 = 0, 0

    for roi in cfg.ROIS:
        roi_id = roi['ID']
        if roi_id in sites:
            print(roi_id)
            data[roi_id] = {}
            bbox = utils.extract_bbox(roi)
            for record in records:
                # unpacking
                sensor = record['SENSOR']
                date_range = ee.DateRange(*record['DATE_RANGE'])
                if sensor == 'sentinel1':
                    n = sentinel1.single_orbit_mean_scene_number(bbox, date_range)
                    total_s1 += n
                else:
                    n = sentinel2toa.custom_composite_scene_number(bbox, date_range)
                    total_s2 += n
                data[roi_id][sensor] = n
                print(f'{sensor}: {n}')
    data['total_sentinel1'] = total_s1
    data['total_sentinel2'] = total_s2

    out_file = Path('C:/Users/shafner/urban_extraction/revision/data') / 'training_validation_scene_numbers.json'
    utils.write_json(out_file, data)