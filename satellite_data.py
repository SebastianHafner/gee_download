import ee
import utils
import s2toa


def s2_bands(key: str):

    if key == 'rgb':
        return ['B2', 'B3', 'B4']
    elif key == 'tm':
        return ['B2', 'B3', 'B4', 'B8']
    elif key == 'nre':
        return ['B2', 'B3', 'B4', 'B8', 'B11', 'B12']
    elif key == 'all':
        return ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12']


def load_modifiers(record: dict):
    return []


def load_filters(record: dict):
    return []


def process_record(record: dict, roi: ee.Geometry, date_range) -> ee.Image:
    # getting satellite data for record setting
    # TODO: put functions in dictionary
    img = ee.Image(1)
    if record['SENSOR'] == 's2sr':
        pass

    if record['SENSOR'] == 's2toa':
        if record['PRODUCT'] == 'cloud_free_mosaic':
            img = s2toa.cloud_free_mosaic(roi, date_range)
            img = img.select(s2_bands(record['BANDS']))
            if record['NORMALIZE'] is True:
                img = img.divide(10000).clamp(0, 1).float()

    if record['SENSOR'] == 's1':
        # TODO: implement s1
        pass

    return img


def get_satellite_data(cfg) -> ee.Image:

    # bounding box
    bbox = utils.extract_bbox(cfg)

    # time series start and end date
    date_range = utils.extract_date_range(cfg)

    satellite_data = []
    for record in cfg.SATELLITE_DATA.RECORDS:
        img = process_record(record, bbox, date_range)
        satellite_data.append(img)

    return ee.Image.cat(satellite_data)

