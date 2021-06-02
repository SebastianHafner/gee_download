import ee

from download_manager import args
from download_manager.config import config

from data_processing import utils


def add_cloud_score(img: ee.Image) -> ee.Image:
    img = ee.Image(img)
    geom = ee.Geometry(img.get('geom'))
    stats = img.select('probability').reduceRegion(reducer=ee.Reducer.mean(),
                                                   geometry=geom,
                                                   scale=10,
                                                   maxPixels=1e12)
    cloud_score = stats.get('probability')
    img = img.set('cloudScore', cloud_score)

    return img


def mostly_cloud_free_mosaic(geom: ee.Geometry, date_range) -> ee.Image:
    s2 = ee.ImageCollection('COPERNICUS/S2') \
        .filterDate(date_range.start(), date_range.end()) \
        .filterBounds(geom) \
        .map(lambda img: ee.Image(img).set('geom', geom))
    s2_clouds = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY') \
        .filterDate(date_range.start(), date_range.end()) \
        .filterBounds(geom)
    join_condition = ee.Filter.equals(leftField='system:index', rightField='system:index')
    s2 = ee.Join.saveFirst('cloudProbability').apply(primary=s2,
                                                     secondary=s2_clouds,
                                                     condition=join_condition)
    s2 = s2.map(lambda img: ee.Image(img).addBands(ee.Image(img.get('cloudProbability'))))

    s2 = s2.map(add_cloud_score)
    s2 = ee.ImageCollection(s2)

    # print(s2.first().getInfo())

    img = s2.sort('cloudScore', False).mosaic()
    img = img.unitScale(0, 10_000).clamp(0, 1)
    return img


def process(cfg, roi_id, start_date: str, end_date: str):
    roi = [roi for roi in cfg.ROIS if roi['ID'] == roi_id][0]
    epsg = roi['UTM_EPSG']
    bbox = utils.extract_bbox(roi)

    start_date = ee.Date(start_date)
    end_date = ee.Date(end_date)
    end_date_str = end_date.format('YYYY-MM-dd').getInfo()

    from_date = start_date

    while True:
        to_date = from_date.advance(1, 'month')
        from_date_str = from_date.format('YYYY-MM-dd').getInfo()
        to_date_str = to_date.format('YYYY-MM-dd').getInfo()
        print(f'From {from_date_str} to {to_date_str}')

        img = mostly_cloud_free_mosaic(bbox, ee.DateRange(from_date, end_date))
        img = img.set('geom', bbox)

        img = add_cloud_score(img)
        print(img.bandNames().getInfo())
        # print(img.get('cloudScore').getInfo())

        from_date = to_date
        if from_date_str == end_date_str:
            break



if __name__ == '__main__':

    start_date = '2019-05-01'
    end_date = '2019-08-01'

    ee.Initialize()

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    process(cfg, 'stockholm', start_date, end_date)
