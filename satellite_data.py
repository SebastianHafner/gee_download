import ee
from data_processing import sentinel1, sentinel2toa


def get_satellite_data(properties: dict, roi: ee.Geometry, date_range) -> ee.Image:

    # getting satellite data for record setting
    processing_functions = {
        'sentinel1': {
            'single_orbit_mosaic': sentinel1.single_orbit_mean
        },
        'sentinel2toa': {
            'simple_cloud_free_mosaic': sentinel2toa.cloud_free_mosaic
        },
        'sentinel2sr': {
            'simple_cloud_free_mosaic': None
        }
    }

    sensor = properties['SENSOR']
    product = properties['PRODUCT']
    bands = properties['BANDS']
    normalize = properties['NORMALIZE']
    normalization_range = properties['NORMALIZATION_BOUNDS']

    func = processing_functions[sensor][product]
    img = func(roi, date_range)

    img = ee.Image(img).select(bands)
    img = img.unitScale(*normalization_range).clamp(0, 1) if normalize else img
    img = img.unmask().float()

    return img
