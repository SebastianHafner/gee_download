import ee
from data_processing import sentinel1, sentinel2toa, sentinel2sr


def get_satellite_data(properties: dict, roi: ee.Geometry, date_range) -> ee.Image:

    # getting satellite data for record setting
    processing_functions = {
        'sentinel1': {'grd': {
            'single_orbit_mosaic': sentinel1.single_orbit_mean,
            'single_orbit_mosaic_temporal': sentinel1.single_orbit_metrics
        }},
        'sentinel2': {
            'toa': {
                'simple_cloud_free_mosaic': sentinel2toa.cloud_free_mosaic,
                'least_cloudy_scene': sentinel2toa.least_cloudy_scene
            },
            'sr': {
                'least_cloudy_scene': sentinel2sr.least_cloudy_scene,
                'least_cloudy_mosaic': sentinel2sr.least_cloudy_mosaic
            }
        }
    }

    sensor = properties['SENSOR']
    processing_level = properties['PROCESSING_LEVEL']
    product = properties['PRODUCT']
    bands = properties['BANDS']

    func = processing_functions[sensor][processing_level][product]
    img = func(roi, date_range)

    img = ee.Image(img).select(bands)
    img = img.unmask().float()

    return img
