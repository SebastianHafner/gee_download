import ee
from gee.spectral_indices import get_spectral_index_func
from gee.utils import compute_time_series_metrics

__SPECTRAL_BANDS__ = ['Blue', 'Green', 'Red', 'RedEdge1', 'RedEdge2', 'RedEdge3', 'NIR', 'RedEdge4', 'SWIR1', 'SWIR2']


# getting list of feature names based on input parameters
def get_feature_names(bands: list, indices: list, metrics: list):
    band_names = [f'{band}_{metric}' for band in bands for metric in metrics]
    index_names = [f'{index}_{metric}' for index in indices for metric in metrics]
    return band_names + index_names


# getting sentinel-2 band name
def get_band_name(band: str) -> str:
    band_names = ee.Dictionary({'Blue': 'B2', 'Green': 'B3', 'Red': 'B4', 'RedEdge1': 'B5', 'RedEdge2': 'B6',
                                'RedEdge3': 'B7','NIR': 'B8', 'RedEdge4': 'B8A', 'SWIR1': 'B11', 'SWIR2': 'B12',
                                'QA': 'QA60'})
    return band_names.get(band)


# function to mask clouds in a sentinel-2 image
def cloud_mask(img: ee.Image) -> ee.Image:
    img = ee.Image(img)
    qa_band = img.select('QA')
    # Bits 10 and 11 are clouds and cirrus, respectively
    bit_mask_cloud = 1 << 10
    bit_mask_cirrus = 1 << 11
    no_clouds = qa_band.bitwiseAnd(bit_mask_cloud).eq(0).And(qa_band.bitwiseAnd(bit_mask_cirrus).eq(0))
    return img.updateMask(no_clouds)


# function to mask snow in a sentinel-2 image
def snow_mask(img: ee.Image) -> ee.Image:
    img = ee.Image(img)
    # TODO: assert that it is a surface reflectance image (otherwise no SCL band)
    no_snow = img.select('SCL').eq(11).Not()
    return img.updateMask(no_snow)


# retrieve sentinel-2 data for region of interest (bbox)
def get_time_series_features(bbox: ee.Geometry, from_date: str, to_date: str, bands: list = None,
                             indices: list = [], metrics: list = ['median'], include_count: bool = False):

    # getting all sentinel-2 (TOA) imagery
    s2 = ee.ImageCollection('COPERNICUS/S2')

    # creating selection of band names
    if bands is None:
        bands = __SPECTRAL_BANDS__

    all_bands = ee.List([*__SPECTRAL_BANDS__, 'QA'])

    # selecting specified band names
    s2 = s2.select(all_bands.map(lambda band: get_band_name(band)), all_bands)

    # sub-setting imagery intersecting bounding box of city
    s2 = s2.filterBounds(bbox)

    # sub-setting imagery to input year
    s2 = s2.filterDate(from_date, to_date)

    # addressing clouds
    max_cloud_percentage = 60
    s2 = s2.filterMetadata('CLOUDY_PIXEL_PERCENTAGE', 'not_greater_than', max_cloud_percentage)
    print(f'Number of Sentinel-2 scenes (cloud percentage <= 60 %): {s2.size().getInfo()}')
    s2 = s2.map(cloud_mask)

    # linearly rescale all images from [0, 10'000] to [0, 1]
    s2 = s2.map(lambda img: ee.Image(img).divide(10000).clamp(0, 1))

    # adding spectral indices
    for index in indices:
        index_func = get_spectral_index_func(index)
        s2 = s2.map(index_func)

    # sub-setting to selected spectral bands and indices
    s2 = s2.select([*bands, *indices])

    # compute statistical metrics of image time series
    features = compute_time_series_metrics(s2, bands, metrics)

    if include_count:
        features = features.addBands(s2.reduce(ee.Reducer.count()).rename('count'))

    return features.unmask().float()
