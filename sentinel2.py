import ee
import utils
import math


# getting sentinel-2 band name
def s2_band_name(band: str) -> str:
    band_names = ee.Dictionary({'Blue': 'B2', 'Green': 'B3', 'Red': 'B4', 'RedEdge1': 'B5', 'RedEdge2': 'B6',
                                'RedEdge3': 'B7','NIR': 'B8', 'RedEdge4': 'B8A', 'SWIR1': 'B11', 'SWIR2': 'B12',
                                'QA': 'QA60', 'SCL': 'SCL'})
    return band_names.get(band)


def s2_bands(identifier: str):
    if identifier == 'all':
        return ['Blue', 'Green', 'Red', 'RedEdge1', 'RedEdge2', 'RedEdge3', 'NIR', 'RedEdge4', 'SWIR1', 'SWIR2']
    elif identifier == 'nre':
        return ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2']
    elif identifier == 'tm':
        return ['Blue', 'Green', 'Red', 'NIR']
    elif identifier == 'rgb':
        return ['Blue', 'Green', 'Red']
    else:
        return []


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
    no_snow = img.select('SCL').eq(11).Not()
    return img.updateMask(no_snow)


def s2sr_collection(bbox: ee.Geometry, date_range: tuple, include_bands: str,
                    mask_clouds: bool = True, mask_snow: bool = True, normalize: bool = True):

    # getting all sentinel-2 surface reflectance scenes
    collection = ee.ImageCollection('COPERNICUS/S2_SR') \
        .filterBounds(bbox) \
        .filterDate(*date_range)

    # selecting bands
    bands = s2_bands(include_bands)
    bands_plus_quality = ee.List([*bands, 'QA', 'SCL'])
    collection = collection.select(bands_plus_quality.map(lambda band: s2_band_name(band)), bands_plus_quality)

    if mask_clouds:
        collection = collection.map(cloud_mask)
    if mask_snow:
        collection = collection.map(snow_mask)
    if normalize:
        collection = collection.map(utils.normalize(0, 10000))

    collection = collection.select(bands)

    return collection


def s2toa_collection(bbox: ee.Geometry, date_range: tuple, include_bands: str,
                     mask_clouds: bool = True, normalize: bool = True):

    # getting all sentinel-2 top of atmosphere scenes
    collection = ee.ImageCollection('COPERNICUS/S2') \
        .filterBounds(bbox) \
        .filterDate(*date_range)

    # selecting bands
    bands = s2_bands(include_bands)
    bands_plus_quality = ee.List([*bands, 'QA'])
    collection = collection.select(bands_plus_quality.map(lambda band: s2_band_name(band)), bands_plus_quality)

    if mask_clouds:
        collection = collection.map(cloud_mask)
    if normalize:
        collection = collection.map(utils.normalize(0, 10000))

    collection = collection.select(bands)

    return collection


def simple_cloud_mosaicking(collection: ee.ImageCollection) -> ee.Image:

    print(collection.size().getInfo())
    quality_property = 'CLOUDY_PIXEL_PERCENTAGE'
    sorted_collection = collection.sort(quality_property, opt_ascending=False)
    print(collection.size().getInfo())
    mosaic = sorted_collection.mosaic().float()

    return mosaic

