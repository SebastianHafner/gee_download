import ee

"""
normalizedDifference(bandNames)
Computes the normalized difference between two bands. If the bands to use are not specified, uses the first two bands.
The normalized difference is computed as (first − second) / (first + second). Note that negative input values are
forced to 0 so that the result is confined to the range (-1, 1).
"""


def get_spectral_index_func(spectral_index):
    spectral_index_functions = {
        'NDBI': NDBI,
        'MNDWI': MNDWI,
        'NDVI': NDVI,
        'NDMIR': NDMIR,
        'NDRB': NDRB,
        'NDGB': NDGB
    }
    return spectral_index_functions.get(spectral_index)

# Normalized Difference Built-Up Index (NDBI)
def NDBI(img):
    img = ee.Image(img)
    ndbi = img.normalizedDifference(['SWIR1', 'NIR']).rename('NDBI')

    return img.addBands(ndbi)


# Modified Normalized Difference Water Index (MNDWI)
def MNDWI(img):
    img = ee.Image(img)
    mndwi = img.normalizedDifference(['Green', 'NIR']).rename('MNDWI')
    return img.addBands(mndwi)


# Normalized Difference Vegetation Index (NDVI)
def NDVI(img):
    img = ee.Image(img)
    ndvi = img.normalizedDifference(['NIR', 'Red']).rename('NDVI')
    return img.addBands(ndvi)


# Normalized Difference Middle Infrared (NDMIR)
def NDMIR(img):
    img = ee.Image(img)
    ndmir = img.normalizedDifference(['SWIR1', 'SWIR2']).rename('NDMIR')
    return img.addBands(ndmir)


# Normalized Difference Red Blue (NDRB)
def NDRB(img):
    img = ee.Image(img)
    ndrb = img.normalizedDifference(['Red', 'Blue']).rename('NDRB')
    return img.addBands(ndrb)


# Normalized Difference Vegetation Index (NDVI)
def NDGB(img):
    img = ee.Image(img)
    ndgb = img.normalizedDifference(['Green', 'Blue']).rename('NDGB')
    return img.addBands(ndgb)