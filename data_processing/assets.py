import ee

# set user name
__USERNAME__ = 'hafnersailing'


# get bounding box of city from assets
def get_bbox(city):

    bbox = ee.FeatureCollection(f'users/{__USERNAME__}/urban_data/cities_bounding_boxes') \
        .filterMetadata('name', 'equals', city) \
        .first() \
        .geometry()

    return bbox


def get_orbit_numbers(city) -> dict:
    city_metadata = ee.FeatureCollection(f'users/{__USERNAME__}/urban_data/cities_bounding_boxes') \
        .filterMetadata('name', 'equals', city) \
        .first()

    dict_orbit_numbers = {}
    for orbit in ['asc', 'desc']:
        orbit_numbers = city_metadata.get(f'{orbit}Orbits').getInfo().split(' ')
        if not orbit_numbers[0] == '-1':
            orbit_numbers = [int(n) for n in orbit_numbers]
        else:
            orbit_numbers = []
        dict_orbit_numbers[orbit] = orbit_numbers
    return dict_orbit_numbers


# retrieve Global Urban Footprint (GUF) data
def get_guf(city):

    # first finding out with GUF tiles intersect with bounding box of city
    bounding_boxes = ee.FeatureCollection(f'users/{__USERNAME__}/urban_data/GUF_bounding_boxes')
    intersections = bounding_boxes.filterBounds(get_bbox(city))
    n_tiles = intersections.size().getInfo()
    if n_tiles == 0:
        print(f'No GUF tiles found for {city}')
    intersections = intersections.toList(n_tiles)

    # loading image for each tile
    tiles = []
    for i in range(n_tiles):
        feature = intersections.get(i)
        file_name = ee.Feature(feature).get('fileName').getInfo()
        asset_name = f'users/{__USERNAME__}/GUF/{file_name}'
        tile = ee.Image(asset_name)
        tiles.append(tile)

    # mosaic tiles together
    guf = ee.ImageCollection(tiles) \
        .mosaic() \
        .divide(255) \
        .byte() \
        .unmask() \
        .rename('GUF')

    return guf


def get_wsf(city):

    wsf = ee.Image(f'users/{__USERNAME__}/WSF/WSF2015_{city}') \
        .divide(255) \
        .byte() \
        .unmask() \
        .rename('WSF')

    return wsf


def get_real_estate_data_stockholm():

    real_estate_data = ee.FeatureCollection(f'users/{__USERNAME__}/Stockholm/real_estate_data')

    real_estate_data = real_estate_data.map(lambda f: ee.Feature(f).set({'urban': 1}))
    # print(real_estate_data.first().getInfo())
    # print(real_estate_data.size().getInfo())

    real_estate_data = real_estate_data.reduceToImage(['urban'], ee.Reducer.first()) \
        .unmask() \
        .byte() \
        .rename('urban')

    return real_estate_data


def get_building_percentage():

    real_estate_data = ee.FeatureCollection(f'users/{__USERNAME__}/Stockholm/real_estate_data') \
        .map(lambda f: ee.Feature(f).set({'urban': 1}))

    building_raster = real_estate_data.reduceToImage(['urban'], ee.Reducer.first()) \
        .unmask() \
        .float()
    # .reproject(crs='EPSG:4326', scale=10) \
    building_percentage = building_raster.reproject(crs='EPSG:4326', scale=1) \
        .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1000) \
        .rename('bp')

    return building_percentage


if __name__ == '__main__':

    # ee.Initialize()

    # city = 'StockholmTest'
    # guf = get_guf(city)
    # print(guf.getInfo())
    print('test')
    # get_real_estate_data_stockholm()


