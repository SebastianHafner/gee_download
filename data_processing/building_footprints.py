import ee
from data_processing import utils


def extract_building_footprints(cfg) -> ee.FeatureCollection:

    building_footprints = ee.FeatureCollection([])
    for asset_id in cfg.BUILDING_FOOTPRINTS.ASSETS:
        asset = ee.FeatureCollection(f'users/{cfg.GEE_USERNAME}/{asset_id}')
        building_footprints = building_footprints.merge(asset)

    # only consider building footprints intersecting bbox
    bbox = utils.extract_bbox(cfg)
    building_footprints = building_footprints.filterBounds(bbox)
    # print(f'Found {building_footprints.size().getInfo()} intersecting building footprints')

    return building_footprints


def rasterize(fc: ee.FeatureCollection, layer_name: str) -> ee.Image:

    fc = fc.map(lambda f: ee.Feature(f).set({layer_name: 1}))

    img = fc.reduceToImage([layer_name], ee.Reducer.first()) \
        .unmask() \
        .float() \
        .rename(layer_name)

    return img


def get_building_percentage(cfg) -> ee.Image:

    building_footprints = extract_building_footprints(cfg)
    buildings = rasterize(building_footprints, 'buildings')

    building_percentage = buildings \
        .reproject(crs=cfg.ROI.UTM_EPSG, scale=1) \
        .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1000) \
        .reproject(crs=cfg.ROI.UTM_EPSG, scale=cfg.PIXEL_SPACING) \
        .rename('buildingPercentage')

    return building_percentage


def get_building_density(cfg):

    building_percentage = get_building_percentage(cfg)

    kernel = ee.Kernel.square(ee.Number(cfg.SAMPLING.NEIGHBORHOOD_SIZE).divide(2))
    building_density = building_percentage.reduceNeighborhood(
        reducer=ee.Reducer.mean(),
        kernel=kernel,
        optimization='boxcar'
    ).rename('buildingDensity')

    return building_density


def get_building_data(cfg) -> ee.Image:

    if cfg.BUILDING_FOOTPRINTS.PIXEL_PERCENTAGE:
        return get_building_percentage(cfg)
    else:
        building_footprints = extract_building_footprints(cfg)
        buildings = rasterize(building_footprints, 'buildings')
        return buildings
