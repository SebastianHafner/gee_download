import ee

from download_manager import args
from download_manager.config import config

import building_footprints
import export
import utils



def setup(args):
    cfg = config.new_config()
    cfg.merge_from_file(f'configs/{args.config_file}.yaml')
    cfg.merge_from_list(args.opts)
    cfg.NAME = args.config_file
    return cfg


def density_sampling(cfg) -> ee.FeatureCollection:

    bbox = utils.extract_bbox(cfg)
    area = ee.Number(ee.Geometry(bbox).area(cfg.ERROR_MARGIN))
    patch_size = ee.Number(cfg.SAMPLING.PATCH_SIZE)
    pixel_spacing = ee.Number(cfg.PIXEL_SPACING)
    patch_area = patch_size.multiply(pixel_spacing).pow(2)

    # computing sample size based on size of roi
    sample_size = area.divide(patch_area).multiply(cfg.SAMPLING.SAMPLE_FRACTION).int()
    max_sample_size = ee.Number(cfg.SAMPLING.MAX_SAMPLE_SIZE)
    sample_size = ee.Number(ee.Algorithms.If(sample_size.gt(max_sample_size), max_sample_size, sample_size))
    samples_per_class = sample_size.subtract(sample_size.mod(4)).divide(4)

    print(sample_size.getInfo())
    print(samples_per_class.getInfo())

    sampling_region = bbox.buffer(
        distance=ee.Number(cfg.SAMPLING.PATCH_SIZE).multiply(cfg.PIXEL_SPACING).divide(2).multiply(-1),
        proj=cfg.ROI.UTM_EPSG
    )
    print(sampling_region.getInfo())

    # building_footprints = ee.FeatureCollection('users/hafnersailing/Stockholm/real_estate_data') \
    #     .filterBounds(bbox) \
    #     .map(lambda f: ee.Feature(f).set({'urban': 1}))
    #
    # building_raster = building_footprints.reduceToImage(['urban'], ee.Reducer.first()) \
    #     .unmask() \
    #     .float() \
    #     .rename('urban') \
    #     .clip(bbox)
    #
    # building_percentage = building_raster.reproject(crs=cfg.ROI.UTM_EPSG, scale=1) \
    #     .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1000) \
    #     .rename('buildingPercentage')

    building_percentage = building_footprints.get_building_percentage(cfg)

    # building_percentage = building_footprints.get_building_percentage(cfg)
    kernel = ee.Kernel.square(ee.Number(cfg.SAMPLING.NEIGHBORHOOD_SIZE).divide(2))
    urban_density = building_percentage.reduceNeighborhood(
        reducer=ee.Reducer.mean(),
        kernel=kernel,
        optimization='boxcar'
    ).rename('urbanDensity')

    density_zones = urban_density.expression(
        '(d <= 0.00001) ? 0 : (d <= 0.01) ? 1 : (d <= 0.1) ? 2 : 3',
        {'d': urban_density}
    ).rename('densityZone')

    sampling_points = density_zones.stratifiedSample(
        numPoints=samples_per_class,
        region=sampling_region,
        scale=cfg.PIXEL_SPACING,
        projection=cfg.ROI.UTM_EPSG,
        dropNulls=True,
        geometries=True
    )

    def point2patch(feature: ee.Feature) -> ee.Feature:
        point = ee.Feature(feature).geometry().transform(utils.extract_crs(cfg))
        buffer_distance = patch_size.divide(2).multiply(pixel_spacing).subtract(pixel_spacing.divide(2))
        patch = point.buffer(distance=buffer_distance, proj=utils.extract_crs(cfg)).bounds(proj=utils.extract_crs(cfg))
        return ee.Feature(patch).copyProperties(feature)

    sampling_patches = sampling_points.map(point2patch)

    sampling_points = sampling_points.map(lambda f: ee.Feature(f).transform(proj='EPSG:4326', maxError=0.01))
    sampling_patches = sampling_patches.map(lambda f: ee.Feature(f).transform(proj='EPSG:4326', maxError=0.01))
    return sampling_points

if __name__ == '__main__':
    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = setup(args)

    ee.Initialize()

    sampling_patches = density_sampling(cfg)

    task = export.table_to_drive(fc=sampling_patches, folder='gee_test_exports',
                                 file_name=f'points_{cfg.ROI.ID}')
    task.start()


