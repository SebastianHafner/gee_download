import ee

from download_manager import args
from download_manager.config import config

from data_processing import building_footprints, exports, utils


def density_sampling(cfg, points: bool = True) -> ee.FeatureCollection:

    # extracting sampling properties from config
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

    building_percentage = building_footprints.get_building_percentage(cfg)

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
    sampling_patches = sampling_patches.map(lambda f: ee.Feature(f).transform(proj='EPSG:4326', maxError=0.01))

    sampling_points = sampling_points.map(lambda f: ee.Feature(f).transform(proj='EPSG:4326', maxError=0.01))

    samples = sampling_points if points else sampling_patches

    return samples


if __name__ == '__main__':
    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    ee.Initialize()

    points = True
    samples = density_sampling(cfg, points=points)
    sample_name = 'points' if points else 'patches'

    dl_desc = f'DriveSample{sample_name.capitalize()}{cfg.ROI.ID}'
    file_name = f'{sample_name}_{cfg.ROI.ID}'

    dl_task = ee.batch.Export.table.toDrive(
        collection=samples,
        description=dl_desc,
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix=file_name,
        fileFormat=cfg.DOWNLOAD.TABLE_FORMAT
    )

    dl_task.start()

