import ee
import json

from download_manager import args
from download_manager.config import config

import satellite_data
import building_footprints
import export
import sampling
import utils
import s1


def setup(args):
    cfg = config.new_config()
    cfg.merge_from_file(f'configs/{args.config_file}.yaml')
    cfg.merge_from_list(args.opts)
    cfg.NAME = args.config_file
    return cfg


if __name__ == '__main__':
    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = setup(args)

    ee.Initialize()

    roi = utils.extract_bbox(cfg)
    # print(roi.getInfo())
    point = roi.centroid()
    # print(point.getInfo())

    patch_size = ee.Number(256)
    pixel_spacing = ee.Number(10)
    patch_area = patch_size.multiply(pixel_spacing).pow(2)
    offset = ee.Number(5)
    crsUTM = cfg.ROI.UTM_EPSG
    crsWGS84 = 'EPSG:4326'

    point = point.transform(crsUTM)
    buffer_distance = patch_size.divide(2).multiply(pixel_spacing).subtract(offset)
    print(buffer_distance.getInfo())
    patch = point.buffer(distance=buffer_distance, proj=crsUTM).bounds(proj=crsUTM)
    patch = patch.transform(crsWGS84, 0.001)

    print('patch projection')
    print(patch.getInfo())

    collection = ee.ImageCollection('COPERNICUS/S2') \
        .filterDate('2019-07-27', '2019-07-30') \
        .filterBounds(roi)
    # print(collection.size().getInfo())

    img = collection.sort('CLOUDY_PIXEL_PERCENTAGE', False) \
        .mosaic() \
        .divide(10000) \
        .select(['B8', 'B4', 'B3'])

    print('image projection')
    print(img.projection().getInfo())

    label = building_footprints.get_building_percentage(cfg)

    satellite_data = satellite_data.get_satellite_data(cfg)
    print(satellite_data.bandNames().getInfo())
    # satellite_data = satellite_data.reproject(cfg.ROI.UTM_EPSG, None, 10)
    satellite_data = satellite_data.changeProj(srcProj=crsWGS84, dstProj=crsUTM)
    print('cloud free projection')
    print(satellite_data.projection().getInfo())


    def add_cloud_band(img: ee.Image) -> ee.Image:
        qa60 = img.select(['QA60'])
        clouds = qa60.bitwiseAnd(1<<10).Or(qa60.bitwiseAnd(1<<11))
        return img.addBands(clouds.rename('clouds'))

    def addStats(img: ee.Image) -> ee.Image:
        patch = ee.Geometry(img.get('patch'))
        patch_area = patch.area(0.001)

        img_footprint = ee.Algorithms.GeometryConstructors.Polygon(
            ee.Geometry(img.get('system:footprint')).coordinates()
        )
        intersection = patch.intersection(img_footprint, ee.ErrorMargin(0.001))
        intersection_area = intersection.area(0.001)
        coverage = ee.Number(intersection_area).divide(patch_area)

        cloud_area_img = img.select('clouds').multiply(ee.Image.pixelArea())
        stats = cloud_area_img.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=patch,
            scale=10,
            maxPixels=1e12
        )
        cloud_area = stats.get('clouds')
        cloud_coverage = ee.Number(cloud_area).divide(patch_area)
        img = img.set('patchCoverage', coverage)
        img = img.set('patchCloudCoverage', cloud_coverage)

        score = coverage.multiply(cloud_coverage)
        img = img.set('patchScore', score)
        return img

    s2toa = ee.ImageCollection('COPERNICUS/S2') \
        .filterDate('2019-06-01', '2019-09-30') \
        .filterBounds(patch) \
        .map(add_cloud_band) \
        .map(lambda img: img.set('patch', patch)) \
        .map(addStats)

    print(s2toa.size().getInfo())

    cloud_free_candidates = s2toa \
        .filterMetadata('patchCoverage', 'equals', 1) \
        .filterMetadata('patchCloudCoverage', 'equals', 0)

    best = cloud_free_candidates \
        .filterMetadata('CLOUDY_PIXEL_PERCENTAGE', 'less_than', 5) \
        .sort('CLOUDY_PIXEL_PERCENTAGE') \
        .first()

    best_local = cloud_free_candidates \
        .sort('CLOUDY_PIXEL_PERCENTAGE') \
        .first()

    s2toa_no_clouds = s2toa.map(lambda img: img.updateMask(img.select('clouds').Not()))

    cloud_free_mosaic = s2toa_no_clouds.sort('patchScore', False).mosaic()

    final = ee.ImageCollection([cloud_free_mosaic, best_local, best]).mosaic()
    final = final.select(['B8', 'B4', 'B3']).divide(10000).clamp(0, 1).float()
    print(final.bandNames().getInfo())

    s1img = s1.single_orbit_mean(patch, ee.DateRange('2019-06-01', '2019-09-30'))

    # task = ee.batch.Export.image.toDrive(
    #     image=satellite_data.float(),
    #     region=patch.getInfo()['coordinates'],
    #     description='PythonToDriveExport',
    #     folder=cfg.DOWNLOAD.DRIVE_FOLDER,
    #     fileNamePrefix='debug_patch_v1',
    #     scale=pixel_spacing.getInfo(),
    #     crs='EPSG:32634',
    #     maxPixels=patch_size.pow(2).getInfo(),
    #     fileFormat='GeoTIFF'
    # )

    task = ee.batch.Export.image.toDrive(
        image=s1img,
        region=patch.getInfo()['coordinates'],
        description='PythonToDriveExport',
        folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        fileNamePrefix='debug_patch_v4',
        scale=pixel_spacing.getInfo(),
        crs=crsUTM,
        maxPixels=patch_size.pow(2).getInfo(),
        fileFormat='GeoTIFF'
    )

    # task.start()


    # getting all satellite data
    # satellite_data = satellite_data.get_satellite_data(cfg)
    # print(satellite_data.bandNames().getInfo())

    #   building_data = building_footprints.get_building_percentage(cfg)






    # include label if specified
    # if cfg.BUILDING_FOOTPRINTS.INCLUDE:
    # building_label = building_footprints.get_building_data(cfg)
    # export_data = ee.Image.cat([building_label])
        # print(export_data.bandNames().getInfo())
    file_name = 'C:/Users/hafne/drive_downloads/patch_samples_stockholm.geojson'
    with open(file_name) as f:
        patches = json.load(f).get('features')



    # patches = [patch for patch in patches if patch.get('properties').get('densityZone') != 0]
    # print(len(patches))
    # for patch in patches:
    #     print(patch)
    #     geom = utils.patch2geom(cfg, patch)
    #     geom = geom.transform(proj='EPSG:4326', maxError=cfg.ERROR_MARGIN)
    #     print(geom.getInfo())
    #     study_area = utils.extract_bbox(cfg)
    #
    #     print(study_area.contains(geom).getInfo())
    #     print(satellite_data.projection().getInfo())
    #     satellite_date = satellite_data.reproject(cfg.ROI.UTM_EPSG, None, cfg.PIXEL_SPACING)
    #     print(satellite_data.projection().getInfo())
    #     print(building_data.projection().getInfo())
    #
    #     satellite_data = satellite_data.reproject(crs=cfg.ROI.UTM_EPSG, scale=cfg.PIXEL_SPACING)
    #     task = ee.batch.Export.image.toDrive(
    #         image=satellite_data,
    #         region=geom.getInfo()['coordinates'],
    #         description='PythonToDriveExport',
    #         folder=cfg.DOWNLOAD.DRIVE_FOLDER,
    #         fileNamePrefix='debug_patch',
    #         scale=10,
    #         crs='EPSG:32634',
    #         maxPixels=1e7,
    #         fileFormat='GeoTIFF'
    #     )
        # task.start()

    #    break

    # create task for export
    # task = export.construct_task(cfg, export_data)

    # run_export = False
    # if run_export:
    #     task.start()














