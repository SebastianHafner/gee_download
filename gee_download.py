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
import s2


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

    # extracting parameters from config
    patch_size = ee.Number(cfg.SAMPLING.PATCH_SIZE)
    pixel_spacing = ee.Number(cfg.PIXEL_SPACING)
    crsUTM = cfg.ROI.UTM_EPSG
    crsWGS84 = 'EPSG:4326'

    # getting region of interest and date range of satellite data
    ee.Initialize()
    roi = utils.extract_bbox(cfg)
    date_range = utils.extract_date_range(cfg)

    # loading sampling points
    with open(f'{cfg.PATH}points_{cfg.ROI.ID}.geojson') as f:
        features = json.load(f)['features']
    features = [feature for feature in features if feature.get('properties').get('densityZone') != 0]
    print(f'Number of patches: {len(features)}')

    for i, feature in enumerate(features):

        print(f'Patch {i}')
        coords = feature['geometry']['coordinates']
        point = ee.Geometry.Point(coords)

        point = point.transform(crsUTM)
        buffer_distance = patch_size.divide(2).multiply(pixel_spacing)
        patch = point.buffer(distance=buffer_distance, proj=crsUTM).bounds(proj=crsUTM)
        patch = patch.transform(crsWGS84, 0.001)

        s1mean = s1.single_orbit_mean(patch, date_range)
        s1task = ee.batch.Export.image.toCloudStorage(
            image=s1mean,
            region=patch.getInfo()['coordinates'],
            description='PythonToCloudExport',
            bucket=cfg.DOWNLOAD.BUCKET_NAME,
            fileNamePrefix=f'{cfg.ROI.ID}/sentinel1/sentinel1_{cfg.ROI.ID}_patch{i + 1}',
            scale=cfg.PIXEL_SPACING,
            crs=cfg.ROI.UTM_EPSG,
            maxPixels=1e6,
            fileFormat='GeoTIFF'
        )

        # s1task = ee.batch.Export.image.toDrive(
        #     image=s1mean,
        #     region=patch.getInfo()['coordinates'],
        #     description='PythonToDriveExport',
        #     folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        #     fileNamePrefix=f'sentinel1_{cfg.ROI.ID}_patch{i + 1}',
        #     scale=cfg.PIXEL_SPACING,
        #     crs=cfg.ROI.UTM_EPSG,
        #     maxPixels=1e6,
        #     fileFormat='GeoTIFF'
        # )

        s2mosaic = s2.cloud_free_mosaic(patch, date_range)
        s2task = ee.batch.Export.image.toCloudStorage(
            image=s2mosaic,
            region=patch.getInfo()['coordinates'],
            description='PythonToCloudExport',
            bucket=cfg.DOWNLOAD.BUCKET_NAME,
            fileNamePrefix=f'{cfg.ROI.ID}/sentinel2/sentinel2_{cfg.ROI.ID}_patch{i + 1}',
            scale=cfg.PIXEL_SPACING,
            crs=cfg.ROI.UTM_EPSG,
            maxPixels=1e6,
            fileFormat='GeoTIFF'
        )

        # s2task = ee.batch.Export.image.toDrive(
        #     image=s2mosaic,
        #     region=patch.getInfo()['coordinates'],
        #     description='PythonToDriveExport',
        #     folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        #     fileNamePrefix=f'sentinel2_{cfg.ROI.ID}_patch{i + 1}',
        #     scale=cfg.PIXEL_SPACING,
        #     crs=cfg.ROI.UTM_EPSG,
        #     maxPixels=1e6,
        #     fileFormat='GeoTIFF'
        # )

        label = building_footprints.get_building_percentage(cfg)
        btask = ee.batch.Export.image.toCloudStorage(
            image=label,
            region=patch.getInfo()['coordinates'],
            description='PythonToCloudExport',
            bucket=cfg.DOWNLOAD.BUCKET_NAME,
            fileNamePrefix=f'{cfg.ROI.ID}/buildings/buildings_{cfg.ROI.ID}_patch{i + 1}',
            scale=cfg.PIXEL_SPACING,
            crs=cfg.ROI.UTM_EPSG,
            maxPixels=1e6,
            fileFormat='GeoTIFF'
        )

        # btask = ee.batch.Export.image.toDrive(
        #     image=label,
        #     region=patch.getInfo()['coordinates'],
        #     description='PythonToDriveExport',
        #     folder=cfg.DOWNLOAD.DRIVE_FOLDER,
        #     fileNamePrefix=f'buildings_{cfg.ROI.ID}_patch{i + 1}',
        #     scale=cfg.PIXEL_SPACING,
        #     crs=cfg.ROI.UTM_EPSG,
        #     maxPixels=1e6,
        #     fileFormat='GeoTIFF'
        # )

        s1task.start()
        s2task.start()
        btask.start()





