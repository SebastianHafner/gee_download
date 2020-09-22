import ee

from download_manager import args
from download_manager.config import config

from data_processing import building_footprints, utils

from tqdm import tqdm


if __name__ == '__main__':

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    # extracting parameters from config
    dl_type = cfg.DOWNLOAD.TYPE
    label_name = cfg.MICROSOFT_BUILDINGS.ID

    # getting region of interest and date range of satellite data
    ee.Initialize()
    roi = utils.extract_bbox(cfg)

    # loading sampling points
    features = utils.load_samples(cfg)
    print(f'Number of patches: {len(features)}')

    for i, feature in enumerate(tqdm(features)):
        patch_id = i + 1

        # creating patch from point coordinates
        patch = utils.feature2patch(cfg, feature)

        dsm = ee.Image("JAXA/ALOS/AW3D30/V2_2").select(['AVE_DSM'], ['Elevation']).float()
        dsm = dsm.unitScale(-1000, 9000).clamp(0, 1).unmask().float()

        img_name = f'dsm_{cfg.ROI.ID}_patch{patch_id}'

        dl_desc = f'{dl_type.capitalize()}DSMDownloadPatch{patch_id}of{len(features)}'

        if dl_type == 'cloud':

            dl_task = ee.batch.Export.image.toCloudStorage(
                image=dsm,
                region=patch.getInfo()['coordinates'],
                description=dl_desc,
                bucket=cfg.DOWNLOAD.BUCKET_NAME,
                fileNamePrefix=f'{cfg.ROI.ID}/dsm/{img_name}',
                scale=cfg.PIXEL_SPACING,
                crs=cfg.ROI.UTM_EPSG,
                maxPixels=1e6,
                fileFormat='GeoTIFF'
            )

        else:  # drive

            dl_task = ee.batch.Export.image.toDrive(
                image=dsm,
                region=patch.getInfo()['coordinates'],
                description=dl_desc,
                folder=cfg.DOWNLOAD.DRIVE_FOLDER,
                fileNamePrefix=img_name,
                scale=cfg.PIXEL_SPACING,
                crs=cfg.ROI.UTM_EPSG,
                maxPixels=1e6,
                fileFormat='GeoTIFF'
            )

        dl_task.start()
