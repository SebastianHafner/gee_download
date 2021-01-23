import ee

from download_manager import args
from download_manager.config import config

from data_processing import ghsl, utils

from tqdm import tqdm

if __name__ == '__main__':

    ee.Initialize()

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    # extracting parameters from config
    dl_type = cfg.DOWNLOAD.TYPE

    for i, roi in enumerate(tqdm(cfg.ROIS)):
        roi_id = roi['ID']
        epsg = roi['UTM_EPSG']
        if roi_id in cfg.ROIS_SUBSET:
            bbox = utils.extract_bbox(roi)
            img = ghsl.get_ghsl()
            img = img.reproject(crs=epsg, scale=cfg.PIXEL_SPACING)
            img_name = f'ghsl_{roi_id}'

            dl_desc = f'{roi_id.capitalize()}GHSL'

            if dl_type == 'cloud':
                dl_task = ee.batch.Export.image.toCloudStorage(
                    image=img,
                    region=bbox.getInfo()['coordinates'],
                    description=dl_desc,
                    bucket=cfg.DOWNLOAD.BUCKET_NAME,
                    fileNamePrefix=f'{roi_id}/ghsl/{img_name}',
                    scale=cfg.PIXEL_SPACING,
                    crs=epsg,
                    maxPixels=1e12,
                    fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
                )
            else:  # drive
                dl_task = ee.batch.Export.image.toDrive(
                    image=img,
                    region=bbox.getInfo()['coordinates'],
                    description=dl_desc,
                    folder=cfg.DOWNLOAD.DRIVE_FOLDER,
                    fileNamePrefix=img_name,
                    scale=cfg.PIXEL_SPACING,
                    crs=epsg,
                    maxPixels=1e12,
                    fileFormat='GeoTIFF'
                )

            dl_task.start()
