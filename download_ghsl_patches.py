import ee

from download_manager import args
from download_manager.config import config

from data_processing import ghsl, utils

from tqdm import tqdm

if __name__ == '__main__':

    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = config.setup(args, 'configs/urban_extraction')

    # extracting parameters from config
    dl_type = cfg.DOWNLOAD.TYPE
    label_name = cfg.MICROSOFT_BUILDINGS.ID

    ee.Initialize()

    for i, roi in enumerate(tqdm(cfg.ROIS)):
        roi_id = roi['ID']
        epsg = roi['UTM_EPSG']
        if roi_id in cfg.ROIS_SUBSET:
            bbox = utils.extract_bbox(roi)
            img = ghsl.get_ghsl()
            img = img.reproject(crs=epsg, scale=cfg.PIXEL_SPACING)
            img_name = f'ghsl_{roi_id}_'

            dl_desc = f'{roi_id.capitalize()}GHSL'
            dl_task = ee.batch.Export.image.toCloudStorage(
                image=img,
                region=bbox.getInfo()['coordinates'],
                description=dl_desc,
                bucket=cfg.DOWNLOAD.BUCKET_NAME,
                fileNamePrefix=f'{roi_id}/ghsl/{img_name}',
                scale=cfg.PIXEL_SPACING,
                crs=epsg,
                fileDimensions=cfg.SAMPLING.PATCH_SIZE,
                maxPixels=1e12,
                skipEmptyTiles=True,
                fileFormat=cfg.DOWNLOAD.IMAGE_FORMAT
            )

            dl_task.start()
