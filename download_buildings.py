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

    for roi in tqdm(cfg.ROIS):
        roi_id = roi['ID']
        epsg = roi['UTM_EPSG']
        if roi_id in cfg.ROIS_SUBSET:

            bbox = utils.extract_bbox(roi)

            buildings = building_footprints.extract_building_footprints(cfg, roi)
            buildings = building_footprints.rasterize(buildings, 'buildings')
            buildings = buildings.uint8()

            img_name = f'buildings_{roi_id}'
            dl_desc = f'DownloadBuildings{str(roi_id).capitalize()}'

            dl_task = ee.batch.Export.image.toCloudStorage(
                image=buildings,
                region=bbox.getInfo()['coordinates'],
                description=dl_desc,
                bucket=cfg.DOWNLOAD.BUCKET_NAME,
                fileNamePrefix=f'buildings/{img_name}',
                scale=cfg.PIXEL_SPACING,
                crs=epsg,
                maxPixels=1e12,
                fileFormat='GeoTIFF'
            )

            dl_task.start()
