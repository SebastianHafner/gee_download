import ee
from data_processing import utils


def construct_task(cfg, data):

    task = None
    if cfg.DOWNLOAD.TYPE == 'drive':
        # exporting an image if no sampling was applied
        if cfg.SAMPLING.TYPE == 'none':
            task = image_to_drive(
                img=data,
                roi=utils.extract_bbox(cfg),
                folder=cfg.DOWNLOAD.DRIVE_FOLDER,
                file_name=cfg.BBOX.ID,
                patch_size=cfg.SAMPLING.PATCH_SIZE
            )
        # if image was sampled exporting a feature collection
        else:
            task = table_to_drive(fc=data, properties=['buildingPercentage', 'B2', 'B3', 'B4', 'B8'],
                                  folder=cfg.DOWNLOAD.DRIVE_FOLDER, file_name=cfg.BBOX.ID)

    if cfg.DOWNLOAD.TYPE == 'cloud' and False:
        pass

    return task


def table_to_drive(fc: ee.FeatureCollection, folder: str, file_name: str):
    task = ee.batch.Export.table.toDrive(
        collection=fc,
        description='PythonToDriveExport',
        folder=folder,
        fileNamePrefix=file_name,
        fileFormat='GeoJSON'
    )
    return task


def image_to_drive(img: ee.Image, roi: ee.Geometry, folder: str, file_name: str, patch_size: int = -1,
                   scale: int = 10, crs: str = 'EPSG:4326'):
    if patch_size == -1:
        task = ee.batch.Export.image.toDrive(
            image=img,
            region=roi.getInfo()['coordinates'],
            description='PythonToDriveExport',
            folder=folder,
            fileNamePrefix=file_name,
            scale=scale,
            crs=crs,
            maxPixels=1e10,
            fileFormat='GeoTIFF'
        )
    else:
        task = ee.batch.Export.image.toDrive(
            image=img,
            region=roi.getInfo()['coordinates'],
            description='PythonToDriveExport',
            folder=folder,
            fileNamePrefix=file_name,
            scale=scale,
            crs=crs,
            maxPixels=1e10,
            fileDimensions=patch_size,
            fileFormat='GeoTIFF'
        )
    return task


def image_to_cloud(img: ee.Image, bbox: ee.Geometry, bucket: str, folder: str, file_name: str, patch_size: int = -1,
                   scale: int = 10, crs: str = 'EPSG:4326'):

    if patch_size == -1:
        task = ee.batch.Export.image.toCloudStorage(
            image=img,
            region=bbox.getInfo()['coordinates'],
            description='PythonToCloudExport',
            bucket=bucket,
            fileNamePrefix= f'{folder}/{file_name}',
            scale=scale,
            crs=crs,
            maxPixels=1e10,
            fileFormat='GeoTIFF'
        )
    else:
        task = ee.batch.Export.image.toCloudStorage(
            image=img,
            region=bbox.getInfo()['coordinates'],
            description='PythonToCloudExport',
            bucket=bucket,
            fileNamePrefix= f'{folder}/{file_name}_',
            scale=scale,
            crs=crs,
            maxPixels=1e10,
            fileDimensions=patch_size,
            fileFormat='GeoTIFF'
        )
    return task
