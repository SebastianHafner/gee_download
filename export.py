import ee


def to_drive(img: ee.Image, bbox: ee.Geometry, folder: str, file_name: str, patch_size: int = 0,
             scale: int = 10, crs: str = 'EPSG:4326'):
    if patch_size == 0:
        task = ee.batch.Export.image.toDrive(
            image=img,
            region=bbox.getInfo()['coordinates'],
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
            region=bbox.getInfo()['coordinates'],
            description='PythonToDriveExport',
            folder=folder,
            fileNamePrefix=file_name,
            scale=scale,
            crs=crs,
            maxPixels=1e10,
            fileDimensions=patch_size,
            fileFormat='GeoTIFF'
        )
    task.start()


def to_cloud(img: ee.Image, bbox: ee.Geometry, bucket: str, folder: str, file_name: str, patch_size: int = 0,
             scale: int = 10, crs: str = 'EPSG:4326'):

    if patch_size == 0:
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
    task.start()
