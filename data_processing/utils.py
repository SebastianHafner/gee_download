import ee
import json
import rasterio
from pathlib import Path


def extract_bbox(cfg) -> ee.Geometry:
    lng_min, lng_max = cfg.ROI.LNG_RANGE
    lat_min, lat_max = cfg.ROI.LAT_RANGE
    bbox = ee.Geometry.Rectangle([[lng_min, lat_min], [lng_max, lat_max]])
    # bbox = bbox.transform(proj=cfg.ROI.UTM_EPSG, maxError=cfg.ERROR_MARGIN)
    return bbox


def extract_crs(cfg) -> str:
    return cfg.ROI.UTM_EPSG


def extract_date_range(cfg):
    date_range = ee.DateRange(*cfg.SATELLITE_DATA.DATE_RANGE)
    return date_range


# loading sample points from (geo)json file
def load_samples(cfg) -> list:
    # loading sampling points
    samples_file = Path(f'{cfg.SAMPLING.PATH}/points_{cfg.ROI.ID}.geojson')
    with open(samples_file) as f:
        features = json.load(f)['features']
    features = [feature for feature in features if feature.get('properties').get('densityZone') != 0]
    return features


def normalize(min_value: float, max_value: float) -> callable:
    def normalize_mapper(img: ee.Image):
        return img.subtract(min_value).divide(max_value - min_value).clamp(0, 1).copyProperties(img)

    return normalize_mapper


# creating patch from from point sample according to config
def feature2patch(cfg, feature) -> ee.Geometry:

    # extracting properties
    patch_size = ee.Number(cfg.SAMPLING.PATCH_SIZE)
    pixel_spacing = ee.Number(cfg.PIXEL_SPACING)
    crsUTM = cfg.ROI.UTM_EPSG
    crsWGS84 = 'EPSG:4326'

    # extracting coordinates from feature
    coords = feature['geometry']['coordinates']

    # converting point to patch
    point = ee.Geometry.Point(coords)
    point = point.transform(crsUTM)
    buffer_distance = patch_size.divide(2).multiply(pixel_spacing)
    patch = point.buffer(distance=buffer_distance, proj=crsUTM).bounds(proj=crsUTM)
    patch = patch.transform(crsWGS84, 0.001)

    return patch


# reading in geotiff file as numpy array
def read_tif(file: Path):
    if not file.exists():
        raise FileNotFoundError(f'File {file} not found')

    with rasterio.open(file) as dataset:
        arr = dataset.read()  # (bands X height X width)
        transform = dataset.transform
        crs = dataset.crs

    return arr.transpose((1, 2, 0)), transform, crs


# writing an array to a geo tiff file
def write_tif(file: Path, arr, transform, crs):
    if not file.parent.exists():
        file.parent.mkdir()

    height, width, bands = arr.shape
    with rasterio.open(
            file,
            'w',
            driver='GTiff',
            height=height,
            width=width,
            count=bands,
            dtype=arr.dtype,
            crs=crs,
            transform=transform,
    ) as dst:
        for i in range(bands):
            dst.write(arr[:, :, i], i + 1)




