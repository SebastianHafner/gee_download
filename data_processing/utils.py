import ee
import json
import rasterio
from pathlib import Path


def extract_bbox(roi) -> ee.Geometry:
    lng_min, lng_max = roi['LNG_RANGE']
    lat_min, lat_max = roi['LAT_RANGE']
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


def load_json(file: Path):
    with open(str(file)) as f:
        d = json.load(f)
    return d


def offset_months(year: int, month: int, dt: int):
    months = year * 12 + month + dt
    new_year = months // 12
    new_month = months % 12
    if new_month == 0:
        new_month = 12
        new_year -= 1
    return new_year, new_month


def month_days(month: int) -> int:
    days = 31 if month in [1, 3, 5, 7, 8, 10, 12] else 30
    return days if not month == 2 else 28


def read_json(file: Path):
    with open(str(file)) as f:
        d = json.load(f)
    return d


def write_json(file: Path, data):
    with open(str(file), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    year = 2019
    month = 6
    offset = 6
    new_year, new_month = offset_months(year, month, offset)
    print(f'{new_year} {new_month}')