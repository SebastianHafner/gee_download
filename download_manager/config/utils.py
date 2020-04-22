import ee


def extract_bbox(cfg) -> ee.Geometry:
    lng_min, lng_max = cfg.ROI.LNG_RANGE
    lat_min, lat_max = cfg.ROI.LAT_RANGE
    bbox = ee.Geometry.Rectangle([[lng_min, lat_min], [lng_max, lat_max]])
    # TODO: transform to correct crs

    return bbox


def extract_crs(cfg) -> str:
    return cfg.ROI.UTM_EPSG


def extract_date_range(cfg):
    date_range = ee.DateRange(*cfg.SATELLITE_DATA.DATE_RANGE)
    return date_range


def normalize(min_value: float, max_value: float) -> callable:
    def normalize_mapper(img: ee.Image):
        return img.subtract(min_value).divide(max_value - min_value).clamp(0, 1).copyProperties(img)
    return normalize_mapper