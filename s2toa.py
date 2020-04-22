import ee
import math


# calcCloudCoverage: Calculates a mask for clouds in the image.
#        input: im - Image from image collection with a valid mask layer
#        output: original image with added stats.
#                - CLOUDY_PERCENTAGE: The percentage of the image area affected by clouds
#                - ROI_COVERAGE_PERCENT: The percentage of the ROI region this particular image covers
#                - CLOUDY_PERCENTAGE_ROI: The percentage of the original ROI which is affected by the clouds in
#                  this image
#                - cloudScore: A per pixel score of cloudiness
def compute_cloud_coverage(img, cloud_thresh=0.2):
    img_poly = ee.Algorithms.GeometryConstructors.Polygon(
        ee.Geometry(img.get('system:footprint')).coordinates()
    )

    roi = ee.Geometry(img.get('ROI'))

    intersection = roi.intersection(img_poly, ee.ErrorMargin(0.5))
    cloud_mask = img.select(['cloudScore']).gt(cloud_thresh).clip(roi).rename('cloudMask')

    cloud_area_img = cloud_mask.multiply(ee.Image.pixelArea())

    stats = cloud_area_img.reduceRegion(
      reducer=ee.Reducer.sum(),
      geometry=roi,
      scale=10,
      maxPixels=1e12,
      bestEffort=True,
      tileScale=16
    )

    max_area_error = 10
    cloud_percent = ee.Number(stats.get('cloudMask')).divide(img_poly.area(max_area_error)).multiply(100)
    coverage_percent = ee.Number(intersection.area(max_area_error)).divide(roi.area(max_area_error)).multiply(100)
    cloud_percent_roi = ee.Number(stats.get('cloudMask')).divide(roi.area(max_area_error)).multiply(100)

    img = img.set('CLOUDY_PERCENTAGE', cloud_percent)
    img = img.set('ROI_COVERAGE_PERCENT', coverage_percent)
    img = img.set('CLOUDY_PERCENTAGE_ROI', cloud_percent_roi)

    return img


def compute_cloud_score(img):
    toa = img.select(['B1','B2','B3','B4','B5','B6','B7','B8','B8A', 'B9','B10', 'B11','B12']) \
              .divide(10000)

    toa = toa.addBands(img.select(['QA60']))

    # ['QA60', 'B1','B2',    'B3',    'B4',   'B5','B6','B7', 'B8','  B8A', 'B9',          'B10', 'B11','B12']
    # ['QA60','cb', 'blue', 'green', 'red', 're1','re2','re3','nir', 'nir2', 'waterVapor', 'cirrus','swir1', 'swir2'])

    # Compute several indicators of cloudyness and take the minimum of them.
    score = ee.Image(1)

    # Clouds are reasonably bright in the blue and cirrus bands.
    score = score.min(rescale(toa, 'img.B2', [0.1, 0.5]))
    score = score.min(rescale(toa, 'img.B1', [0.1, 0.3]))
    score = score.min(rescale(toa, 'img.B1 + img.B10', [0.15, 0.2]))

    # Clouds are reasonably bright in all visible bands.
    score = score.min(rescale(toa, 'img.B4 + img.B3 + img.B2', [0.2, 0.8]))

    # Clouds are moist
    ndmi = img.normalizedDifference(['B8','B11'])
    score=score.min(rescale(ndmi, 'img', [-0.1, 0.1]))

    # However, clouds are not snow.
    ndsi = img.normalizedDifference(['B3', 'B11'])
    score=score.min(rescale(ndsi, 'img', [0.8, 0.6]))

    # Clip the lower end of the score
    score = score.max(ee.Image(0.001))

    # Remove small regions and clip the upper bound
    dilated = dilated_erosion(score).min(ee.Image(1.0))

    score = score.reduceNeighborhood(
        reducer=ee.Reducer.mean(),
        kernel=ee.Kernel.square(5)
      )

    return img.addBands(score.rename('cloudScore'))


def rescale(img, exp, thresholds):
    return img.expression(exp, {"img": img}) \
              .subtract(thresholds[0]) \
              .divide(thresholds[1] - thresholds[0])


def dilated_erosion(score, dilation_pixels=3, erode_pixels=1.5):
    # Perform opening on the cloud scores
    score = score \
        .reproject('EPSG:4326', None, 20) \
        .focal_min(radius=erode_pixels, kernelType='circle', iterations=3) \
        .focal_max(radius=dilation_pixels, kernelType='circle', iterations=3) \
        .reproject('EPSG:4326', None, 20)
    return score


def clipToROI(x, roi):
    return x.clip(roi).set('ROI', roi)


# Implementation of Basic cloud shadow shift
# Author: Gennadii Donchyts
# License: Apache 2.0
# Modified by Lloyd Hughes to reduce spurious cloud shadow masks
def compute_shadow_score(img: ee.Image, cloud_heights=list(range(200, 10000, 250)), cloud_thresh=0.2,
                         ir_sum_thresh=0.3, ndvi_thresh=-0.1) -> ee.Image:

    mean_azimuth = img.get('MEAN_SOLAR_AZIMUTH_ANGLE')
    mean_zenith = img.get('MEAN_SOLAR_ZENITH_ANGLE')

    cloud_heights = ee.List(cloud_heights)

    cloud_mask = img.select(['cloudScore']).gt(cloud_thresh)

    # Find dark pixels
    dark_pixels_img = img.select(['B8', 'B11', 'B12']) \
        .divide(10000) \
        .reduce(ee.Reducer.sum())

    ndvi = img.normalizedDifference(['B8', 'B4'])
    water_mask = ndvi.lt(ndvi_thresh)

    dark_pixels = dark_pixels_img.lt(ir_sum_thresh)

    # Get the mask of pixels which might be shadows excluding water
    dark_pixel_mask = dark_pixels.And(water_mask.Not())
    dark_pixel_mask = dark_pixel_mask.And(cloud_mask.Not())

    # Find where cloud shadows should be based on solar geometry
    # Convert to radians
    az_rad = ee.Number(mean_azimuth).add(180).multiply(math.pi).divide(180.0)
    zen_rad = ee.Number(mean_zenith).multiply(math.pi).divide(180.0)

    def find_shadows(cloud_height):
        cloud_height = ee.Number(cloud_height)

        shadow_casted_distance = zen_rad.tan().multiply(cloud_height)  # Distance shadow is cast
        x = az_rad.sin().multiply(shadow_casted_distance).multiply(-1)  # .divide(nominalScale)#X distance of shadow
        y = az_rad.cos().multiply(shadow_casted_distance).multiply(-1)  # Y distance of shadow
        # return cloudMask.changeProj(cloudMask.projection(), cloudMask.projection().translate(x, y))
        return img.select(['cloudScore']).displace(ee.Image.constant(x).addBands(ee.Image.constant(y)))

    # Find the shadows
    shadows = cloud_heights.map(find_shadows)

    shadow_masks = ee.ImageCollection.fromImages(shadows)
    shadow_mask = shadow_masks.mean()

    # Create shadow mask
    shadow_mask = dilated_erosion(shadow_mask.multiply(dark_pixel_mask))

    shadow_score = shadow_mask.reduceNeighborhood(
        reducer=ee.Reducer.max(),
        kernel=ee.Kernel.square(1)
    )

    img = img.addBands(shadow_score.rename(['shadowScore']))

    return img


def compute_quality_score(img):

    score = img.select(['cloudScore']).max(img.select(['shadowScore']))
    score = score.reproject('EPSG:4326', None, 20).reduceNeighborhood(
        reducer=ee.Reducer.mean(),
        kernel=ee.Kernel.square(5)
    )
    score = score.multiply(-1)

    return img.addBands(score.rename('cloudShadowScore'))


def cloud_free_mosaic(roi: ee.Geometry, date_range, cloud_free_keep_thresh: float = 5) -> ee.Image:

    collection = ee.ImageCollection('COPERNICUS/S2') \
        .filterDate(date_range.start(), date_range.end()) \
        .filterBounds(roi)

    collection = collection \
        .map(lambda img: img.clip(roi)) \
        .map(lambda img: img.set('ROI', roi)) \
        .map(compute_cloud_score) \
        .map(compute_cloud_coverage) \
        .map(compute_shadow_score) \
        .map(compute_quality_score) \
        .sort('CLOUDY_PERCENTAGE')

    # print(collection.size().getInfo())

    best = collection.filterMetadata('CLOUDY_PERCENTAGE', 'less_than', cloud_free_keep_thresh) \
        .sort('CLOUDY_PERCENTAGE', False)
    filtered = collection.qualityMosaic('cloudShadowScore')

    new_collection = ee.ImageCollection.fromImages([filtered, best.mosaic()])
    cloud_free = ee.Image(new_collection.mosaic()).float()

    return cloud_free



