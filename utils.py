import ee


def compute_time_series_metrics(time_series: ee.ImageCollection, bands: list, metrics: list) -> ee.Image:

    time_series_metrics = []
    if 'mean' in metrics:
        time_series_metrics.append(time_series.reduce(ee.Reducer.mean()))
    if 'median' in metrics:
        time_series_metrics.append(time_series.reduce(ee.Reducer.median()))
    if 'max' in metrics:
        time_series_metrics.append(time_series.reduce(ee.Reducer.max()))
    if 'min' in metrics:
        time_series_metrics.append(time_series.reduce(ee.Reducer.min()))
    if 'stdDev' in metrics:
        time_series_metrics.append(time_series.reduce(ee.Reducer.stdDev()))
    if 'iqr' in metrics:
        p25_bands = [f'{band}_p25' for band in bands]
        p75_bands = [f'{band}_p75' for band in bands]
        iqr_bands = [f'{band}_iqr' for band in bands]
        percentiles = time_series.reduce(ee.Reducer.percentile([25, 75]))
        iqr = percentiles.select(p75_bands).subtract(percentiles.select(p25_bands)) \
            .select(p75_bands, iqr_bands)
        time_series_metrics.append(iqr)

    time_series_metrics = ee.Image.cat(time_series_metrics)
    new_order = [f'{band}_{metric}' for band in bands for metric in metrics]
    time_series_metrics = time_series_metrics.select(new_order)

    return time_series_metrics

