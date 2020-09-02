import ee

from download_manager import args
from download_manager.config import config

import export
import utils


def setup(args):
    cfg = config.new_config()
    cfg.merge_from_file(f'configs/{args.config_file}.yaml')
    cfg.merge_from_list(args.opts)
    cfg.NAME = args.config_file
    return cfg


if __name__ == '__main__':
    # setting up config based on parsed argument
    parser = args.argument_parser()
    args = parser.parse_known_args()[0]
    cfg = setup(args)

    ee.Initialize()

    bbox = utils.extract_bbox(cfg)
    bbox = ee.FeatureCollection([ee.Feature(bbox)])

    task = export.table_to_drive(fc=bbox, folder=cfg.DOWNLOAD.DRIVE_FOLDER,
                                 file_name=f'bbox_{cfg.ROI.ID}')
    task.start()


