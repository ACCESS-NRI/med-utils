#!/usr/bin/env python

from .ilamb import add_model_to_tree
import argparse
import yaml
from pathlib import Path

if __name__ == "__main__":

    parser=argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        '--config_file',
    )

    parser.add_argument(
        '--ilamb_root',
    )
    args = parser.parse_args()
    config_file = args.config_file
    ilamb_root = args.ilamb_root

    try:
        Path(f"{ilamb_root}/DATA").symlink_to("/g/data/ct11/access-nri/replicas/ILAMB", target_is_directory=True)
    except :
        pass

    with open(config_file, 'r') as file:
        data = yaml.safe_load(file)

    datasets = data["datasets"]

    for dataset in datasets:
        add_model_to_tree(**dataset)