from pathlib import Path
import glob
import os
import xarray
import yaml
from .utilities import MyParser

rootpath = {
    "CMIP6": ["/g/data/fs38/publications/CMIP6", "/g/data/oi10/replicas/CMIP6"],
    "CMIP5": ["/g/data/r87/DRSv3/CMIP5", "/g/data/al33/replicas/CMIP5/combined", "/g/data/rr3/publications/CMIP5/output1"]
}

mip_vars ={
    'Emon':['cSoil'],
    'Lmon':['cVeg','gpp','lai','nbp','ra','rh','tsl'],
    'Amon':['evspsbl','hfls','hfss','hurs','pr','rlds','rlus','rsds','rsus','tasmax','tasmin','tas'],
    'Omon':['hfds'],
    }

def get_CMIP6_path(institute = "*", dataset = "*", exp = "*", ensemble = "*", mip="*", version="**", var="*"):
    return f"CMIP/{institute}/{dataset}/{exp}/{ensemble}/{mip}/{var}/**/{version}/*.nc"

def get_CMIP5_path(institute = "*", dataset = "*", exp = "*", ensemble = "*", mip="*", version="**", var="*"):
    return f"{institute}/{dataset}/{exp}/mon/*/{mip}/{ensemble}/*/{var}/*.nc"


get_path_function = {
    "CMIP6": get_CMIP6_path,
    "CMIP5": get_CMIP5_path
}

def add_model_to_tree(ilamb_root, institute, dataset, project, exp, ensemble):
    """
    """
    print(f"Adding {dataset} to the ILAMB Tree")
    model_root = f"{ilamb_root}/MODELS/{dataset}/{exp}/{ensemble}"
    Path(model_root).mkdir(parents=True, exist_ok=True)

    for mip, vars in mip_vars.items():
        for var in vars:
            for path in rootpath[project]:
                search_path = os.path.join(path, get_path_function[project](
                    institute=institute, 
                    dataset=dataset, 
                    exp=exp, 
                    ensemble=ensemble, 
                    mip=mip,
                    var=var))
                files = glob.glob(search_path)
                if not files:
                    continue
                
                unique_files = []
                for file in files:
                    filenames = [Path(path).stem for path in unique_files]
                    if Path(file).stem not in filenames:
                        unique_files.append(file)
                files = unique_files

                if len(files) > 1:
                    with xarray.open_mfdataset(files, use_cftime=True, combine_attrs='drop_conflicts') as f:
                        f.to_netcdf(f"{model_root}/{var}.nc")
                else:
                    try:
                        Path(f"{model_root}/{var}.nc").unlink()
                    except:
                        pass
                    Path(f"{model_root}/{var}.nc").symlink_to(f"{files[0]}")

    return


def tree_generator():

    parser=MyParser(description="Generate an ILAMB-ROOT tree")

    parser.add_argument(
        '--datasets',
        default=False,
        nargs="+",
        help="YAML file specifying the model output(s) to add.",
    )

    parser.add_argument(
        '--ilamb_root',
        default=False,
        nargs="+",
        help="Path of the ILAMB-ROOT",
    )
    args = parser.parse_args()
    dataset_file = args.datasets[0]
    ilamb_root = args.ilamb_root[0]

    Path(ilamb_root).mkdir(parents=True, exist_ok=True)
    try:
        Path(f"{ilamb_root}/DATA").unlink()
    except :
        pass
    Path(f"{ilamb_root}/DATA").symlink_to("/g/data/ct11/access-nri/replicas/ILAMB", target_is_directory=True)

    with open(dataset_file, 'r') as file:
        data = yaml.safe_load(file)

    datasets = data["datasets"]

    for dataset in datasets:
        add_model_to_tree(**dataset, ilamb_root=ilamb_root)
    
    return
