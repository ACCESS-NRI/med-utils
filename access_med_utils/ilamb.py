from pathlib import Path
import glob
import os
import xarray
import yaml
from .utilities import MyParser
from .CMORise import generate_cmip

rootpath = {
    "CMIP6": ["/g/data/fs38/publications/CMIP6", "/g/data/oi10/replicas/CMIP6"],
    "CMIP5": ["/g/data/r87/", "/g/data/al33/", "/g/data/rr3/"],
    "non-CMIP": ["/g/data/p73/archive/non-CMIP"]
}

mip_vars ={
    'Emon':['cSoil'],
    'Lmon':['cVeg','gpp','lai','nbp','ra','rh','tsl'],
    'Amon':['evspsbl','hfls','hfss','hurs','pr','rlds','rlus','rsds','rsus','tasmax','tasmin','tas'],
    'Omon':['hfds'],
    }

def get_CMIP6_path(mip="*", institute = "*", dataset = "*", exp = "*", ensemble = "*", frequency="*", version="**", var="*"):
    return f"{mip}/{institute}/{dataset}/{exp}/{ensemble}/{frequency}/{var}/**/{version}/*.nc"

def get_CMIP5_path(group="*", mip="*", institute = "*", dataset = "*", exp = "*", ensemble = "*", frequency="*", version="**", var="*"):
    if group=="r87":
        return f"{group}/DRSv3/{mip}/{institute}/{dataset}/{exp}/mon/*/{frequency}/{ensemble}/*/{var}/*.nc"
    if group=="al33":
        return f"{group}/replicas/{mip}/combined/{institute}/{dataset}/{exp}/mon/*/{frequency}/{ensemble}/*/{var}/*.nc"
    if group=="rr3":
        return f"{group}/publications/{mip}/ouput1/{institute}/{dataset}/{exp}/mon/*/{frequency}/{ensemble}/*/{var}/*.nc"


get_path_function = {
    "CMIP6": get_CMIP6_path,
    "CMIP5": get_CMIP5_path
}

def add_model_to_tree(ilamb_root, mip, institute, dataset, project, exp, ensemble = None, path = None):
    """
    """

    if mip == 'non-CMIP':
        print(f"CMORisering {exp} and add result to ILAMB Tree")
        if path == None:
            path = rootpath['non-CMIP'][0]

        noncmip_path=f"{path}/{dataset}/{exp}"
        model_root=f"{ilamb_root}/MODELS/{dataset}/{exp}"
        Path(model_root).mkdir(parents=True,exist_ok=True)
        mip_vars.pop('Omon')
        generate_cmip(noncmip_path,model_root,mip_vars)
    
    else:
        print(f"Adding {dataset} to the ILAMB Tree")
        model_root = f"{ilamb_root}/MODELS/{dataset}/{exp}/{ensemble}"
        Path(model_root).mkdir(parents=True, exist_ok=True)

    

    
        for frequency, vars in mip_vars.items():
            for var in vars:
                for path in rootpath[project]:
                    if project=='CMIP5':
                        search_path = os.path.join(path, get_path_function[project](
                            group=path.split('/')[-2],
                            mip=mip,
                            institute=institute, 
                            dataset=dataset, 
                            exp=exp, 
                            ensemble=ensemble, 
                            frequency=frequency,
                        var=var))
                    else:
                        search_path = os.path.join(path, get_path_function[project](
                            mip=mip,
                            institute=institute, 
                            dataset=dataset, 
                            exp=exp, 
                            ensemble=ensemble, 
                            frequency=frequency,
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


if __name__=='__main__':
    tree_generator()
    
