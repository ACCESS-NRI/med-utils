# Source Generated with Decompyle++
# File: parse_config_var.cpython-39.pyc (Python 3.9)

import os
import time
import gc
import pandas as pd
import csv
import glob
import xarray as xr
import cdms2
import datetime
import numpy as np
import multiprocessing as mp
from multiprocessing import Pool

os.environ['ANCILLARY_FILES'] = '/g/data/p66/CMIP6/APP_ancils'
from .app_functions import *

#GLOBAL
UM_realms = [
    'atmos',
    'land',
    'aerosol',
    'atmosChem',
    'landIce']
MOM_realms = [
    'ocean',
    'ocnBgchem']
CICE_realms = [
    'seaIce']


def Parse_config_var(var_dict, master_filename):
    '''
    Extract variables based on the config file and extract mapping imformation from master_map.csv

    Parameters:
    ------------
    config_filename : str
        path of config file which we used in ilamb
    master_filename: str
        path of master_map file

    Returns:
    ------------
    A dict with variable name as key and variable imformation as value
    '''
    map_dic={}
    result={}

    with open(master_filename,'r') as g:
        champ_reader = csv.reader(g, delimiter=',')
        for raw in champ_reader:
            if not raw[0].startswith('#') and raw[0] not in map_dic.keys():
                map_dic[raw[0]]=raw[1:]

    for value in var_dict.values():
        for var in value:
            if var not in result.keys() and var in map_dic.keys():
                    result[var]=map_dic[var]
    return result


def get_filestructure(master_line, history_path):
    '''
    Find path to each variable

    Parmeters:
    -------------    
    master_line : list[]
        Variables information 
    history_path : str
        Path of noncmip dataset
    
    Returns:
    conplete noncmip file path
    '''
    cmipvar = master_line[0]
    realm = master_line[8]
    access_version = master_line[7]
    access_vars = master_line[2]
    if os.path.exists(history_path + '/atm/netCDF/link/'):
        atm_file_struc = '/atm/netCDF/link/'
    else:
        atm_file_struc = '/atm/netCDF/'
    if realm in UM_realms or access_version == 'both':
        if cmipvar in ('tasmax', 'tasmin'):
            file_structure = atm_file_struc + '*_dai.nc'
        else:
            file_structure = atm_file_struc + '*_mon.nc'
    elif realm == 'ocean':
        file_structure = '/ocn/ocean_month.nc-*'
    elif realm == 'ocnBgchem':
        if access_version == 'OM2':
            file_structure = '/ocn/ocean_bgc_mth.nc-*'
        elif access_version in ('CM2', 'OM2-025'):
            file_structure = None
        elif access_vars.find('_raw') != -1:
            file_structure = '/ocn/ocean_bgc_mth.nc-*'
        else:
            file_structure = '/ocn/ocean_bgc.nc-*'
    elif realm in CICE_realms:
        if access_version == 'CM2':
            file_structure = '/ice/iceh_m.????-??.nc'
        elif access_version == 'ESM':
            file_structure = '/ice/iceh.????-??.nc'
        elif access_version.find('OM2') != -1:
            file_structure = '/ice/iceh.????-??.nc'
        else:
            file_structure = None
    else:
        file_structure = None
    return file_structure


def create_result_dict(var_mapping_dic, history_path):
    """
    Create a temp dictionary which contain variable informations

    Parameters:
    ------------
    var_mapping_dic : dict{}
        A dictionary contain informations from master_map.csv
    history_path : str
        path to history data

    Return:
    -------------
    dictionary which contain variable informations

    """
    result_dict={}
    for key in var_mapping_dic.keys():
        temp_list = [key]+var_mapping_dic[key]
        file_structure = get_filestructure(temp_list, history_path)
        result_dict[temp_list[0]] = temp_list[1:]
        result_dict[temp_list[0]].append(file_structure)
    return result_dict


def create_structure_dict(result_dict):
    """
    Create a temp dictionary which contain variable informations

    Parameters:
    ------------
    result_dict : dict{}
        A dictionary contain variable informations

    Return:
    -------------
    dictionary which key is the file structure to a specific variable

    """
    structure_dict = {}
    for item in result_dict.keys():
        if result_dict[item][-1] not in structure_dict:
            structure_dict[result_dict[item][-1]] = [item]
            continue
        structure_dict[result_dict[item][-1]].append(item)
    return structure_dict


def generate_cmip(noncmip_path, new_nc_path,mip_vars_dict):
    '''
    Main function, trigger the whole mapping process

    Parameters:
    ------------
    noncmip_path : str
        path to noncmip dataset file
    new_nc_path: str
        path to save the new CMIP format dile
    config_path: str
        path to ilamb config file
    '''
    history_path = noncmip_path + '/history/'
    master_map_path='./master_map.csv'
    var_mapping_dic = Parse_config_var(mip_vars_dict, master_map_path)
    result_dict = create_result_dict(var_mapping_dic, history_path)
    structure_dict = create_structure_dict(result_dict)

    new_netcdf(history_path, structure_dict, result_dict, new_nc_path)



def mp_newdataset(file_varset):
    '''
    mapping noncmip data to CMIP format

    Parameters: 
    -------------
    file : str
        file name of each noncmip dataset file
    
    Return:
    -------------
    xarray under CMIP format
    '''    
    file=file_varset[0]
    var_set=file_varset[1]
    var_info_dict=file_varset[2]
    ds_dict={}
    ds=xr.open_dataset(file)

    def addtb(time):
        temp_t=str(time)
        t0=datetime.datetime(int(temp_t[:4]),int(temp_t[5:7]),1)
        if int(temp_t[5:7])==12:
            t1=datetime.datetime(int(temp_t[:4])+1,1,1)
        else:
            t1=datetime.datetime(int(temp_t[:4]),int(temp_t[5:7])+1,1)
        return np.asarray([np.datetime64(t0),np.datetime64(t1)])

    def addlatb(lats):
        gap=(lats[1]-lats[0])/2
        lat_bnds=[]
        for lat in lats:
            lat_bnds.append([lat-gap,lat+gap])
        return np.asarray(lat_bnds)

    def addlonb(lons):
        gap=(lons[1]-lons[0])/2
        lon_bnds=[]
        for lon in lons:
            lon_bnds.append([lon-gap,lon+gap])
        return np.asarray(lon_bnds)
    
    time_bnds=addtb(ds.time[0].data)
    lat_bnds=addlatb(ds.lat.data)
    lon_bnds=addlonb(ds.lon.data)

    for var_name in var_set:
        
        temp_list=var_info_dict[var_name]

        if len(temp_list[1].split())>=1 and temp_list[2]!='':
            var=[]
            ds_1=cdms2.open(file,'r')
            for var_num in temp_list[1].split():
                var.append(ds_1[var_num])
        
            if temp_list[2].find('times')!=-1:
                times = ds_1[temp_list[1].split()[0]].getTime()
            if temp_list[2].find('depth')!=-1:
                depth = ds[temp_list[1].split()[0]].getAxis(1)
            if temp_list[2].find('lat')!=-1:
                lat = ds[temp_list[1].split()[0]].getLatitude()
            if temp_list[2].find('lon')!=-1:
                lon = ds[temp_list[1].split()[0]].getLatitude()
            

            if temp_list[2]!=None:
                var_data=eval(temp_list[2])

            ds_1.close()
        else:
            var_data=ds[temp_list[1].strip()]

        if isinstance(var_data, xr.core.dataarray.DataArray):
            temp_ds=xr.Dataset(
                data_vars=dict(
                key=var_data,
                time_bnds=(['bnds'],time_bnds),
                lat_bnds=(['lat','bnds'],lat_bnds),
                lon_bnds=(['lon','bnds'],lon_bnds),
                ),
                coords=dict(
                time=ds.coords['time'],
                lat=ds.coords['lat'],
                lon=ds.coords['lon'],
                ),
            )                    
        else:
            if len(ds.coords['time'])>1:
                coords_time=ds.coords['time'][0]
            else:
                coords_time=ds.coords['time']
           
            if var_name=='nbp':
                var_data=var_data.asma()
            try:    
                temp_ds=xr.Dataset(
                    data_vars=dict(
                    key=(['time','lat','lon'],var_data),
                    time_bnds=(['bnds'],time_bnds),
                    lat_bnds=(['lat','bnds'],lat_bnds),
                    lon_bnds=(['lon','bnds'],lon_bnds),
                    ),
                    coords=dict(
                    time=np.atleast_1d(coords_time),
                    lat=ds.coords['lat'],
                    lon=ds.coords['lon'],
                    ),
                )
            
            except Exception as e:
                raise e
    
        temp_ds=temp_ds.rename({'key':var_name})
        temp_ds[var_name].attrs['units']=temp_list[3]
        temp_ds['time'].attrs=ds.time.attrs
        temp_ds['lat'].attrs=ds.lat.attrs
        temp_ds['lon'].attrs=ds.lon.attrs

        if var_name=='tsl':
            depth_val,depth_bounds = cableSoilLevels()
            temp_ds=temp_ds.rename({'soil_model_level_number':'depth'})
            temp_ds=temp_ds.assign_coords(depth=depth_val)
            temp_ds=temp_ds.assign(depth_bnds=(['depth','bnds'],depth_bounds))
        
        if var in ['rsus', 'tasmax', 'tasmin', 'cVeg', 'rlus', 'lai', 'nbp', 'cSoil']:
            temp_ds.time.encoding['units']='days since 1850-1-1 00:00:00' 
        
        if var_name not in ds_dict.keys():
            ds_dict[var_name]=temp_ds

        ds.close()

    return ds_dict


def multi_combine(dataset_list):
    '''
    Combine dataset by time and write as netCDF file

    Parameters:
    ------------
    dataset_list : list[]
        A list contain all the dataet (timerange:1850-2014) of one variable
    '''
    var=dataset_list[0]
    ds_list=dataset_list[1]
    new_nc_path=dataset_list[2]
    dataset=xr.combine_by_coords(ds_list)
    dataset['lat_bnds']=dataset['lat_bnds'][0]
    dataset['lon_bnds']=dataset['lon_bnds'][0]
    if os.path.isfile(new_nc_path+'/'+var+'.nc'):
        os.remove(new_nc_path+'/'+var+'.nc')
    dataset.to_netcdf(new_nc_path+'/'+var+'.nc')
    del dataset
    gc.collect()


def pool_process(func, process_list):
    """
    run a function in a multiprocess pool

    Parameters:
    ------------
    func : function
        a function to input to multiprocess pool
    process_list : list[]
        a list which contains input variables to func 

    Return:
    -------------
        a list contain returns fron func
    """
    num_cpu=mp.cpu_count()
    if num_cpu == 1:
        pool=Pool(int(1))
    else:
        pool=Pool(int(num_cpu/2))
    result=pool.map(func,process_list)
    pool.close()
    pool.join()
    return result


def get_variable_from_file(s_dic, non_cmip_path, new_nc_path, var_info_dict):
    """
    Load non-cmip data from file and save into a dict

    Parameters:
    ------------
        s_dic : dict{}
            directory contain variables pair with DRS
        non_cmip_path : str
            path to noncmip-history directory
        new_nc_path : str
            path to write the cmorised data 

    Return:
    -------------
        A list of all the cmorised data

    """
    results=[]
    var_sets=[]
    dataset_list=[]
    file_set=[]

    for path in s_dic.keys():
        file_set+=[[f, s_dic[path], var_info_dict] for f in glob.glob(non_cmip_path+path)]
        var_sets+=s_dic[path]
    results = pool_process(mp_newdataset, file_set)
    
    for var in var_sets:
        temp_list=[f[var] for f in results if var in f.keys()]
        dataset_list.append([var,temp_list,new_nc_path])

    return dataset_list


def write_cmorised_data(dataset_list):
    """
    Combine all the cmorised data of each variable and write to a new netCDF file

    Parameters:
    ------------
        dataset_list : list[]
            A list of all the cmorised data
    """
    pool_process(multi_combine, dataset_list)


def new_netcdf(non_cmip_path,s_dic,var_dict_out,new_nc_path):
    '''
    parameters:
    ----------
    non_cmip_path : str
        path to noncmip-history directory
    s_dic : dict{}
        key is the variables name and value is the DRS of noncmip dataset file from noncmip-history directory
    var_dict_out : dict{}
        key is variables name and value is variables information.
    new_nc_path : str
        path to save the new NETcdf file
    '''
    time_start=time.time()

    dataset_list = get_variable_from_file(s_dic, non_cmip_path, new_nc_path, var_dict_out)
    if not os.path.isdir(new_nc_path):
        os.makedirs(new_nc_path)
    write_cmorised_data(dataset_list)

    time_end=time.time()
    time_cost=time_end-time_start
    print('total_time_cost:',time_cost)
