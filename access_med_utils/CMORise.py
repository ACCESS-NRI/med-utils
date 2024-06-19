# Source Generated with Decompyle++
# File: parse_config_var.cpython-39.pyc (Python 3.9)

import os
import time
import re
import gc
import pandas as pd
import csv
import fnmatch
import glob
import dask
import xarray as xr
import sys
import cdms2
from netCDF4 import Dataset
import datetime
import numpy as np
import multiprocessing as mp
from multiprocessing import Pool
sys.path.append('/g/data/kj13/users/yz9299/app4/APP4/subroutines')
sys.path.append('./')
os.environ['ANCILLARY_FILES'] = '/g/data/p66/CMIP6/APP_ancils'
from app_functions import *
# from non_cmip_dataset import *

################################################################################################
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

CONFIG_FILENAME = '/g/data/kj13/users/yz9299/ilamb_test/CMIP6.cfg'
MASTER_FILENAME = '/g/data/kj13/users/yz9299/app4/APP4/input_files/master_map.csv'
ANCILLARY_FILENAME = '/g/data/p66/CMIP6/APP_ancils'


var_set=[]
var_dict={}

# special_var=['rsus','tasmax','tasmin']
################################################################################################

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

    # for line in open(config_filename).readlines():
    #     line = line.strip()
    #     if line.startswith("#"): continue
    #     line = line[:line.index("#")] if "#" in line else line

    #     m=re.search(r"(.*)=(.*)",line)

    #     if m and m.group(1).strip() in ['variable','alternate_vars','derived','ctype']:
    #         # print(m.group(2).strip())
    #         if m.group(1).strip()=='ctype' and m.group(2).strip().replace('"','')=='ConfSoilCarbon':
    #             # print(m.group(1).strip(),m.group(2).strip())
    #             var_list=['ra','gpp','tas','pr']
    #         elif m.group(1).strip()=='derived':
    #             var_list=re.compile('\w+').findall(m.group(2).strip())
    #         else:
    #             var_list=[m.group(2).strip().replace('"','')]
    #         # print(var_list)
    #         for var in var_list:
    #             if var not in result.keys() and var in map_dic.keys():
    #                 result[var]=map_dic[var]
    for value in var_dict.values():
        for var in value:
            if var not in result.keys() and var in map_dic.keys():
                    result[var]=map_dic[var]
    return result


def get_filestructure(master_line, history_path):
    '''
    Find DRS of each variable

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
    result_dict = {}
    for key in var_mapping_dic.keys():
        temp_list = [key]+var_mapping_dic[key]
        file_structure = get_filestructure(temp_list, history_path)
        result_dict[temp_list[0]] = temp_list[1:]
        result_dict[temp_list[0]].append(file_structure)

    structure_dict = {}
    for item in result_dict.keys():
        if result_dict[item][-1] not in structure_dict:
            structure_dict[result_dict[item][-1]] = [item]
            continue
        structure_dict[result_dict[item][-1]].append(item)
    print('structure_dict:',structure_dict)
    print('history_path:',history_path)
    print('result_dict:',result_dict)
    new_netcdf(history_path, structure_dict, result_dict, new_nc_path)



def mp_newdataset(file):
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
        
    global var_set
    global special_var
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
        
        temp_list=var_dict[var_name]

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
            
            # if temp_list[2].find('times')!=-1:
            #     times = access_file[0][varNames[0]].getTime()
            #     print('times:',times)
            # if temp_list[2].find('depth')!=-1:
            #     depth = access_file[0][varNames[0]].getAxis(1)
            # if temp_list[2].find('lat')!=-1:
            #     lat = access_file[0][varNames[0]].getLatitude()
            # if temp_list[2].find('lon')!=-1:
            #     lon = access_file[0][varNames[0]].getLatitude()
            if temp_list[2]!=None:
                var_data=eval(temp_list[2])

            ds_1.close()
        else:
            var_data=ds[temp_list[1].strip()]

        if str(type(var_data))=="<class 'xarray.core.dataarray.DataArray'>":
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
            # print('var_data:',var_data)  
            print('var_name:',var_name)
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
        
        if var_name not in ds_dict.keys():
            # ds_dict[var_name]=pickle.dumps(temp_ds,protocol=-1)#change
            ds_dict[var_name]=temp_ds
        else:
            print('Wromg!!!')
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

    global special_var

    var=dataset_list[0]
    ds_list=dataset_list[1]
    new_nc_path=dataset_list[2]
    dataset=xr.combine_by_coords(ds_list)
    dataset['lat_bnds']=dataset['lat_bnds'][0]
    dataset['lon_bnds']=dataset['lon_bnds'][0]
    print(dataset)
    if os.path.isfile(new_nc_path+'/'+var+'.nc'):
        os.remove(new_nc_path+'/'+var+'.nc')
    dataset.to_netcdf(new_nc_path+'/'+var+'.nc')
    del dataset
    gc.collect()
    if var =='rsus' or var =='tasmax' or var =='tasmin' or var=='cVeg' or var=='rlus' or var=='lai' or var=='nbp' or var=='cSoil':
        dset=Dataset(new_nc_path+'/'+var+'.nc',mode='a')
        dset.variables['time'].units='days since 1850-1-1 00:00:00'   
        dset.close()


def new_netcdf(non_cmip_path,s_dic,var_dict_out,new_nc_path):
    '''
    parameters:
    ----------
    non+cmip_path : str
        path to noncmip-history directory
    s_dic : dict{}
        key is the variables name and value is the DRS of noncmip dataset file from noncmip-history directory
    var_dict_out : dict{}
        key is variables name and value is variables information.
    new_nc_path : str
        path to save the new NETcdf file
    '''

    global var_dict
    var_dict=var_dict_out
    results=[]
    var_sets=[]
    dataset_list=[]

    print('------------------------------------------------------------------------------------------------------')
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')

    t0=time.time()
    
    #Calculate and map variables 
    for path in s_dic.keys():

        file_set=[f for f in glob.glob(non_cmip_path+path)]

        global var_set
        var_set=s_dic[path]
        var_sets.append(var_set)
        num_cpu=mp.cpu_count()
        pool=Pool(int(num_cpu/2))
        result=pool.map(mp_newdataset,file_set)
        pool.close()
        pool.join()
        results.append(result)
        

    print('calcu_time:',time.time()-t0)
    print('results:',results)

    i=0
    for result in results:
        var_set=var_sets[i]
        for var in var_set:
            temp_list=[f[var] for f in result]
            print(len(temp_list),var)
            dataset_list.append([var,temp_list,new_nc_path])
        i+=1

    if not os.path.isdir(new_nc_path):
        os.makedirs(new_nc_path)

    tt0=time.time()
    pool=Pool(int(num_cpu/2))
    pool.map(multi_combine,dataset_list)
    tt1=time.time()
    print(tt1-tt0)
    pool.close()
    pool.join()

    t1=time.time()
    dt1=t1-t0
    print('total_time_cost:',dt1)
