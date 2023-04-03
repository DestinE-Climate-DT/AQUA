import re
from statistics import mean

def data_size(data):
    if 'DataArray' in str(type(data)):
            _size = data.size
    elif 'Dataset' in str(type(data)): 
        _names = list(data.dims) #_coord_names)
        _size = 1
        for i in _names:
            _size *= data[i].size
    return _size





def read_VmRSS():
    file_proc = open("/proc/self/status")
    content = file_proc.read()

    lines = re.split(r'[\n]', content)
    for ind in range(0, len(lines)):
        parts = re.findall(r'[a-zA-Z]+|\W+', lines[ind])  
        if 'VmRSS' in parts:
            print(lines[ind])
            break
    Vm = float(re.split(r'[\t :]', lines[ind])[-2])
    unit = re.split(r'[\t :]', lines[ind])[-1]
    for ind in range(0, len(lines)):
        parts = re.findall(r'[a-zA-Z]+|\W+', lines[ind])  
        if 'Pid' in parts:
            #print(lines[ind])
            break
    #Pid =  float(re.split(r'[\t :]', lines[ind])[-1]) 
    #print('Pid: ', float(re.split(r'[\t :]', lines[ind])[-1]) )
    file_proc.close()
    return Vm, unit


def read_VmRSS_av(iter_max = 5):
    Vm_tab =[]
    Pid = []
    for ind in range(0, iter_max):
        file_proc = open("/proc/self/status")
        content = file_proc.read()

        lines = re.split(r'[\n]', content)
        for ind in range(0, len(lines)):
            parts = re.findall(r'[a-zA-Z]+|\W+', lines[ind])  
            if 'VmRSS' in parts:
                #print(lines[ind])
                break
        Vm_tab.append(float(re.split(r'[\t :]', lines[ind])[-2]))
        
        unit = re.split(r'[\t :]', lines[ind])[-1]
        for ind in range(0, len(lines)):
            parts = re.findall(r'[a-zA-Z]+|\W+', lines[ind])  
            if 'Pid' in parts:
                #print(lines[ind])
                break
        #Pid.append(float(re.split(r'[\t :]', lines[ind])[-1]) )
        #print('Pid: ', float(re.split(r'[\t :]', lines[ind])[-1]) )
        file_proc.close()
    return mean(Vm_tab), unit 

def read_MemAvail():
    file_proc = open("/proc/meminfo")
    # Reading from file
    content = file_proc.read()
    lines = re.split(r'[\n]', content)
    for ind in range(0, len(lines)):
        parts = re.findall(r'[a-zA-Z]+|\W+', lines[ind])
        if 'MemAvailable' in parts:
            break
    file_proc.close()
    print(lines[ind]) 
    return float(re.split(r'[\t ]', lines[ind])[-2]), re.split(r'[\t :]', lines[ind])[-1]



def expected_total_memory_usage(ds_part,  ds_full, VmRSS_1):

    #VmRSS_1 = read_VmRSS()
    
    """ TEST PART   TEST PART   TEST PART   TEST PART   TEST PART """ 
 
    """ TEST PART   TEST PART   TEST PART   TEST PART   TEST PART """ 
    
    ds_part_size = data_size(ds_part)
    ds_full_size = data_size(ds_full)
    VmRSS_2, mem_units = read_VmRSS_av()
 
    Mem_Consumed = VmRSS_2 - VmRSS_1
    
    Mem_Consumed_by_Single_Object = Mem_Consumed/ds_part_size
    #Mem_Consumed_by_Single_Object = max(Mem_Consumed, 8/1024)

    print(Mem_Consumed_by_Single_Object)


    return Mem_Consumed_by_Single_Object * ds_full_size, mem_units 

def adaptive_data_load(ds_part,  ds_full, VmRSS_1, Mem_Perc_Max = 0.5):
 

    #VmRSS_1 = read_VmRSS()

    """ TEST PART   TEST PART   TEST PART   TEST PART   TEST PART """ 

    
    """ TEST PART   TEST PART   TEST PART   TEST PART   TEST PART """ 
    VmRSS_2, Vm_units  = read_VmRSS_av()

    ds_part_size = data_size(ds_part)
    ds_full_size = data_size(ds_full)

    Mem_Avail, Mem_Av_units  = read_MemAvail()
    Mem_Avail = Mem_Perc_Max * Mem_Avail
 
    Mem_Consumed = VmRSS_2 - VmRSS_1
    Mem_Consumed_by_Single_Object = Mem_Consumed/ds_part_size

    Mem_Estimation = Mem_Consumed_by_Single_Object * ds_full_size

    Fittable_Size = min(Mem_Avail/Mem_Consumed_by_Single_Object, ds_full_size)

    if Vm_units == Mem_Av_units:
        mem_units = Vm_units
    else:
        return 'unknown type'
        
    return Mem_Estimation, Fittable_Size/ds_full_size, mem_units

def mem_units_converter(old_mem_unit, old_value, desirable_mem_unit):
    if old_mem_unit == 'kB':
        if desirable_mem_unit == 'MB':
            return old_value / (1024)
        if desirable_mem_unit == 'GB':
            return old_value / (1024)**2
        if desirable_mem_unit == 'TB':
            return old_value / (1024)**3
        if desirable_mem_unit == 'PB':
            return old_value / (1024)**4
    else:
        return 'unknown type'