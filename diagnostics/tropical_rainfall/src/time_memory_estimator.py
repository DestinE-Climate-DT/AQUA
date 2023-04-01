import re
#import tr_pr_mod

def time_estimator(ds_part, calc_time, ds_full):

    calc_time_per_element = calc_time/ds_part.size

    expected_calc_time = calc_time_per_element * ds_full.size 
    
    return float(expected_calc_time)


def timestep_desirable(ds_part, calc_time, wanted_time, ds_full):

    calc_time_per_element = calc_time/ds_part.size

    expected_calc_time = calc_time_per_element * ds_full.size 
    
    return float(expected_calc_time/wanted_time)

def read_VmRSS():
    file_proc = open("/proc/self/status")
    content = file_proc.read()

    lines = re.split(r'[\n]', content)
    for ind in range(0, len(lines)):
        parts = re.findall(r'[a-zA-Z]+|\W+', lines[ind])  
        if 'VmRSS' in parts:
            print(lines[ind])
            break
    return float(re.split(r'[\t :]', lines[ind])[-2])

def read_MemAvail():
    file_proc = open("/proc/meminfo")
    # Reading from file
    content = file_proc.read()
    lines = re.split(r'[\n]', content)
    for ind in range(0, len(lines)):
        parts = re.findall(r'[a-zA-Z]+|\W+', lines[ind])
        if 'MemAvailable' in parts:
            break
    print(lines[ind]) 
    return float(re.split(r'[\t ]', lines[ind])[-2])

def mem_estimator(ds_part,  ds_full, VmRSS_1):

    #VmRSS_1 = read_VmRSS()
    
    """ TEST PART   TEST PART   TEST PART   TEST PART   TEST PART """ 
 
    """ TEST PART   TEST PART   TEST PART   TEST PART   TEST PART """ 
    
    
    VmRSS_2 = read_VmRSS()
 
    Mem_Consumed = VmRSS_2 - VmRSS_1
    Mem_Consumed_by_Single_Object = Mem_Consumed/ds_part.size


    return Mem_Consumed_by_Single_Object * ds_full.size

def size_estimator(ds_part,  ds_full, VmRSS_1, Mem_Perc_Max = 0.5):
 

    #VmRSS_1 = read_VmRSS()

    """ TEST PART   TEST PART   TEST PART   TEST PART   TEST PART """ 

    
    """ TEST PART   TEST PART   TEST PART   TEST PART   TEST PART """ 
    VmRSS_2 = read_VmRSS()

    Mem_Avail = read_MemAvail()
    Mem_Avail = Mem_Perc_Max * Mem_Avail
 
    Mem_Consumed = VmRSS_2 - VmRSS_1
    Mem_Consumed_by_Single_Object = Mem_Consumed/ds_part.size

    Mem_Estimation = Mem_Consumed_by_Single_Object * ds_full.size

    Fittable_Size = Mem_Avail/Mem_Consumed_by_Single_Object 

    return Mem_Estimation, Mem_Estimation/Fittable_Size