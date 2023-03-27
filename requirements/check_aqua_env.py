#!/usr/bin/env python

import re
import os

keyword = 'aqua'
manager = 'conda'

def find_the_dep(_lines, dep_name=keyword):
    for line in _lines:
        parts = re.split(r'[  <=> \n]', line)
        if dep_name in parts:
            return True
            break
    return False

def find_the_key(_lines, keyword=keyword):
    i=0
    key_line = -1
    for line in _lines:
        parts =  re.findall(r'[a-zA-Z]+|\W+', line)
        if keyword in parts:
            key_line=i+1
            break
        i+=1
    return key_line



with os.popen(str(manager)+" env list ") as f:
     env_list_full = f.readlines()

def env_deactivate(keyword=keyword):
    if find_the_key(env_list_full, ' * ')==find_the_key(env_list_full, keyword):
        os.system("source  deactivate " + str(keyword))
        return True

def env_remove(manager = manager, keyword=keyword):  
    os.system(str(manager) + " env remove -n " + str(keyword))
    return True


if find_the_dep(  env_list_full, dep_name=keyword):
    env_deactivate()

if find_the_dep(  env_list_full, dep_name=keyword):
    env_remove()    	

