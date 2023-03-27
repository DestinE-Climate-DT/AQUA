#!/usr/bin/env python

import re
import os 

with os.popen("pwd ") as f:
    _pwd = f.readline()
pwd = re.split(r'[\n]', _pwd)[0]

print(pwd)
writing_started = False

keyword = 'dependencies'

# Read the text file
with open(str(pwd)+'/requirements/conda_requirements.txt', 'r') as f:
    content = f.read()

# Extract the dependencies using regular expressions
dependencies = re.findall(r'^\s*([^\s]+)\s*$', content, flags=re.MULTILINE)

def dependency_splitter(dep):
    return re.split(r'[<=>]', dep)

def dep_splitter(dep):
    try:
        key, value = dep.strip().split('=<>')
    except ValueError:
        key = dep.strip().split('=<>')
        value = -1
    return key, value

def find_the_key(_lines, _keyword):
    i=0
    key_line = -1
    for line in _lines:
        parts =  re.findall(r'[a-zA-Z]+|\W+', line)
        if _keyword in parts:
            key_line=i+1
            break
        i+=1
    return key_line

def find_the_dep(_lines, dep_name):
    i=0
    key_line = -1
    for line in _lines:
        parts = re.split(r'[  <=> \n]', line)
        if dep_name in parts:
            key_line=i
            break
        i+=1
    return key_line



filename = str(pwd)+'/environment.yml'

with open(filename, 'r') as f:
    contents = f.readlines()

    for dependency in dependencies:
        dep_name = dependency_splitter(dependency)[0]
        num_of_line =  find_the_dep(contents, dep_name)
        if  num_of_line>0:
            del contents[num_of_line]
    i_key = find_the_key(contents, keyword)
    for dependency in dependencies:
        new_line = '  - '+str(dependency)+'\n'
        contents.insert(i_key+1, new_line)
        i_key+=1
with open(filename, 'w') as f:
    f.writelines(contents)
