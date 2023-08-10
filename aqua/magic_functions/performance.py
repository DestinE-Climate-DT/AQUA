from IPython.core.magic import register_line_magic, register_cell_magic, register_line_cell_magic, line_cell_magic

import timeit
import time
import multiprocessing #threading

from aqua.logger import log_configure
loglevel = 'debug'
logger = log_configure(loglevel, 'Mag. Functions')

def data_size(data):
    if 'DataArray' in str(type(data)):
            size = data.size
    elif 'Dataset' in str(type(data)): 
        names = list(data.dims)
        size = 1
        for i in names:
            size *= data[i].size
    return size

@line_cell_magic
@register_line_cell_magic
def estimated_calculation_time(line='', cell=None):
    logger.debug("Line: {}".format(line))
    logger.debug("Cell contents: {}".format(cell))
    if line == "" and cell is None:
            return
    elif cell:
         src = cell
    else:
         src = line
    return function_which_estimates_calculation_time(src)

def function_which_estimates_calculation_time(src):

    parts = src.split('(')

    if len(parts) < 2:
        logger.warning("Invalid input. Please provide a function name followed by arguments.")
        return
    function_name = parts[0].strip()
    arguments_str = parts[1].strip(' )')
    logger.debug("Name of the function: {}".format(function_name))
    logger.debug("String of Function Arguments: {}".format(arguments_str))
    try:
        arguments = [arg.strip() for arg in arguments_str.split(',')]
        #[eval(arg.strip()) for arg in arguments_str.split(',')]
        logger.debug("Arguments of function: {}".format(arguments))
    except Exception as e:
        logger.warning(f"Error parsing arguments: {e}")
        return

    arg_number = len(arguments)
    counter = 0
    for i in range(0, 1): #len(arguments)):
        arg = arguments[i]
        logger.debug("Argument: {}".format(arg))
        logger.debug("Type of Argument: {}".format(type(arg)))
        if True: #'xarray' in str(type(arg)):
            dataset_full = arg
            dataset_part = dataset_full+'.isel(time=0)'
            
            arguments[i] = dataset_part

            result = get_ipython().run_line_magic('timeit',  globals()[function_name](*arguments))
            #result = %timeit  -r 1 -n 1 -o globals()[function_name](*arguments)
            calc_time = result.average
            break
        else:
            counter = counter+1
    if counter == arg_number:
        raise KeyError("Provided function not contains the dataset.")
    ds_full_size = dataset_full.time.size
    expected_calc_time = calc_time * ds_full_size 
    
    print(f"The expecred calculation time is {expected_calc_time} s")
    #return float(expected_calc_time),  (time_units_converter(expected_calc_time,   units))

def target_function_with_progress(src):
    logger.info('target_function_with_progress')

    namespace = {}
    dummy_src_name = "<string>"
    src_copy = """ 
    src 
    """
    #code = compile(src, dummy_src_name, "exec")
    exec(src_copy, globals()) #, namespace, {})

    #res = namespace.get('result')

    #logger.debug("Function result: ", res)

    #return res
    

def function_with_progress_bar():
    total_iterations = 100  # Total number of iterations
    progress_bar_template = "[{:<40}] {}%"

    for i in range(total_iterations):
        # Perform some work
        time.sleep(5/total_iterations)

        # Calculate progress and update the progress bar
        ratio = i / total_iterations
        progress = int(40 * ratio)
        print(progress_bar_template.format("=" * progress, int(ratio * 100)), end="\r")

@line_cell_magic
@register_line_cell_magic
def run_with_progress(line='', cell=None):
    logger.debug("Line: {}".format(line))
    logger.debug("Cell contents: {}".format(cell))
    if line == "" and cell is None:
            return
    elif cell:
         src = cell
    else:
         src = line
    # Create threads for each process
    thread1 = multiprocessing.Process(target=target_function_with_progress, args=(src,)) 
    thread2 = multiprocessing.Process(target=function_with_progress_bar)

    # Start the threads
    thread1.start()
    thread2.start()

    # Wait for threads to finish
    thread1.join()
    thread2.join()







