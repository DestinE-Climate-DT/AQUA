from IPython.core.magic import register_line_magic, register_cell_magic, register_line_cell_magic, line_cell_magic
from IPython.testing.skipdoctest import skip_doctest
from IPython.core.magic import (
    Magics,
    cell_magic,
    line_cell_magic,
    line_magic,
    magics_class,
    needs_local_scope,
    no_var_expand,
    on_off,
    output_can_be_silenced,
)

from IPython.utils.timing import clock, clock2

import ast
import gc
import sys
import itertools

from typing import Dict, Any
import timeit
import time
import math
import multiprocessing #threading

from aqua.logger import log_configure
loglevel = 'debug'
logger = log_configure(loglevel, 'Mag. Functions')




class TimeitTemplateFiller(ast.NodeTransformer):
    """Fill in the AST template for timing execution.

    This is quite closely tied to the template definition, which is in
    :meth:`ExecutionMagics.timeit`.
    """
    def __init__(self, ast_setup, ast_stmt):
        self.ast_setup = ast_setup
        self.ast_stmt = ast_stmt

    def visit_FunctionDef(self, node):
        "Fill in the setup statement"
        self.generic_visit(node)
        if node.name == "inner":
            node.body[:1] = self.ast_setup.body

        return node

    def visit_For(self, node):
        "Fill in the statement to be timed"
        if getattr(getattr(node.body[0], 'value', None), 'id', None) == 'stmt':
            node.body = self.ast_stmt.body
        return node

class Timer(timeit.Timer):
    """Timer class that explicitly uses self.inner
    
    which is an undocumented implementation detail of CPython,
    not shared by PyPy.
    """
    # Timer.timeit copied from CPython 3.4.2
    def timeit(self, number=timeit.default_number):
        """Time 'number' executions of the main statement.

        To be precise, this executes the setup statement once, and
        then returns the time it takes to execute the main statement
        a number of times, as a float measured in seconds.  The
        argument is the number of times through the loop, defaulting
        to one million.  The main statement, the setup statement and
        the timer function to be used are passed to the constructor.
        """
        it = itertools.repeat(None, number)
        gcold = gc.isenabled()
        gc.disable()
        try:
            timing = self.inner(it, self.timer)
        finally:
            if gcold:
                gc.enable()
        return timing
        
class TimeitResult(object):
    """
    Object returned by the timeit magic with info about the run.

    Contains the following attributes :

    loops: (int) number of loops done per measurement
    repeat: (int) number of times the measurement has been repeated
    best: (float) best execution time / number
    all_runs: (list of float) execution time of each run (in s)
    compile_time: (float) time of statement compilation (s)

    """
    def __init__(self, loops, repeat, all_runs, compile_time, precision):
        self.all_runs = all_runs
        self.compile_time = compile_time
        self._precision = precision
        self.loops = loops
        self.timings = [ dt / self.loops for dt in all_runs]

    @property
    def average(self):
        return math.fsum(self.timings) / len(self.timings)
    
    @property
    def stdev(self):
        mean = self.average
        return (math.fsum([(x - mean) ** 2 for x in self.timings]) / len(self.timings)) ** 0.5

    def __str__(self):
        pm = '+-'
        if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding:
            try:
                u'\xb1'.encode(sys.stdout.encoding)
                pm = u'\xb1'
            except:
                pass
        return "{mean}".format(
            mean=_format_time(self.average),
        )

    def _repr_pretty_(self, p , cycle):
        unic = self.__str__()
        p.text(u'<TimeitResult : '+unic+u'>')

def _format_time(timespan):
    """Formats the timespan in a human readable form"""

    if timespan >= 60.0:
        # we have more than a minute, format that in a human readable form
        # Idea from http://snipplr.com/view/5713/
        parts = [("d", 60*60*24),("h", 60*60),("min", 60), ("s", 1)]
        time = []
        leftover = timespan
        for suffix, length in parts:
            value = int(leftover / length)
            if value > 0:
                leftover = leftover % length
                time.append(u'%s%s' % (str(value), suffix))
            if leftover < 1:
                break
        return " ".join(time)

@magics_class
class ExecutionMagics(Magics):
    """Magics related to code execution, debugging, profiling, etc."""

    _transformers: Dict[str, Any] = {}

    def __init__(self, shell):
        super(ExecutionMagics, self).__init__(shell)
        # Default execution function used to actually run user code.
        self.default_runner = None

    def _read_cell(self, line='', cell=None):
        #logger = log_configure(loglevel, 'Cell Reader')
        logger.debug("Line: {}".format(line))
        logger.debug("Cell contents: {}".format(cell))

        opts, stmt = self.parse_options(
            line, "n:r:tcp:qo", posix=False, strict=False, preserve_non_opts=True
        )
        if stmt == "" and cell is None:
            logger.debug("line and cell are empty")
            return
        else:
            logger.debug("opts, intered code: {}, {}".format(opts, stmt))
            return opts, stmt 
        
    @skip_doctest
    @no_var_expand
    @line_cell_magic
    @needs_local_scope
    def estimated_calculation_time(self, line='', cell=None, local_ns=None):
        logger = log_configure(loglevel, 'Estim.Calc.Time')
        opts, _stmt = self._read_cell(line=line, cell=cell)
        dataset_time_size, stmt, stmt_original = self._one_time_element(_stmt)

        _dataset_time_size, two_steps, _stmt_original = self._two_time_elements(_stmt)

        logger.debug("Time-sliced dataset: {}".format(stmt))
        logger.debug("Two Time-sliced dataset: {}".format(two_steps))
        #logger.debug("Original/Full dataset: {}".format(stmt_original))
        #logger.debug("Numer of timesteps of original dataset: {}".format(dataset_time_size))

        transform  = self.shell.transform_cell

        if cell is None:
            # called as line magic
            ast_setup = self.shell.compile.ast_parse("pass")
            ast_stmt = self.shell.compile.ast_parse(transform(stmt))
            ast_two_steps = self.shell.compile.ast_parse(transform(two_steps))
            expr = self.shell.transform_cell(dataset_time_size)

        else:
            ast_setup = self.shell.compile.ast_parse(transform(stmt))
            ast_stmt = self.shell.compile.ast_parse(transform(cell))
            ast_two_steps = self.shell.compile.ast_parse(transform(two_steps))
            expr = self.shell.transform_cell(dataset_time_size)

        ast_setup = self.shell.transform_ast(ast_setup)
        ast_stmt = self.shell.transform_ast(ast_stmt)
        ast_two_steps = self.shell.transform_ast(ast_two_steps)

        expr_ast = self.shell.compile.ast_parse(expr)
        expr_ast = self.shell.transform_ast(expr_ast)

        self.shell.compile(ast_setup, "<magic-timeit-setup>", "exec")
        self.shell.compile(ast_stmt, "<magic-timeit-stmt>", "exec")
        #self.shell.compile(ast_two_time_steps, "<magic-timeit-stmt>", "exec")

        expr_val=None
        if len(expr_ast.body)==1 and isinstance(expr_ast.body[0], ast.Expr):
            mode = 'eval'
            source = '<timed eval>'
            expr_ast = ast.Expression(expr_ast.body[0].value)
        else:
            mode = 'exec'
            source = '<timed exec>'
            # multi-line %%time case
            if len(expr_ast.body) > 1 and isinstance(expr_ast.body[-1], ast.Expr):
                expr_val= expr_ast.body[-1]
                expr_ast = expr_ast.body[:-1]
                expr_ast = ast.Module(expr_ast, [])
                expr_val = ast.Expression(expr_val.value)
        code_data_size = self.shell.compile(expr_ast, source, mode)
        
        timeit_ast_template = ast.parse('def inner(_it, _timer):\n'
                                        '    setup\n'
                                        '    _t0 = _timer()\n'
                                        '    for _i in _it:\n'
                                        '        stmt\n'
                                        '    _t1 = _timer()\n'
                                        '    return _t1 - _t0\n')

        
        timeit_ast = TimeitTemplateFiller(ast_setup, ast_stmt).visit(timeit_ast_template)
        timeit_ast = ast.fix_missing_locations(timeit_ast)
        
        timeit_ast_two = TimeitTemplateFiller(ast_setup, ast_two_steps).visit(timeit_ast_template)
        timeit_ast_two = ast.fix_missing_locations(timeit_ast_two)

        # Track compilation time so it can be reported if too long
        # Minimum time above which compilation time will be reported
        tc_min = 0.1
        ns = {}
        glob = self.shell.user_ns

        data_size_calc = eval(code_data_size, glob, local_ns)
        logger.debug("Number of Timesteps of Original Dataset: {}".format(data_size_calc))

        # handles global vars with same name as local vars. We store them in conflict_globs.
        conflict_globs = {}
        if local_ns and cell is None:
            for var_name, var_val in glob.items():
                if var_name in local_ns:
                    conflict_globs[var_name] = var_val
            glob.update(local_ns)

        #if 'c' in opts:
        t0 = time.time() 
        #logger.debug("c in options, repited")
        code = self.shell.compile(timeit_ast, "<magic-timeit>", "exec")
        tc = time.time() - t0
        exec(code, glob, ns)
        timer = Timer(timer=clock)
        timer.inner = ns["inner"]
        t1 = time.time()
        repeat, number = 1, 1
        all_runs = timer.repeat(repeat, number)
        t2 = time.time()


        #_t0 = time.time() 
        #code = self.shell.compile(timeit_ast_two, "<magic-timeit>", "exec")
        ##exec(code, glob, ns)
        #timer = Timer(timer=clock)
        #timer.inner = ns["inner"]
        #repeat, number = 1, 2
        #all_runs = timer.repeat(repeat, number)
        #_t2 = time.time()

        expected_calc_time = float(t1 - t0 + 0.1)*int(data_size_calc) + float(t2-t0)
        logger.debug("Expected calculation time approx: {}".format(expected_calc_time ))

            #del_tot_time = (_t2 - _t0)/4 -  (t2 - t0)/2
            #expected_calc_time = float(del_tot_time)*int(data_size_calc) + float(0.5*(t2-t0) - del_tot_time)
            #logger.debug("first duration and second: {}/{}".format(_t2 - _t0, t2 - t0))
            #logger.debug("Expected calculation time AFTER: {}/{}".format(del_tot_time, expected_calc_time ))
        #elif 'o' in opts:
        #    logger.debug("o option")
        #    code = self.shell.compile(timeit_ast, "<magic-timeit>", "exec")
        #    
        #    exec(code, glob, ns)
        #    t0 = time.time() 
        #    timer = Timer(timer=clock)
        #    timer.inner = ns["inner"]
        #    repeat, number = 1, 1
        #    all_runs = timer.repeat(repeat, number)
        #    t1 = time.time()
        #    expected_calc_time = float(t1-t0)*int(data_size_calc)
        #else:
        #    logger.debug("None in options")
        #    t0 = time.time() 
        #    code = self.shell.compile(timeit_ast, "<magic-timeit>", "exec")
        ##    tc = time.time() - t0
        #    exec(code, glob, ns)
        #    t1 = time.time()
        #    expected_calc_time = float(t1-t0)*int(data_size_calc)


        logger.debug("Expected calculation time: {}".format(expected_calc_time ))
        return expected_calc_time
        
        #timeit_result = TimeitResult(number, repeat, all_runs, tc, 3)
        #return timeit_result
        #https://github.com/ipython/ipython/blob/main/IPython/core/magics/execution.py


    def _one_time_element(self, src):
        #logger = log_configure(loglevel, 'Func.Splitter')
        parts = src.split('(')
        if len(parts) < 2:
            logger.warning("Invalid input. Please provide a function name followed by arguments.")
            return
        function_name = parts[0].strip()
        arguments_str = parts[1].strip(' )')

        try:
            arguments = [arg.strip() for arg in arguments_str.split(',')]
        except Exception as e:
            logger.warning(f"Error parsing arguments: {e}")
            return
        original_function = function_name+'('+', '.join(str(e) for e in arguments)+')'
        dataset_time_size = arguments[0]+'.time.size'
        
        arguments[0] = arguments[0]+'.isel(time=0)'
        modified_function = function_name+'('+', '.join(str(e) for e in arguments)+')'

        return dataset_time_size, modified_function,  original_function
    
    def _two_time_elements(self, src):
        parts = src.split('(')
        if len(parts) < 2:
            logger.warning("Invalid input. Please provide a function name followed by arguments.")
            return
        function_name = parts[0].strip()
        arguments_str = parts[1].strip(' )')

        try:
            arguments = [arg.strip() for arg in arguments_str.split(',')]
        except Exception as e:
            logger.warning(f"Error parsing arguments: {e}")
            return
        original_function = function_name+'('+', '.join(str(e) for e in arguments)+')'
        dataset_time_size = arguments[0]+'.time.size'
        
        arguments[0] = arguments[0]+'.isel(time=slice(0,2))'
        modified_function = function_name+'('+', '.join(str(e) for e in arguments)+')'

        return dataset_time_size, modified_function,  original_function

    @line_cell_magic
    @register_line_cell_magic
    def run_with_progress(self, line='', cell=None, local_ns=None):
        logger = log_configure(loglevel, 'Progress Bar')
        #opts, _stmt = self._read_cell(line=line, cell=cell)


        # Create threads for each process
        thread1 = multiprocessing.Process(target=self._target_function_with_progress, args=(line, cell, None)) 
        thread2 = multiprocessing.Process(target=self._function_with_progress_bar, args=(line, cell, None))

        # Start the threads
        thread1.start()
        thread2.start()

        # Wait for threads to finish
        thread1.join()
        thread2.join()

    def _function_with_progress_bar(self, line='', cell=None, local_ns=None):
        expected_calc_time = self.estimated_calculation_time(line=line, cell=cell, local_ns=local_ns)
        logger.debug("Expected calculation time: {}".format(expected_calc_time ))

        total_iterations = 100  # Total number of iterations
        progress_bar_template = "[{:<40}] {}%"

        for i in range(total_iterations):
            # Perform some work
            time.sleep(expected_calc_time/total_iterations)

            # Calculate progress and update the progress bar
            ratio = i / total_iterations
            progress = int(40 * ratio)
            print(progress_bar_template.format("=" * progress, int(ratio * 100)), end="\r")

    def _target_function_with_progress(self, line='', cell=None, local_ns=None):

        logger.info('target_function_with_progress')
        opts, stmt = self._read_cell(line=line, cell=cell)

        transform  = self.shell.transform_cell

        if cell is None:
            # called as line magic
            ast_setup = self.shell.compile.ast_parse("pass")
            ast_stmt = self.shell.compile.ast_parse(transform(stmt))
        else:
            ast_setup = self.shell.compile.ast_parse(transform(stmt))
            ast_stmt = self.shell.compile.ast_parse(transform(cell))

        ast_setup = self.shell.transform_ast(ast_setup)
        ast_stmt = self.shell.transform_ast(ast_stmt)

        self.shell.compile(ast_setup, "<magic-timeit-setup>", "exec")
        self.shell.compile(ast_stmt, "<magic-timeit-stmt>", "exec")

        timeit_ast_template = ast.parse('def inner(_it, _timer):\n'
                                        '    setup\n'
                                        '    _t0 = _timer()\n'
                                        '    for _i in _it:\n'
                                        '        stmt\n'
                                        '    _t1 = _timer()\n'
                                        '    return _t1 - _t0\n')

        
        timeit_ast = TimeitTemplateFiller(ast_setup, ast_stmt).visit(timeit_ast_template)
        timeit_ast = ast.fix_missing_locations(timeit_ast)
        
        # Track compilation time so it can be reported if too long
        # Minimum time above which compilation time will be reported
        ns = {}
        glob = self.shell.user_ns

        # handles global vars with same name as local vars. We store them in conflict_globs.
        conflict_globs = {}
        if local_ns and cell is None:
            for var_name, var_val in glob.items():
                if var_name in local_ns:
                    conflict_globs[var_name] = var_val
            glob.update(local_ns)

        t0 = time.time() 
        #logger.debug("t0: {}".format(t0))
        code = self.shell.compile(timeit_ast, "<magic-timeit>", "exec")

        #if 'q' in opts:
        #    tc = time.time() - t0
        #    exec(code, glob, ns)
        #    t1 = time.time()
            #total_calculation_time = float(t1-t0)
        #else:

        exec(code, glob, ns)
        timer = Timer(timer=clock)
        timer.inner = ns["inner"]
        t1 = time.time()
            #total_calculation_time = float(t1-t0)
        
        total_calculation_time =  t1 - t0
        logger.debug("Total calculation time: {}".format(total_calculation_time))

        #timer = Timer(timer=clock)
        #timer.inner = ns["inner"]
        #repeat, number = 1, 1
        #all_runs = timer.repeat(repeat, number)
        if 'o' in opts:
            return total_calculation_time




ip = get_ipython()
ip.register_magics(ExecutionMagics)

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


    








