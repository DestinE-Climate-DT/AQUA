import operator
import re
import xarray as xr
from aqua.core.logger import log_configure, log_history

# define math operators: order is important, since defines
# which operation is done at first!
# Higher precedence operations are evaluated first
OPS = {
    '^': operator.pow,      # Power operator (highest precedence)
    '/': operator.truediv,  # Division
    "*": operator.mul,      # Multiplication  
    "-": operator.sub,      # Subtraction
    "+": operator.add       # Addition (lowest precedence)
}

class EvaluateFormula:
    """
    Class to evaluate a formula based on a string input.
    """

    def __init__(self, data: xr.Dataset, formula: str,
                 units: str = None, short_name: str = None, long_name: str = None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the EvaluateFormula class.

        Args:
            data (xr.Dataset): The input data to evaluate the formula against.
            formula (str): The formula to evaluate. Supports basic arithmetic operators (+, -, *, /, ^)
                          and parentheses for grouping operations. Examples:
                          - "var1 + var2"
                          - "var1 * (var2 + var3)"
                          - "var1^2 + var2^0.5"
                          - "(var1 + var2) / (var3 - var4)"
            units (str, optional): The units of the resulting data.
            short_name (str, optional): A short name for the resulting data.
            long_name (str, optional): A long name for the resulting data.
            loglevel (str, optional): The logging level to use. Defaults to 'WARNING'.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='EvaluateFormula')
        self.data = data
        self.formula = self.consolidate_formula(formula)
        self.units = units
        self.short_name = short_name
        self.long_name = long_name

        self.token = self._extract_tokens()

    def _evaluate(self):
        """
        Evaluate the formula using the provided data.
        Handles parentheses by recursively evaluating sub-expressions.

        Returns:
            xr.DataArray: The result of the evaluated formula as an xarray DataArray.
        """
        self.logger.debug('Evaluating formula: %s', self.formula)
        
        # Handle parentheses first by recursively evaluating sub-expressions
        formula_with_parentheses = self._handle_parentheses(self.formula)
        
        # Re-tokenize after parentheses handling
        self.token = self._extract_tokens(formula_with_parentheses)
        
        if not self.token:
            self.logger.error('No tokens extracted from the formula.')

        if len(self.token) > 1:
            if self.token[0] == '-':
                return -self.data[self.token[1]]
            return self._operations()
        return self.data[self.token[0]]
    

    def evaluate(self):
        """
        Evaluate the formula using the provided data.

        Returns:
            xr.DataArray: The result of the evaluated formula as an xarray DataArray.
        """

        out = self._evaluate()
        return self._update_attributes(out)


    def _extract_tokens(self, formula_str=None):
        """
        Tokenize the formula string into individual components.

        Returns:
            list: A list of tokens extracted from the formula.
        """
        if formula_str is None:
            formula_str = self.formula

        token = [i for i in re.split('([^\\w.]+)', formula_str) if i and i.strip()]
        return token
    
    def _handle_parentheses(self, formula_str):
        """
        Handle parentheses in the formula by recursively evaluating sub-expressions.
        
        This method finds the innermost parentheses, evaluates the expression within them,
        stores the result as a temporary variable, and replaces the parentheses expression
        with the temporary variable name.

        Args:
            formula_str (str): The formula string potentially containing parentheses.

        Returns:
            str: The formula string with parentheses resolved.
        """
        temp_var_counter = 0
        
        while '(' in formula_str:
            # Find the innermost parentheses (rightmost opening parenthesis)
            start = -1
            for i, char in enumerate(formula_str):
                if char == '(':
                    start = i
                elif char == ')' and start != -1:
                    # Found a complete parenthesis pair
                    sub_expr = formula_str[start+1:i]
                    
                    # Create temporary variable name
                    temp_var_name = f'_temp_{temp_var_counter}'
                    temp_var_counter += 1
                    
                    # Recursively evaluate the sub-expression
                    sub_evaluator = EvaluateFormula(
                        data=self.data,
                        formula=sub_expr,
                        loglevel=self.loglevel
                    )
                    sub_result = sub_evaluator._evaluate()
                    
                    # Store the result in our data dictionary for later use
                    # We'll need to modify _operations to handle temp variables
                    self.data = self.data.copy()  # Avoid modifying original data
                    self.data[temp_var_name] = sub_result
                    
                    # Replace the parentheses expression with temp variable
                    formula_str = formula_str[:start] + temp_var_name + formula_str[i+1:]
                    break
                    
        return formula_str
    
    def _operations(self):
        """
        Parsing of the operations using operator package and precedence-based evaluation.
        Now supports power operator (^) and handles temporary variables from parentheses.
        
        Operations are evaluated in order of precedence:
        1. ^ (power) - highest precedence
        2. *, / (multiplication, division)
        3. +, - (addition, subtraction) - lowest precedence

        Returns:
            xr.DataArray: The result of the evaluated formula as an xarray DataArray.
        """

        # use a dictionary to store xarray field and call them easily
        dct = {}
        for k in self.token:
            if k not in OPS:
                try:
                    dct[k] = float(k)
                except ValueError:
                    # Handle both regular variables and temporary variables from parentheses
                    if k in self.data:
                        dct[k] = self.data[k]
                    else:
                        self.logger.error(f'Variable {k} not found in data')
                        raise KeyError(f'Variable {k} not found in data')
        
        # apply operators to all occurrences, from top priority
        # Operations are processed in order of precedence (as defined in OPS dictionary)
        code = 0
        for p in OPS:
            while p in self.token:
                code += 1
                x = self.token.index(p)
                name = 'op' + str(code)
                
                # Use apply_ufunc to maintain xarray functionality
                replacer = xr.apply_ufunc(OPS.get(p), dct[self.token[x - 1]], dct[self.token[x + 1]],
                                        keep_attrs=True, dask='parallelized')
                dct[name] = replacer
                self.token[x - 1] = name
                del self.token[x:x + 2]

        return replacer
    
    def _update_attributes(self, out):
        """
        Update the attributes of the output DataArray.

        Args:
            out (xr.DataArray): The output DataArray to update.

        Returns:
            xr.DataArray: The updated DataArray with new attributes.
        """
        if self.units:
            out.attrs['units'] = self.units
        if self.short_name:
            out.attrs['short_name'] = self.short_name
            out.attrs['name'] = self.short_name
        if self.long_name:
            out.attrs['long_name'] = self.long_name
        out.attrs['AQUA_formula'] = self.formula

        msg = f'Evaluated formula: {self.formula}'
        if self.units:
            msg += f' with units: {self.units}'
        if self.short_name:
            msg += f', name: {self.short_name}'
        if self.long_name:
            msg += f', long name: {self.long_name}'

        out = log_history(out, msg)
        self.logger.debug(msg)

        return out
    
    @staticmethod
    def consolidate_formula(formula: str):
        """
        Consolidate the formula by removing unnecessary spaces and ensuring proper formatting.
        Now also validates parentheses matching.

        Args:
            formula (str): The formula to consolidate.

        Returns:
            str: The consolidated formula.
            
        Raises:
            ValueError: If parentheses are not properly matched.
        """
        # Remove spaces and ensure proper formatting
        consolidated = re.sub(r'\s+', '', formula)
        
        # Validate parentheses matching
        paren_count = 0
        for char in consolidated:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
                if paren_count < 0:
                    raise ValueError("Mismatched parentheses: closing parenthesis without opening")
        
        if paren_count != 0:
            raise ValueError("Mismatched parentheses: unclosed opening parenthesis")
            
        return consolidated
