import operator
import re
import xarray as xr
from aqua.logger import log_configure, log_history

# define math operators: order is important, since defines
# which operation is done at first!
OPS = {
    '/': operator.truediv,
    "*": operator.mul,
    "-": operator.sub,
    "+": operator.add
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
            formula (str): The formula to evaluate.
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

        Returns:
            xr.DataArray: The result of the evaluated formula as an xarray DataArray.
        """
        self.logger.debug('Evaluating formula: %s', self.formula)
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


    def _extract_tokens(self):
        """
        Tokenize the formula string into individual components.

        Returns:
            list: A list of tokens extracted from the formula.
        """
        token = [i for i in re.split('([^\\w.]+)', self.formula) if i]
        return token
    
    def _operations(self):
        """
        Parsing of the CDO-based commands using operator package
        and an ad-hoc dictionary. Could be improved, working with four basic
        operations only.

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
                    dct[k] = self.data[k]
        
        # apply operators to all occurrences, from top priority
        # so far this is not parsing parenthesis
        code = 0
        for p in OPS:
            while p in self.token:
                code += 1
                # print(token)
                x = self.token.index(p)
                name = 'op' + str(code)
                # replacer = ops.get(p)(dct[token[x - 1]], dct[token[x + 1]])
                # Using apply_ufunc in order not to
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

        Args:
            formula (str): The formula to consolidate.

        Returns:
            str: The consolidated formula.
        """
        # Remove spaces and ensure proper formatting
        return re.sub(r'\s+', '', formula)
