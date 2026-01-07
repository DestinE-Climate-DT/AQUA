"""
Title generation class and utilities for AQUA plots.
"""

from typing import Optional, Union
from .util import to_list
from .string import strlist_to_phrase


class TitleBuilder:
    """
    Class to generate standardized titles for AQUA plots.

    Args:
        title (str, optional): Explicit title override.
        diagnostic (str, optional): Name of the diagnostic (e.g., 'Seasonal cycle', 'Global bias').
        variable (str, optional): Long name of the variable (e.g., 'Total precipitation rate').
        regions (str or list, optional): Region name(s) (e.g., 'global', 'North Atlantic').
        models (str or list, optional): Model name(s).
        exps (str or list, optional): Experiment name(s).
        realizations (str or list, optional): Realization name(s).
        comparison (str, optional): Formulation for the comparison. Default is 'relative to'.
        ref_model (str or list, optional): Reference model name.
        ref_exp (str or list, optional): Reference experiment name.
        timeseason (str, optional): Season or month (e.g., 'JJA', 'March').
        extra_info (str or list, optional): Extra information to be added to the title.

    Returns:
        str: The generated title.
    """

    def __init__(self, 
                 title: Optional[str] = None,
                 diagnostic: Optional[str] = None,
                 variable: Optional[str] = None,
                 regions: Optional[Union[str, list]] = None,
                 conjunction: Optional[str] = None,
                 catalog: Optional[str] = None,
                 models: Optional[Union[str, list]] = None, 
                 exps: Optional[Union[str, list]] = None,
                 realizations: Optional[Union[str, list]] = None,
                 comparison: Optional[str] = None,
                 ref_model: Optional[str] = None, 
                 ref_exp: Optional[str] = None,
                 timeseason: Optional[str] = None,
                 startyear: Optional[int] = None,
                 endyear: Optional[int] = None,
                 extra_info: Optional[Union[str, list]] = None,
                 ):

        self.title = title
        self.diagnostic = diagnostic
        self.variable = variable
        self.regions = regions
        self.conjunction = conjunction
        self.catalog = catalog
        self.models = to_list(models) if models is not None else []
        self.exps = to_list(exps) if exps is not None else []
        self.realizations = to_list(realizations) if realizations is not None else []
        self.comparison = comparison
        self.ref_model = ref_model
        self.ref_exp = ref_exp
        self.timeseason = timeseason
        self.startyear = str(startyear) if isinstance(startyear, int) else startyear
        self.endyear = str(endyear) if isinstance(endyear, int) else endyear
        self.extra_info = extra_info


    def _set_models(self) -> str | None:
        """
        Generate the models
        """
        if self.models or self.exps:
            if len(self.models) > 1 or len(self.exps) > 1:
                return "Multi-model "
            parts = ''
            parts += f"{self.models[0]} " if len(self.models) > 0 else ''
            parts += f"{self.exps[0]} " if len(self.exps) > 0 else ''
            return parts
        return None

    def _set_ref(self) -> str | None:
        """
        Generate the reference
        """
        if self.ref_model or self.ref_exp:
            parts = ''
            parts += f"{self.catalog} " if self.catalog else ''
            parts += f"{self.ref_model} " if self.ref_model else ''
            parts += f"{self.ref_exp} " if self.ref_exp else ''
            return parts
        return None
    
    def _set_years(self) -> str | None:
        """
        Generate the years
        """
        if self.startyear and self.endyear:
            return f"{self.startyear}-{self.endyear}"
        if self.startyear:
            return self.startyear
        if self.endyear:
            return self.endyear
        return None

    def generate(self) -> str:
        """
        Generate the whole title
        """

        if self.title:
            return self.title

        title = ''
        if self.diagnostic:
            title += f"{self.diagnostic} "

        if self.variable:
            title += f"of {self.variable} " if self.diagnostic else f"{self.variable} "

        if self.regions:
            regions_list = to_list(self.regions)
            regions_str = strlist_to_phrase(regions_list)
            if regions_str:
                title += f"[{regions_str}] "
        
        models_part = self._set_models()
        if models_part:
            if self.variable:
                title += f"{self.conjunction} " if self.conjunction else 'for '
            title += f"{models_part} "

        if self.realizations:
            if len(self.realizations) > 1:
                title += f"Multi-realization "
            else:
                title += f"{self.realizations[0]} "

        ref_part = self._set_ref()
        if ref_part:
            if self.variable:
                title += self.comparison if self.comparison else 'relative to '
            title += ref_part

        if self.timeseason:
            title += f" {self.timeseason}"

        years = self._set_years()
        if years:
            title += f" {years}"

        if self.extra_info:
            title += f" {' '.join(to_list(self.extra_info))}"

        return title.strip()
