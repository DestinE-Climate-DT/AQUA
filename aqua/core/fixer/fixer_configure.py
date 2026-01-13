"""Class for fixer configuration and loading"""
from aqua.core.logger import log_configure

class FixerConfigure:
    """
    Class to configure the fixer based on convention and fixer_name.
    Args:
        convention (str): The convention name (e.g., 'eccodes'). Default is None
        fixes_dictionary (dict): The fixes dictionary loaded from the fixes file.
        fixer_name (str): The fixer name to load specific fixes. Default is None
        loglevel (str): Log level for logging. Default is 'WARNING'.
    """

    def __init__(self, convention=None, fixes_dictionary=None, fixer_name=None, loglevel='WARNING'):

        self.convention = convention
        self.fixer_name = fixer_name
        self.fixes_dictionary = fixes_dictionary
        self.logger = log_configure(log_level=loglevel, log_name="FixerConfigure")

    def find_fixes(self):
        """
        Get the fixes for the required model, experiment and source.
        A convention dictionary is loaded and merged with the fixer_name.
        The default convention is eccodes-2.39.0.

        If not found, looks for default fixes for the model.
        Then source_fixes are loaded and merged with base_fixes.

        This second block of search is deprecated and will be removed in the future.

        Args:
            The fixer class

        Return:
            The fixer dictionary
        """
        # The convention dictionary is defined by stardard: eccodes and version: 2.39.0
        # At the actual stage 'eccodes' is the default and if not set to None or 'eccodes'
        # an error is raised in the Reader initialization
        convention_dictionary = self._load_convention_dictionary() if self.convention else None

        # Here we load the fixer_name, the specific fix that is merged with the convention dictionary
        # The merge is done only if the base_fixes dictionary has the 'convention' field matching the
        # convention dictionary.
        base_fixes = self._load_fixer_name()
        return self._combine_convention(base_fixes, convention_dictionary)

    def _load_convention_dictionary(self, version='2.39.0'):
        """
        Load the convention dictionary from the fixer file.
        The convention_name block should be called <convention>-<version>.

        Args:
            version: The convention name version. Default is 2.39.0

        Returns:
            The convention dictionary
        """
        convention_dictionary = self.fixes_dictionary.get("convention_name", None)
        if convention_dictionary is None:
            self.logger.error("Convention dictionary not found, will be disabled")
            return None

        convention_name = self.convention + '-' + version

        convention_dictionary = convention_dictionary.get(convention_name, None)
        if convention_dictionary is None:
            self.logger.error("No convention dictionary found for %s", convention_name)
            return None
        else:
            self.logger.info("Convention dictionary: %s", convention_name)

        return convention_dictionary

    def _combine_convention(self, base_fixes: dict, convention_dictionary: dict):
        """
        Combine convention dictionary with the fixes.
        It scan the 'vars' block and merge the convention dictionary with the fixer_name.
        If an item is new in the convention dictionary, it is added to the fixes.
        If the item is already present, non existing keys are added,
        otherwise the existing keys (in the base file) are kept.

        Args:
            base_fixes (dict): The base fixes (coming from the fixer_name)
            convention_dictionary (dict): The convention dictionary

        Returns:
            The final fixes, with the convention dictionary merged
        """
        if base_fixes is None:
            self.logger.info("No fixer_name found, only convention will be applied")
            base_fixes = {}
            base_convention = 'eccodes'
        elif convention_dictionary is None:
            self.logger.info("No convention dictionary found, only fixer_name will be applied")
            return base_fixes
        else:
            base_convention = base_fixes.get('convention', None)
        # We do not crash if the convention is not eccodes, but we log an error and return the base fixes
        convention = convention_dictionary.get('convention', None)
        if convention != 'eccodes':
            self.logger.error("Convention %s not supported, only eccodes is supported", convention)
            return base_fixes

        # We make sure that the convention is the same in the convention dictionary and in the fixer_name
        # Additionally, we check that the version is the same, knowing that for the moment we are not going to
        # document this feature since the version is hardcoded in the code.
        # TODO: version should be a parameter in the fixer_name and in the convention dictionary
        if base_convention is not None:
            if base_convention != convention and convention is not None:
                raise ValueError(f"The convention in the convention dictionary: {base_convention} is different from the fixer_name: {convention}")
            if 'version' in base_fixes and 'version' in convention_dictionary:
                if base_fixes['version'] != convention_dictionary['version']:
                    raise ValueError(f"The version in the convention dictionary: {base_fixes['version']} is different from the fixer_name: {convention_dictionary['version']}")
        else:
            self.logger.info("No convention found in the fixer_name, the convention dictionary will not be used")
            return base_fixes

        # Merge one by one the variables. This is done so that we can be careful at the level of the individual variables.
        # If a field for the specific variable is present in the base_fixes, it has priority.
        if 'vars' in convention_dictionary:
            if 'vars' not in base_fixes:
                base_fixes['vars'] = convention_dictionary['vars']
            else:  # A merge one variable by one is needed
                for var_key in convention_dictionary['vars'].keys():
                    if var_key in base_fixes['vars']:
                        # self.logger.debug("Variable %s already present in the fixes, merging...", var_key)
                        # This requires python >= 3.9
                        base_fixes['vars'][var_key] = convention_dictionary['vars'][var_key] | base_fixes['vars'][var_key]
                        # We need to check that only one between 'source' and 'derived' is present.
                        # If both are present, we give priority to 'derived' and log an info.
                        if 'source' in base_fixes['vars'][var_key] and 'derived' in base_fixes['vars'][var_key]:
                            self.logger.info("Variable %s has both 'source' and 'derived' in the fixes, 'derived' will be used",  # noqa: E501
                                             var_key)
                            base_fixes['vars'][var_key].pop('source')
                    else:
                        # This is a new variable to fix
                        # We need to manipulate the convention_dictionary to set the grib flag
                        # TODO: expand to other formats (cmor tables, etc.)
                        new_var = convention_dictionary['vars'][var_key]
                        if convention == 'eccodes':
                            new_var['grib'] = convention_dictionary['vars'][var_key].get('grib', True)
                        base_fixes['vars'][var_key] = new_var
        else:
            self.logger.warning("No 'vars' block found in the convention dictionary")

        return base_fixes

    def _load_fixer_name(self):
        """
        Load the fixer_name reading from the metadata of the catalog.
        If the fixer_name has a parent, load it and merge it giving priority to the child.
        """
        # if fixer name is found, get it
        if self.fixer_name is not None:
            self.logger.info('Fix names in metadata is %s', self.fixer_name)
            fixes = self.fixes_dictionary["fixer_name"].get(self.fixer_name)
            if fixes is None:
                self.logger.error("The requested fixer_name %s does not exist in fixes files", self.fixer_name)
                return None
            else:
                self.logger.info("Fix names %s found in fixes files", self.fixer_name)

                if 'parent' in fixes:
                    parent_fixes = self.fixes_dictionary["fixer_name"].get(fixes['parent'])
                    if parent_fixes is not None:
                        self.logger.info("Parent fix %s found! Mergin with fixer_name fixes %s!", fixes['parent'],
                                         self.fixer_name)
                        fixes = self._merge_fixes(parent_fixes, fixes)
                    else:
                        self.logger.error("Parent fix %s defined but not available in the fixes file.", fixes['parent'])

            return fixes

        # if not fixes found, return fixes None
        return None

    @staticmethod
    def _merge_fixes(base, specific):
        """
        Small function to merge fixes. Base fixes will be used as a default
        and specific fixes will replace where necessary. Dictionaries will be merged
        for variables, with priority for the specific ones.

        Args:
            base (dict): Base fixes
            specific (dict): Specific fixes

        Return:
            dict with merged fixes
        """
        final = base
        for item in specific.keys():
            if item == 'vars' and item in base:
                final[item] = {**base[item], **specific[item]}
            else:
                final[item] = specific[item]

        return final