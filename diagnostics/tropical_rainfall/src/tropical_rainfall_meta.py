from .tropical_rainfall_main import MainClass 

# Full import
methods_to_import = [method for method in dir(MainClass) if callable(getattr(MainClass, method)) and not method.startswith("__")]
methods_to_import.remove('class_attributes_update')

# Reduced import will shorten the documentation.
# methods_to_import = ['histogram', 'merge_list_of_histograms', 'histogram_plot', 'average_into_netcdf',
#                    'plot_of_average', 'plot_bias', 'plot_seasons_or_months', 'seasonal_or_monthly_mean',
#                    'map', 'get_95percent_level', 'seasonal_095level_into_netcdf', 'add_UTC_DataAaray',
#                    'daily_variability_plot']

class MetaClass(type):
    def __new__(cls, name, bases, dct):
        if 'import_methods' in dct:
            methods_to_import = [method for method in dir(MainClass) if
                                 callable(getattr(MainClass, method)) and not method.startswith("__")]
            for method_name in methods_to_import:
                dct[method_name] = getattr(MainClass, method_name)
                
        if 'class_attributes_update' in dct:
            def class_attributes_update(self, **kwargs):
                attribute_names = ['trop_lat', 's_time', 'f_time', 's_year', 'f_year', 's_month',
                                   'f_month', 'num_of_bins', 'first_edge', 'width_of_bin', 'bins',
                                   'model_variable', 'new_unit']
                for attr_name in attribute_names:
                    if attr_name in kwargs and isinstance(kwargs[attr_name], type(getattr(self, attr_name))):
                        setattr(self, attr_name, kwargs[attr_name])
                        setattr(self.main, attr_name, kwargs[attr_name])
                    # Check the types of the attributes. The commented lines below don't work correctly.
                    # elif attr_name in kwargs and not isinstance(kwargs[attr_name], type(getattr(self, attr_name))):
                    #    raise TypeError(f"{attr_name} must be {type(getattr(self, attr_name))}")
                    else:
                        pass
            dct['class_attributes_update'] = class_attributes_update
        return super(MetaClass, cls).__new__(cls, name, bases, dct)