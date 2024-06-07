from .tropical_rainfall_histograms import HistogramClass
from .tropical_rainfall_zonal_mean import ZonalMeanClass
from .tropical_rainfall_daily_variability import DailyVariabilityClass
from .tropical_rainfall_extra import ExtraFunctionalityClass

# Combine methods from all classes
methods_to_import = []
classes_to_import = [HistogramClass, ZonalMeanClass, DailyVariabilityClass, ExtraFunctionalityClass]

for cls in classes_to_import:
    methods_to_import.extend(
        method for method in dir(cls) if callable(getattr(cls, method)) and not method.startswith("__")
    )
    
class MetaClass(type):
    def __new__(cls, name, bases, dct):
        if 'import_methods' in dct:
            classes_to_import = [HistogramClass, ZonalMeanClass, DailyVariabilityClass, ExtraFunctionalityClass]
            
            # Combine methods from all classes
            for import_cls in classes_to_import:
                methods_to_import = [
                    method for method in dir(import_cls) if callable(getattr(import_cls, method)) and not method.startswith("__")
                ]
                for method_name in methods_to_import:
                    dct[method_name] = getattr(import_cls, method_name)
                    
            # Define a method to combine attributes from all classes
            def class_attributes_update(self, **kwargs):
                for import_cls in classes_to_import:
                    attribute_names = [attr for attr in dir(import_cls) if not callable(getattr(import_cls, attr)) and not attr.startswith("__")]
                    for attr_name in attribute_names:
                        if attr_name in kwargs and kwargs[attr_name] is not None:
                            setattr(self, attr_name, kwargs[attr_name])
                            for cls_instance in self.classes_instances:
                                if hasattr(cls_instance, attr_name):
                                    setattr(cls_instance, attr_name, kwargs[attr_name])
            dct['class_attributes_update'] = class_attributes_update

        return super(MetaClass, cls).__new__(cls, name, bases, dct)
