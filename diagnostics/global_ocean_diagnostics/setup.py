
from global_ocean_class_basin_T_S_means import Global_OceanDiagnostic

# Create an instance of the Ocean_circulationDiagnostic class
diagnostic = Global_OceanDiagnostic(model='FESOM', exp='tco2559-ng5-cycle3', source='lra-r100-monthly')

# Run the diagnostics
diagnostic.run()