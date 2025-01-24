diagnostic_config = {
    'atmglobalmean': [
        {
            'config_file': 'atm_mean_bias_config.yaml',
            'source_path': 'diagnostics/atmglobalmean/cli/config',
            'target_path': 'diagnostics/atmglobalmean/cli'
        },
    ],
    'global_biases': [
        {
            'config_file': 'config_global_biases.yaml',
            'source_path': 'config/diagnostics/global_biases',
            'target_path': 'diagnostics/global_biases/cli'
        },
    ],
    'ecmean': [
        {
            'config_file': 'ecmean_config_climatedt.yaml',
            'source_path': 'config/diagnostics/ecmean',
            'target_path': 'diagnostics/ecmean'
        },
        {
            'config_file': 'interface_AQUA_climatedt.yaml',
            'source_path': 'config/diagnostics/ecmean/interface',
            'target_path': 'diagnostics/ecmean'
        },
        {
            'config_file': 'config_ecmean_cli.yaml',
            'source_path': 'config/diagnostics/ecmean',
            'target_path': 'diagnostics/ecmean'
        },
        {
            'config_file': 'atm_mask_r100.nc',
            'source_path': 'config/diagnostics/ecmean/data',
            'target_path': 'diagnostics/ecmean'
        },
        {
            'config_file': 'cell_area_r100.nc',
            'source_path': 'config/diagnostics/ecmean/data',
            'target_path': 'diagnostics/ecmean'
        },
        {
            'config_file': 'oce_mask_r100.nc',
            'source_path': 'config/diagnostics/ecmean/data',
            'target_path': 'diagnostics/ecmean'
        }
    ],
    'timeseries': [
        {
            'config_file': 'regions.yaml',
            'source_path': 'config/diagnostics/timeseries/interface',
            'target_path': 'diagnostics/timeseries/interface'
        },
        {
            'config_file': 'config_seasonalcycles_atm.yaml',
            'source_path': 'config/diagnostics/timeseries',
            'target_path': 'diagnostics/timeseries'
        },
        {
            'config_file': 'config_timeseries_atm.yaml',
            'source_path': 'config/diagnostics/timeseries',
            'target_path': 'diagnostics/timeseries'
        },
        {
            'config_file': 'config_timeseries_oce.yaml',
            'source_path': 'config/diagnostics/timeseries',
            'target_path': 'diagnostics/timeseries'
        }
    ],
    'ocean3d': [
        {
            'config_file': 'regions.yaml',
            'source_path': 'diagnostics/ocean3d/config',
            'target_path': 'diagnostics/ocean3d/config'
        },
        {
            'config_file': 'config.circulation.yaml',
            'source_path': 'diagnostics/ocean3d/cli',
            'target_path': 'diagnostics/ocean3d/cli'
        },
        {
            'config_file': 'config.drift.yaml',
            'source_path': 'diagnostics/ocean3d/cli',
            'target_path': 'diagnostics/ocean3d/cli'
        },
        {
            'config_file': 'config.yaml',
            'source_path': 'diagnostics/ocean3d/cli',
            'target_path': 'diagnostics/ocean3d/cli'
        }
    ],
    'radiation': [
        {
            'config_file': 'radiation_config.yml',
            'source_path': 'diagnostics/radiation/cli/config',
            'target_path': 'diagnostics/radiation/cli'
        },
        {
            'config_file': 'config_radiation-boxplots.yaml',
            'source_path': 'config/diagnostics/radiation',
            'target_path': 'diagnostics/radiation/cli'
        },
        {
            'config_file': 'config_radiation-biases.yaml',
            'source_path': 'config/diagnostics/radiation',
            'target_path': 'diagnostics/radiation/cli'
        },
        {
            'config_file': 'config_radiation-timeseries.yaml',
            'source_path': 'config/diagnostics/radiation',
            'target_path': 'diagnostics/radiation/cli'
        },
    ],
    'seaice': [
        {
            'config_file': 'regions_definition.yaml',
            'source_path': 'diagnostics/seaice/config',
            'target_path': 'diagnostics/seaice/config'
        },
        {
            'config_file': 'config_Concentration.yaml',
            'source_path': 'diagnostics/seaice/cli',
            'target_path': 'diagnostics/seaice/cli'
        },
        {
            'config_file': 'config_Extent.yaml',
            'source_path': 'diagnostics/seaice/cli',
            'target_path': 'diagnostics/seaice/cli'
        },
        {
            'config_file': 'config_Thickness.yaml',
            'source_path': 'diagnostics/seaice/cli',
            'target_path': 'diagnostics/seaice/cli'
        },
        {
            'config_file': 'config_Volume.yaml',
            'source_path': 'diagnostics/seaice/cli',
            'target_path': 'diagnostics/seaice/cli'
        }
    ],
    'ssh': [
        {
            'config_file': 'config.yaml',
            'source_path': 'diagnostics/ssh',
            'target_path': 'diagnostics/ssh/config'
        },
        {
            'config_file': 'config.yaml',
            'source_path': 'diagnostics/ssh/cli',
            'target_path': 'diagnostics/ssh/cli'
        },
    ],
    'teleconnections': [
        {
            'config_file': 'teleconnections-ci.yaml',
            'source_path': 'config/diagnostics/teleconnections/interface',
            'target_path': 'diagnostics/teleconnections/config'
        },
        {
            'config_file': 'teleconnections-destine.yaml',
            'source_path': 'config/diagnostics/teleconnections/interface',
            'target_path': 'diagnostics/teleconnections/config'
        },
        {
            'config_file': 'teleconnections-netgems3.yaml',
            'source_path': 'config/diagnostics/teleconnections/interface',
            'target_path': 'diagnostics/teleconnections/config'
        },
        {
            'config_file': 'cli_config_atm.yaml',
            'source_path': 'config/diagnostics/teleconnections',
            'target_path': 'diagnostics/teleconnections'
        },
        {
            'config_file': 'cli_config_oce.yaml',
            'source_path': 'config/diagnostics/teleconnections',
            'target_path': 'diagnostics/teleconnections'
        },
        {
            'config_file': 'config_bootstrap.yaml',
            'source_path': 'config/diagnostics/teleconnections',
            'target_path': 'diagnostics/teleconnections'
        },
    ],
    'tropical_cyclones': [
        {
            'config_file': 'config_tcs_cli.yaml',
            'source_path': 'diagnostics/tropical_cyclones/cli',
            'target_path': 'diagnostics/tropical_cyclones/cli'
        },
    ],
    'tropical_rainfall': [
        {
            'config_file': 'config-tropical-rainfall.yml',
            'source_path': 'diagnostics/tropical_rainfall/tropical_rainfall',
            'target_path': 'diagnostics/tropical_rainfall/config'
        },
        {
            'config_file': 'cli_config_trop_rainfall.yml',
            'source_path': 'diagnostics/tropical_rainfall/cli',
            'target_path': 'diagnostics/tropical_rainfall/cli'
        }
    ],
    # Add other diagnostic configurations here
}
