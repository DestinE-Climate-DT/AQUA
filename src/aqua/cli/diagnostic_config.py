diagnostic_config = {
    'global_biases': [
        {
            'config_file': 'config_global_biases.yaml',
            'source_path': 'config/diagnostics/global_biases',
            'target_path': 'diagnostics/global_biases'
        },
    ],
    'lat_lon_profiles': [
        {
            'config_file': 'regions.yaml',
            'source_path': 'config/diagnostics/lat_lon_profiles/definitions',
            'target_path': 'diagnostics/lat_lon_profiles/definitions'
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
            'source_path': 'config/diagnostics/timeseries/definitions',
            'target_path': 'diagnostics/timeseries/definitions'
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
            'target_path': 'diagnostics/ocean3d/'
        },
        {
            'config_file': 'regions.yaml',
            'source_path': 'config/diagnostics/ocean3d/definitions',
            'target_path': 'diagnostics/ocean3d/definitions'
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
        },
        {
            'config_file': 'config_ocean_trends.yaml',
            'source_path': 'config/diagnostics/ocean3d',
            'target_path': 'diagnostics/ocean3d'
        },
        {
            'config_file': 'config_ocean_drift.yaml',
            'source_path': 'config/diagnostics/ocean3d',
            'target_path': 'diagnostics/ocean3d'
        }
    ],
    'radiation_surface': [
        {
            'config_file': 'config-radiation_surface-boxplots.yaml',
            'source_path': 'config/diagnostics/radiation_surface',
            'target_path': 'diagnostics/radiation_surface'
        },
        {
            'config_file': 'config-radiation_surface-biases.yaml',
            'source_path': 'config/diagnostics/radiation_surface',
            'target_path': 'diagnostics/radiation_surface'
        },
        {
            'config_file': 'config-radiation_surface-timeseries.yaml',
            'source_path': 'config/diagnostics/radiation_surface',
            'target_path': 'diagnostics/radiation_surface'
        }
    ],

    'radiation_toa': [
        {
            'config_file': 'config-radiation_toa-boxplots.yaml',
            'source_path': 'config/diagnostics/radiation_toa',
            'target_path': 'diagnostics/radiation_toa'
        },
        {
            'config_file': 'config-radiation_toa-biases.yaml',
            'source_path': 'config/diagnostics/radiation_toa',
            'target_path': 'diagnostics/radiation_toa'
        },
        {
            'config_file': 'config-radiation_toa-timeseries.yaml',
            'source_path': 'config/diagnostics/radiation_toa',
            'target_path': 'diagnostics/radiation_toa'
        },
    ],


    'seaice': [
        {
            'config_file': 'regions.yaml',
            'source_path': 'config/diagnostics/seaice/definitions',
            'target_path': 'diagnostics/seaice/definitions'
        },
        {
            'config_file': 'config_seaice.yaml',
            'source_path': 'config/diagnostics/seaice',
            'target_path': 'diagnostics/seaice'
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
            'source_path': 'config/diagnostics/teleconnections/definitions',
            'target_path': 'diagnostics/teleconnections/definitions'
        },
        {
            'config_file': 'teleconnections-destine.yaml',
            'source_path': 'config/diagnostics/teleconnections/definitions',
            'target_path': 'diagnostics/teleconnections/definitions'
        },
        {
            'config_file': 'config_teleconnections_atm.yaml',
            'source_path': 'config/diagnostics/teleconnections',
            'target_path': 'diagnostics/teleconnections'
        },
        {
            'config_file': 'config_teleconnections_oce.yaml',
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
    'ensemble': [
        {
            'config_file': 'config_global_2D_ensemble.yaml',
            'source_path': 'config/diagnostics/ensemble',
            'target_path': 'diagnostics/ensemble'
        },
        {
            'config_file': 'config_timeseries_ensemble.yaml',
            'source_path': 'config/diagnostics/ensemble',
            'target_path': 'diagnostics/ensemble'
        },
        {
            'config_file': 'config_zonalmean_ensemble.yaml',
            'source_path': 'config/diagnostics/ensemble',
            'target_path': 'diagnostics/ensemble'
        }
    ],  # Add other diagnostic configurations here
}
