diagnostic_config = {
    'atmglobalmean': [
        {
            'config_file': 'atm_mean_bias_config.yaml',
            'source_path': 'diagnostics/atmglobalmean/cli/config',
            'target_path': 'diagnostics/atmglobalmean/cli'
        }
    ],
    'ecmean': [
        {
            'config_file': 'ecmean_config_destine-v1-levante.yml',
            'source_path': 'diagnostics/ecmean/config',
            'target_path': 'diagnostics/ecmean/config'
        },
        {
            'config_file': 'ecmean_config_destine-v1.yml',
            'source_path': 'diagnostics/ecmean/config',
            'target_path': 'diagnostics/ecmean/config'
        },
        {
            'config_file': 'interface_AQUA_destine-v1.yml',
            'source_path': 'diagnostics/ecmean/config',
            'target_path': 'diagnostics/ecmean/config'
        },
        {
            'config_file': 'config_ecmean_cli.yaml',
            'source_path': 'diagnostics/ecmean/cli',
            'target_path': 'diagnostics/ecmean/cli'
        }
    ],
    'global_time_series': [
        {
            'config_file': 'config_seasonal_cycles_atm.yaml',
            'source_path': 'diagnostics/global_time_series/cli',
            'target_path': 'diagnostics/global_time_series/cli'
        },
        {
            'config_file': 'config_time_series_atm.yaml',
            'source_path': 'diagnostics/global_time_series/cli',
            'target_path': 'diagnostics/global_time_series/cli'
        },
        {
            'config_file': 'config_time_series_oce.yaml',
            'source_path': 'diagnostics/global_time_series/cli',
            'target_path': 'diagnostics/global_time_series/cli'
        }
    ],
    'ocean3d': [
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
        }
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
            'source_path': 'diagnostics/teleconnections/config',
            'target_path': 'diagnostics/teleconnections/config'
        },
        {
            'config_file': 'teleconnections-destine.yaml',
            'source_path': 'diagnostics/teleconnections/config',
            'target_path': 'diagnostics/teleconnections/config'
        },
        {
            'config_file': 'teleconnections-netgems.yaml',
            'source_path': 'diagnostics/teleconnections/config',
            'target_path': 'diagnostics/teleconnections/config'
        },
        {
            'config_file': 'cli_config_atm.yaml',
            'source_path': 'diagnostics/teleconnections/cli',
            'target_path': 'diagnostics/teleconnections/cli'
        },
        {
            'config_file': 'cli_config_oce.yaml',
            'source_path': 'diagnostics/teleconnections/cli',
            'target_path': 'diagnostics/teleconnections/cli'
        },
        {
            'config_file': 'config_bootstrap.yaml',
            'source_path': 'diagnostics/teleconnections/cli',
            'target_path': 'diagnostics/teleconnections/cli'
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