diagnostic_config = {
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
    ]
}
