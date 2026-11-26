# Minimal stand-in of a project plugin following the plugin contract v2 (see aqua.core.console.components)
__version__ = "0.1.0"


def get_install_dirs():
    return {
        "contract": 2,
        "config": ["analysis"],
        "flat": ["mock_flat"],
        "templates": ["mock_templates"],
        "files": ["grids"],
    }
