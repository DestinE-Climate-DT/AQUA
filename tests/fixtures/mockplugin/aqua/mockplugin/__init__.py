# Minimal stand-in of a project plugin following the plugin contract v2 (see aqua.core.console.components)
__version__ = "0.1.0"


def get_install_dirs():
    return {
        "contract": 2,
        "project": ["analysis"],
        "exclusive": ["mock_flat"],
        "shared": ["grids", "tools", "templates"],
    }
