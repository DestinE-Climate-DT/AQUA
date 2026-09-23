# Minimal stand-in mirroring the real aqua.diagnostics entry-point contract
MOCKPLUGIN_CONFIG_DIRECTORIES = ["mock_config"]
MOCKPLUGIN_TEMPLATE_DIRECTORIES = ["mock_templates"]


def get_install_dirs():
    return {"config": MOCKPLUGIN_CONFIG_DIRECTORIES, "templates": MOCKPLUGIN_TEMPLATE_DIRECTORIES}
