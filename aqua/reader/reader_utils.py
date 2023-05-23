"""Common functions for the Reader"""


def check_catalog_source(cat, model, exp, source, name="dictionary"):
    """
    Check the entries of a nested dictionary based on the model/exp/source structure
    and return an updated source. The name argument can be used for proper printing.

    Args:
        cat (dict): The nested dictionary containing the catalog information.
        model (str): The model ID to check in the catalog.
        exp (str): The experiment ID to check in the catalog.
        source (str): The source ID to check in the catalog.
        name (str, optional): The name used for printing. Defaults to "dictionary".

    Returns:
        str: An updated source ID. If the source is not specified, "default"
            is chosen, or, if missing, the first source.
    """

    if model not in cat:
        raise KeyError(f"Model {model} not found in {name}.")
    if exp not in cat[model]:
        raise KeyError(f"Experiment {exp} not found in {name} for model {model}.")
    if not cat[model][exp].keys():
        raise KeyError(f"Experiment {exp} in {name} for model {model} has no sources")

    if source:
        if source not in cat[model][exp]:
            if "default" not in cat[model][exp]:
                raise KeyError(f"Source {source} of experiment {exp} "
                               f"not found in {name} for model {model}.")
            source = "default"
    else:
        source = list(cat[model][exp].keys())[0]  # take first source if none provided

    return source
