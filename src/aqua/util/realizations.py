def format_realization(realization: int | str = None) -> str:
    """
    Format the realization as r<realization> if int, leave the string
    otherwise and set r1 if nothing is given.
    To be used in folder or filename creation.
    If realization is None or empty, it defaults to "r1".

    Args:
        realization (int | str, optional): the realization, either as an integer or string.
    
    Returns:
        str: the realization formatted to be used in folder or filename creation.
    """
    if not realization:
        return "r1"
    if isinstance(realization, int) or (isinstance(realization, str) and realization.isdigit()):
        return f"r{realization}"
    return realization
