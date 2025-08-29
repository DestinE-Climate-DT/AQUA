from typing import Optional, Union

DEFAULT_REALIZATION = 'r1'  # Default realization if not specified

def format_realization(realization: Optional[str | int | list | None] = None) -> Union[str, list]:
    """
    Format the realization string by prepending 'r' if it is a digit.

    Args:
        realization (str | int | list | None): The realization value. Can be:
            - str/int: Single realization value
            - list: List of realization values
            - None: Returns default realization

    Returns:
        str | list: Formatted realization string or list of formatted strings.
    """
    if not realization:
        return DEFAULT_REALIZATION
    if isinstance(realization, list):
        for i, r in enumerate(realization):
            if not r:
                realization[i] = DEFAULT_REALIZATION
            else:
                realization[i] = f'r{r}' if str(r).isdigit() else str(r)
        return realization
    if isinstance(realization, (int, str)):
        return f'r{realization}' if str(realization).isdigit() else str(realization)
