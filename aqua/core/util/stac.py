"""STAC utilities shared by the intake STAC driver and the pystac backend."""


def select_asset_key(asset_roles: dict, asset: str | None = None, owner: str = None) -> str:
    """Select the key of a STAC asset.

    Args:
        asset_roles (dict): Mapping of asset keys to their list of roles.
        asset (str, optional): Requested asset key, or a role matching a single asset.
            If None, defaults to 'data', then to the only asset with role 'data', then to the first asset.
        owner (str, optional): Id of the item or collection owning the assets, used in error messages.

    Returns:
        str: The selected asset key.

    Raises:
        KeyError: If the requested asset is not found or matches multiple assets by role.
    """
    if asset is None:
        if "data" in asset_roles:
            return "data"
        data_role_assets = [k for k, roles in asset_roles.items() if "data" in roles]
        return data_role_assets[0] if len(data_role_assets) == 1 else next(iter(asset_roles))

    if asset in asset_roles:
        return asset
    # Check if asset name was supplied as a role (e.g. role 'data')
    role_matching = [k for k, roles in asset_roles.items() if asset in roles]
    if len(role_matching) == 1:
        return role_matching[0]
    if len(role_matching) > 1:
        raise KeyError(
            f"Asset '{asset}' not found in STAC item '{owner}'. "
            f"It matches multiple assets by role: {role_matching}. "
            f"Please specify one of: {list(asset_roles)}"
        )
    raise KeyError(f"Asset '{asset}' not found in STAC item '{owner}'. Available assets: {list(asset_roles)}")
