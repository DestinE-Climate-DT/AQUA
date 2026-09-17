from intake.readers.datatypes import FileData


class Icechunk(FileData):
    structure = {"array", "hierarchy"}
    # contains = {"snapshots"}

    def __init__(
        self,
        url,
        storage_options: dict | None = None,
        branch: str | None = "main",
        metadata: dict | None = None,
    ):
        """
        Args:
            url (str): File path or URL to the IceChunk repository.
            storage_options (dict, optional): Additional keyword arguments
                forwarded to the underlying store. Defaults to None.
            branch (str, optional): Branch or tag of the IceChunk repository
                to open. Defaults to "main" (use main branch).
            metadata (dict, optional): Metadata associated with the data source.
                Defaults to None.
        """
        self.url = url
        self.branch = branch
        self.storage_options = storage_options
        super().__init__(url=url, storage_options=storage_options, metadata=metadata)
