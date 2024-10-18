import aqua


class ETCCDI_daily ():

    def __init__(self, Reader: aqua.Reader = None, index: str = None, year: int = None):
        """
        Initialize the ETCCDI_daily class.

        Arguments:
            Reader (aqua.Reader):  A Reader object. Defaults to None.
            index (str):  The ETCCDI index to be calculated. Defaults to None.
            year (int, opt):  The year to be analyzed. Defaults to None.
        """

        if Reader is None or not isinstance(Reader, aqua.Reader):
            raise ValueError("Reader is not provided or not a aqua.Reader object.")
        else:
            self.Reader = Reader

        self.catalog = Reader.catalog
        self.model = Reader.model
        self.exp = Reader.exp
        self.source = Reader.source
