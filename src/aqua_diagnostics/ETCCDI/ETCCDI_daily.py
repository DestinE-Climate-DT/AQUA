import aqua
from aqua.logger import log_configure


available_indices = ['fd', 'id', 'su', 'tr']


class ETCCDI_daily ():

    def __init__(self, Reader: aqua.Reader = None, index: str = None, year: int = None,
                 longname: str = None, var: str = None,
                 condition: str = None, condition_sign: str = None,
                 statistic: str = None, units: str = 'days',
                 cmap: str = 'Blues', loglevel: str = 'WARNING'):
        """
        Initialize the ETCCDI_daily class.

        Arguments:
            Reader (aqua.Reader):  A Reader object. Defaults to None.
            index (str):  The ETCCDI index to be calculated. Defaults to None.
            year (int, opt):  The year to be analyzed. Defaults to None.
            longname (str, opt):  The long name of the ETCCDI index. Defaults to None.
            var (str):  The variable to be analyzed. Defaults to None.
            condition (str):  The condition to be applied. Defaults to None.
            condition_sign (str):  The condition sign to be applied. Defaults to None.
            statistic (str):  The statistic to be applied. Defaults to None.
            units (str):  The units of the ETCCDI index. Defaults to 'days'.
            cmap (str):  The colormap to be used. Defaults to 'Blues'.
        """

        if Reader is None or not isinstance(Reader, aqua.Reader):
            raise ValueError("Reader is not provided or not a aqua.Reader object.")
        else:
            self.Reader = Reader

        if index is None or index not in available_indices:
            raise ValueError(f"Index is not provided or not a valid ETCCDI index, available indices: {available_indices}")
        else:
            self.index = index

        if year is None:
            raise ValueError("Year is not provided.")
        else:
            self.year = year

        if var is None:
            raise ValueError("Variable is not provided.")
        else:
            self.var = var

        if condition is None:
            raise ValueError("Condition is not provided.")
        else:
            self.condition = condition

        if condition_sign is None:
            raise ValueError("Condition sign is not provided.")
        else:
            self.condition_sign = condition_sign

        if statistic is None:
            raise ValueError("Statistic is not provided.")
        else:
            self.statistic = statistic

        self.aggregation = 'D'
        self.units = units
        self.cmap = cmap

        if longname is None:
            self.longname = f"ETCCDI_{index}"
        else:
            self.longname = longname

        logname = f"ETCCDI_{index}"
        self.logger = log_configure(loglevel, logname)
        self.logger.info(f"ETCCDI index: {index}, valid configuration")

        self.catalog = Reader.catalog
        self.model = Reader.model
        self.exp = Reader.exp
        self.source = Reader.source

        self.startdate = f"{self.year}0101"
        self.enddate = f"{self.year}1231"
        self.month = int(self.startdate[4:6])

        # set the streaming
        self.Reader.stream(startdate=self.startdate, enddate=self.enddate, aggregation=self.aggregation)