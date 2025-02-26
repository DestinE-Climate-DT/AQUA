import aqua
from aqua.logger import log_configure


class ETCCDI_daily ():

    def __init__(self, Reader: aqua.Reader = None, year: int = None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the ETCCDI_daily class.
        This is a Mixin class for each individual ETCCDI index based on daily data.

        Arguments:
            Reader (aqua.Reader):  A Reader object. Defaults to None.
            year (int, opt):  The year to be analyzed. Defaults to None.
            loglevel (str, opt):  The logging level. Defaults to 'WARNING'.
        """

        if Reader is None or not isinstance(Reader, aqua.Reader):
            raise ValueError("Reader is not provided or not a aqua.Reader object.")
        else:
            self.Reader = Reader

        self.logger = log_configure(loglevel, 'ETCCDI_daily')

        if year is None:
            raise ValueError("Year is not provided.")
        else:
            self.year = year
            self.logger.debug(f"Year: {self.year}")

        self.aggregation = 'D'

        self.catalog = Reader.catalog
        self.model = Reader.model
        self.exp = Reader.exp
        self.source = Reader.source

        self.startdate = f"{self.year}0101"
        self.enddate = f"{self.year}1231"
        self.month = int(self.startdate[4:6])

        # set the streaming
        self.Reader.stream(startdate=self.startdate, enddate=self.enddate, aggregation=self.aggregation)

    # def monthy_loop(self):
    #     """
    #     Loop through each month of the year and calculate the ETCCDI index.
    #     """

    #     for month in range(1, 13):
    #         self.month = month
    #         self.startdate = f"{self.year}{month:02d}01"
    #         self.enddate = f"{self.year}{month:02d}31"
    #         self.logger.debug(f"Month: {self.month} Start Date: {self.startdate} End Date: {self.enddate}")

    #         self.Reader.stream(startdate=self.startdate, enddate=self.enddate, aggregation=self.aggregation)
    #         self.calculate_index()