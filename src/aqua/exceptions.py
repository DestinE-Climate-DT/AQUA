"""Module for custom exceptions used in AQUA"""


class NoDataError(Exception):
    """Exception raised when there is no data available"""

    def __init__(self, message="No data available"):
        self.message = message
        super().__init__(self.message)


class NoDiagnosticError(Exception):
    """Exception raised when there is no diagnostic selected"""

    def __init__(self, message="No diagnostic selected"):
        self.message = message
        super().__init__(self.message)


class NoEcCodesShortNameError(Exception):
    """Exception raised when there is no ecCodes shortName available"""

    def __init__(self, message="No ecCodes shortName available"):
        self.message = message
        super().__init__(self.message)


class NotEnoughDataError(Exception):
    """Exception raised when there is not enough data available"""

    def __init__(self, message="Not enough data available"):
        self.message = message
        super().__init__(self.message)


class NoObservationError(Exception):
    """Exception raised when there is no observation available"""

    def __init__(self, message="No observation available"):
        self.message = message
        super().__init__(self.message)


class NoRegridError(Exception):
    """Exception raised when no regrid is available"""

    def __init__(self, message="No regrid available"):
        self.message = message
        super().__init__(self.message)


class SetupCheckerFailed(Exception):
    """Exception raised when setup checker failed"""

    def __init__(self, message="Setup checker failed"):
        self.message = message
        super().__init__(self.message)
