from aqua import catalogue

"""Dummy diagnostic for testing purposes"""

class DummyDiagnostic():
    """Dummy diagnostic class"""
    def __init__(self, option=None, configdir=None):
        """The DummyDiagnostic constructor."""

        self.option = option
        self.configdir = configdir

    def run(self):
        """The run method."""
        
        cat = catalogue(configdir=self.configdir)
        print(cat)
