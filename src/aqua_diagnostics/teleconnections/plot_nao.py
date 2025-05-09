from .base import PlotBaseMixin


class PlotNAO(PlotBaseMixin):

    def __init__(self, loglevel: str = 'WARNING'):
        super().__init__(loglevel=loglevel)

    def plot_index(self, indexes: list, thresh: float = 0.5):
        return super().plot_index(indexes, thresh=thresh)
