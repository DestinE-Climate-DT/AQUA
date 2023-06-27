import numpy as np

def time_interpreter(dataset):
    """Identifying unit of timestep in the Dataset

    Args:
        dataset (xarray):       The Dataset

    Returns:
        str:                    The unit of timestep in input Dataset
    """

    if dataset['time'].size==1:
        return 'False. Load more timesteps then one'
    try:
        if np.count_nonzero(dataset['time.second'] == dataset['time.second'][0]) == dataset.time.size:
            if np.count_nonzero(dataset['time.minute'] == dataset['time.minute'][0]) == dataset.time.size:
                if  np.count_nonzero(dataset['time.hour'] == dataset['time.hour'][0]) == dataset.time.size:
                    if np.count_nonzero(dataset['time.day'] == dataset['time.day'][0] ) == dataset.time.size or \
                        np.count_nonzero([dataset['time.day'][i] in [1, 28, 29, 30, 31] for i in range(0, len(dataset['time.day']))]) == dataset.time.size:

                        if np.count_nonzero(dataset['time.month'] == dataset['time.month'][0]) == dataset.time.size:
                            return 'Y'
                        else:
                            return 'M'
                    else:
                        return 'D'
                else:
                    timestep = dataset.time[1] - dataset.time[0]
                    n_hours = int(timestep/(60 * 60 * 10**9) )
                    return str(n_hours)+'H'
            else:
                timestep = dataset.time[1] - dataset.time[0]
                n_minutes = int(timestep/(60  * 10**9) )
                return str(n_minutes)+'m'
        else:
            return 1

    except KeyError and AttributeError:
        timestep = dataset.time[1] - dataset.time[0]
        if timestep >=28 and timestep <=31:
            return 'M'