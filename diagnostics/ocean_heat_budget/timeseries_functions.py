"""Functions for global time series diagnostics.
"""
import matplotlib.pyplot as plt

def plot_time_series(var1, var2, var1_label, var2_label):
    fig, ax1 = plt.subplots()

    color1 = 'tab:red'
    ax1.set_xlabel('Time')
    ax1.set_ylabel(var1_label, color=color1)
    ax1.plot(var1.time, var1, color=color1, label=var1_label)
    ax1.tick_params(axis='y', labelcolor=color1)

    color2 = 'tab:blue'
    ax2 = ax1.twinx()
    ax2.set_ylabel(var2_label, color=color2)
    ax2.plot(var1.time[:-1], var2, color=color2, label=var2_label)
    ax2.tick_params(axis='y', labelcolor=color2)

    fig.tight_layout()
    fig.legend(loc='upper right')
    plt.show()