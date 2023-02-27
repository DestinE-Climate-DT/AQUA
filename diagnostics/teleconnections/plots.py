'''
This module contains simple functions for data plotting.
'''
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import xarray as xr

def set_layout(fig, ax,title=None,xlabel=None,ylabel=None,xlog=False,ylog=False):
    """
    Set the layout of the plot

    Args:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
        title (str,opt):        title of the plot
        xlabel (str,opt):       label of the x axis
        ylabel (str,opt):       label of the y axis
        xlog (bool,opt):        enable or disable x axis log scale, default is False
        ylog (bool,opt):        enable or disable y axis log scale, default is False

    Returns:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
    """
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if xlog:
        ax.set_xscale('symlog')
    if ylog:
        ax.set_yscale('symlog')
    return fig, ax

def cor_plot(indx,field,projection_type='PlateCarree',plot=True):
    """
    Evaluate and plot correlation map of a teleconnection index 
    and a DataArray field

    Args:
        indx (DataArray):       index DataArray
        field (DataArray):      field DataArray
        projection_type (str):  projection style for cartopy
                                If a wrong one is provided, it will fall back
                                to PlateCarree
        plot (bool):            enable or disable the plot output, true by default
    
    Returns:
        reg (DataArray): DataArray for correlation map
    """
    projection_types = {
        'PlateCarree': ccrs.PlateCarree(),
        'LambertConformal': ccrs.LambertConformal(),
        'Mercator': ccrs.Mercator()
    }

    # 1. -- Evaluate the map --
    cor = xr.corr(indx,field, dim="time")

    proj = projection_types.get(projection_type, ccrs.PlateCarree())

    if plot:
        fig, ax = plt.subplots(subplot_kw={'projection': proj},figsize=(8,4))
        ax.set_xlabel('longitude')
        ax.set_ylabel('latitude')
        ax.coastlines()
        cor.plot(ax=ax)

    return cor

def hovmoller_plot_test(infile,dim='lon',outputdir=None,title=None,
                   xlabel=None,ylabel=None,contour=True,xlog=False,ylog=False):
    '''
    Args:
        infile (DataArray):     DataArray to be plot
        dim (str,opt):          dimension to be averaged over, default is 'lon'
        outputdir (str,opt):    directory to save the plot
        title (str,opt):        title of the plot
        xlabel (str,opt):       label of the x axis
        ylabel (str,opt):       label of the y axis
        contour (bool,opt):     enable or disable contour plot, default is True
        xlog (bool,opt):        enable or disable x axis log scale, default is False
        ylog (bool,opt):        enable or disable y axis log scale, default is False
    
    Returns:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
    '''
    infile_mean = infile.mean(dim=dim,keep_attrs=True)

    fig, ax = plt.subplots()

    # Contour or pcolormesh plot
    if contour:
        im = ax.contourf(infile_mean.coords['time'], infile_mean.coords[infile_mean.dims[-1]], infile_mean.T)
    else:
        im = ax.pcolormesh(infile_mean.coords['time'], infile_mean.coords[infile_mean.dims[-1]], infile_mean.T)
        
    # Colorbar
    try:
        cbar_label = infile_mean.name
        cbar_label += ' [' + infile_mean.units + ']'
        print(cbar_label)
        plt.colorbar(im, ax=ax, label=cbar_label)
    except AttributeError:
        plt.colorbar(im, ax=ax, label=infile_mean.name)
    
    set_layout(fig,ax,title=title,xlabel=xlabel,ylabel=ylabel,xlog=xlog,ylog=ylog)
    
    # Custom labels if provided
    if xlabel == None:
        ax.set_xlabel('time')
    if ylabel == None:
        ax.set_ylabel(infile_mean.dims[-1])
    if title == None:
        ax.set_title(f'Hovmoller Plot ({dim} mean)')
    
    return fig, ax

def index_plot(indx,title=None,xlabel=None,ylabel=None,xlog=False,
               ylog=False,save=False,outputdir='./',filename='index.png'):
    """
    Index plot together with a black line at indx=0

    Args:
        indx (DataArray): Index DataArray
        title (str,opt):        title of the plot
        xlabel (str,opt):       label of the x axis
        ylabel (str,opt):       label of the y axis
        xlog (bool,opt):        enable or disable x axis log scale, 
                                default is False
        ylog (bool,opt):        enable or disable y axis log scale, 
                                default is False
        save (bool,opt):        enable or disable saving the plot,
                                default is False
        outputdir (str,opt):    directory to save the plot
    
    Returns:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
    """
    # 1. -- Generate the figure --
    fig, ax = plt.subplots(figsize=(12, 8))

    # 2. -- Plot the index --
    plt.fill_between(indx.time,indx.values,where=indx.values>=0, step="pre", 
                     alpha=0.6,color='red')
    plt.fill_between(indx.time,indx.values,where=indx.values<0, step="pre",
                     alpha=0.6,color='blue')
    indx.plot.step(ax=ax,color='black',alpha=0.8)

    ax.hlines(y=0,xmin=min(indx['time']),xmax=max(indx['time']),color='black')

    # 3. -- Set the layout --
    set_layout(fig,ax,title=title,xlabel=xlabel,ylabel=ylabel,xlog=xlog,ylog=ylog)

    # 4. -- Save the figure --
    if save:
        fig.savefig(outputdir + filename)

    return fig, ax

def reg_plot(indx,field,plot=True,projection_type='PlateCarree',
             title=None,xlabel=None,ylabel=None,xlog=False,ylog=False,
             contour=False,levels=8,save=False,outputdir='./',filename='reg.png'):
    """
    Evaluate and plot regression map of a teleconnection index 
    and a DataArray field

    Args:
        indx (DataArray):       index DataArray
        field (DataArray):      field DataArray
        projection_type (str):  projection style for cartopy
                                If a wrong one is provided, it will fall back
                                to PlateCarree
        plot (bool):            enable or disable the plot output, true by default
        title (str,opt):        title of the plot
        xlabel (str,opt):       label of the x axis
        ylabel (str,opt):       label of the y axis
        xlog (bool,opt):        enable or disable x axis log scale, default is False
        ylog (bool,opt):        enable or disable y axis log scale, default is False
        contour (bool,opt):     enable or disable contour plot, default is False
        levels (int,opt):       number of contour levels, default is 8
        save (bool,opt):        enable or disable saving the plot, default is False
        outputdir (str,opt):    directory to save the plot
        filename (str,opt):     filename of the plot
    
    Returns:
        reg (DataArray):        DataArray for regression map
        fig (Figure,opt):       Figure object
        ax (Axes,opt):          Axes object
    """
    # 1. -- List of accepted projection maps --
    projection_types = {
        'PlateCarree': ccrs.PlateCarree(),
        'LambertConformal': ccrs.LambertConformal(),
        'Mercator': ccrs.Mercator()
    }

    # 2. -- Evaluate the regression --
    reg = xr.cov(indx, field, dim="time")/indx.var(dim='time',skipna=True).values
    
    # 3. -- Plot the regression map --
    proj = projection_types.get(projection_type, ccrs.PlateCarree())

    if plot:
        fig, ax = plt.subplots(subplot_kw={'projection': proj},figsize=(8,4))
        
        ax.coastlines()
        if contour:
            reg.plot.contourf(ax=ax, transform=ccrs.PlateCarree(),levels=levels)
        else:
            reg.plot(ax=ax, transform=ccrs.PlateCarree())
        
        set_layout(fig, ax, title=title, xlabel=xlabel, ylabel=ylabel, xlog=xlog, ylog=ylog)
        
        # 4. -- Save the figure --
        if save:
            fig.savefig(outputdir + filename)

        return reg, fig, ax
    else:
        return reg