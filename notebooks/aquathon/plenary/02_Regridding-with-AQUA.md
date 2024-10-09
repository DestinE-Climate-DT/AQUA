# Regridding with AQUA

[TOC]

## Regridding with AQUA

In this section we will explore the usage of the regrid method and how to regrid both horizontally and vertically the data retrieved with AQUA.

The regrid functionalities rely on the [smmregrid package](https://github.com/jhardenberg/smmregrid), a publicly available package that is available on pypi. It allows to use pre-computed weights (or to build them if not available) to perform sparse metrix multiplication to regrid data. The weights and areas file evaluated with this tool (that internally makes use of CDO for the initial file generation) must be stored in a folder, that in lumi and levante is a folder shared among a project. This allow to not have to compute weights for most of the standard grids used.

The grids are defined in specific yaml files in the grids folder of the installation folder. They contain basic info for the Reader about how to use the provided grid file:

```yaml
grids:
  hpz5-nested:
    space_coord: ["ncells"]
    vert_coord: ["2d"]
    path: '{{ grids }}/HealPix/hpz5_nested_atm.nc'
```

Also in this case the AQUA catalog is built to handle these details in the yaml catalog files, in order to make for the final user totally seamless the regrid for any different source.

:::warning
If you have to generate weights, please consider the possibility to use a computational node, since the operation can be very time and memory expensive.
:::

:::info
The notebooks with working examples are available in your AQUA repository and specifically in the folder $AQUA/notebooks/aquathon/plenary
:::
