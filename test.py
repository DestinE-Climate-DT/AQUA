CLI


config:

    drop:
        run: true
        resolution: null
        stat: histogram


1. Drop (stat: histogram, resolution: null)
    result: model=same as entry, exp, source=generated one based on drop config block
2. ComputeClass
    ComputeClass(Diagnostic)(
        model=same as entry,
        exp=exp,
        source=source
    )
    .run()
    result: netcdf individual model
3. PlotClass
    PlotClass(
        datasets=datasets
    )


