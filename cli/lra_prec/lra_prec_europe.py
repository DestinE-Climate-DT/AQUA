import argparse
from aqua import LRAgenerator

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process LRA generation parameters.")
    parser.add_argument('--var', type=str, required=True, help="Variable name to extract")
    parser.add_argument('--catalog', type=str, required=True, help="Catalog name")
    parser.add_argument('--model', type=str, required=True, help="Model name")
    parser.add_argument('--exp', type=str, required=True, help="Experiment name")
    parser.add_argument('--source', type=str, required=True, help="Source name")
    parser.add_argument('--regrid', type=str, required=True, help="Target grid")
    parser.add_argument('--freq', type=str, required=True, help="Frequency of the data")

    args = parser.parse_args()
    
    varname = args.var
    model = args.model
    exp = args.exp
    source = args.source
    catalog = args.catalog
    regrid= args.regrid
    frequency = args.freq

    print(f"Generating LRA for {varname} for {model} {exp} {source} from {catalog}")
    lra = LRAgenerator(
                    catalog=catalog, model=model, exp=exp, source=source,
                    var=varname, resolution=regrid, stat='mean', drop=True,
                    frequency=frequency, fix=True, nproc=12,
                    outdir="/scratch/project_462000911/mnurisso/prec_italy_new_selection", tmpdir="/scratch/project_462000911/mnurisso/lra_tmp",
                    loglevel="DEBUG", definitive=True, compact="cdo",
                    region={'name': 'Europe', 'lon': (-30, 40), 'lat': (25, 70)})
    print("---CHECK------")
    lra.check_integrity(varname)
    print("---RETRIEVE------")
    lra.retrieve()
    print("---GENERATE------")
    lra.generate_lra()
