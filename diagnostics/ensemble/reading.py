from aqua import Reader

catalog = 'diagnosis'
model = 'aqua-timeseries'
exp = 'IFS-NEMO-hist'
source = 't2m-mon'
startdate = '1990-01-01'
enddate = '2000-12-31'
var = 't2'

reader = Reader(model=model,exp=exp,source=source,startdate=startdate,enddate=enddate,areas=False)

data = reader.retrieve()

print(data)
