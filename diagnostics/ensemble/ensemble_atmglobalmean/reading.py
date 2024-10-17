


from aqua import Reader



reader = Reader(model='aqua-atmglobalmean',exp='IFS-NEMO-hist',source='t2',areas=False)

data = reader.retrieve()
