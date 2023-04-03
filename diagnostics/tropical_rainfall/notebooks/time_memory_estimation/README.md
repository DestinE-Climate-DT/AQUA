# Expected memory usage and adaptive data loading

## Expected memory usage ``` expected_total_memory_usage() ```

We can estimate the amount of memory usage by diagnostic by measuring the memory consumption of a small part of the dataset.
Let's call the tested part of the entire dataset a chunk.

The algorithm is following:

 - Before we ran the chunk diagnostic, we measured the amount of used virtual memory (``` VmRSS_1 ```) by a current process in the ``` /proc/self/status ``` file.

 - Run any diagnostic of a chunk. 

 - Measure the amount of used virtual memory (``` VmRSS_2 ```) by the current process in the ``` /proc/self/status ``` file. 

 - Total memory consumption by diagnostic is a difference between the amount of used virtual memory (``` VmRSS_2 - VmRSS_1 ```).

 - The formula can find memory consumption by the entire dataset:

 $$ {\text{Total memory consumption}} =  \text{Memory Consumption by single Object  x }  \text{ Total Size of Dataset} $$ 
 
 $$ {\text{Total memory consumption}} =   \frac{\text{Memory Consumption by chunk }}{\text{ Size of chunk }}   \text{ x Total Size of Dataset}$$


## Adaptive data loading ``` adaptive_data_load() ```

By adaptive data loading, we assume that we load only such part of the dataset, which
 -  fit the amount of available memory
 - fit only specific/specified amounts of memory.

Let's assume that the amount of memory that we would like to use equal 

$$ \text{Availibale Memory} =  \text{Total Availibale Memory   x }  \text{Maximum Percent of Memory} $$


We can find the amount of available memory (``` MemAvailable ```) on a SPECIFIC device from ``` /proc/meminfo ```.

Then we can find the dataset size, which we can fit into memory by formula

$$   \text{Fittable Size} = min( \frac{\text{ Availibale Memory }}{\text{ Mem Consumed by Single Object } }, \text{ . Total Size of Dataset}) $$


If we divide \text{Fittable Size/Total Size of Dataset}, we can find the number of timesteps we can load into the memory. 
