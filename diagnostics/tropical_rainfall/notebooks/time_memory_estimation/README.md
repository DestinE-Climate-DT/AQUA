# Expected memory usage and adaptive data loading

## Expected memory usage ``` expected_total_memory_usage() ```

We can estimate the amout of memory usage by diagnostic by mesuare of memory consumption of small part of dataset.
Lets call the tested part of entire dataset as a chunk.

The algorithmm is following:

 - Before we ran the diagnostic of chunk, we measure amout of used virtual memory (``` VmRSS_1 ```) by current process in ``` /proc/self/status ``` file. After the diagnostic i

 - Run any diagnostic of chunk  

 - Measure amout of used virtual memory (``` VmRSS_2 ```) by current process in ``` /proc/self/status ``` file. 

 - Total memory consumption by dianostic is a difference between amout of used virtual memory (``` VmRSS_2 - VmRSS_1 ```)

 - Memory consumption by entire dataset can be find by formula

$$ {\text{Total memory consumption}} =  \text{Memory Consumption by single Object} x  \text{Total Size of Dataset}  =  \frac{\text{Memory Consumption by chunk}}{\text{Size of chunk}} x  \text{Total Size of Dataset}$$


## Adaptive data loading ``` adaptive_data_load() ```

By adaptive data loading we assume that we load only such part of dataset, which
 -  fit amout of availibale memory
 - fit only certain/specified amout of memory

Lets assume that amount of memory which we would like to use equal 
$$ \text{Availibale Memory} =  \text{Total Availibale Memory} x \text{Maximum Percent of Memory} $$

We can find amount of availibale memory (``` MemAvailable ```) on a SPECIFIC device from ``` /proc/meminfo ```.

Then we can find the size of dataset which we can fit into memory by formula 

$$   \text{Fittable Size} = min( \frac{Availibale Memory}{Mem Consumed by Single Object}, \text{Total Size of Dataset}) $$


If we devide \text{Fittable Size/Total Size of Dataset} we can find the amout of timesteps which we can load into the memory. 
