## Running experiments

In order to run the experiments one should download the dataset file from 
[repository](https://bitbucket.org/cross-uel/flock-pattern-algorithms/downloads) and [run directly 
the executable](#markdown-header-running-directly-the-executable) in the `build/` folder, 
passing the correct arguments, or [use the script](#markdown-header-running-through-script) that
 is in the `bin/` folder.


### Running directly the executable


After compiling the project, the executable `build/flocks` accepts the following parameters:

***-m*** (_method_) Algorithm to run: _BFE or PSI_;

***-s*** (_size_) Minimum number of entities to form a flock (_µ > 1 (µ ∈ N)_);

***-d*** (_distance_) Disk diameter (_ε > 0_)

***-l*** (_length_) Number of consecutive time instances to form a flock (_δ > 1 (δ ∈ N)_);

***-f*** (_dataset_) Dataset file;

***-p*** (_metrics_) Metrics log file.


The following example executes the BFE algorithm, in the buses dataset located in the `.
./flocks/data` folder with _Flock(5, 0.8, 10)_, outputting the metrics in the `logs/metrics.log` 
file.

```
:::bash
build/flocks -m BFE -s 5 -d 0.8 -l 10 -f ../flocks/data/buses_by_time.txt -p logs/metrics.log 
```

### Running through script


Rather than calling the directly the executable and passing all arguments to execute one simple 
experiment, it is possible to run the script `bin/run_buses.sh` with already predefined 
parameters for a set of consecutive experiments, and is called as follow:

```
:::bash
bin/run_buses.sh
```

In this script, in its CONFIGURATION section, these parameters can be changed to set up other 
experiments. One important observation is that in this script the default folder to the datasets 
(`DATA_DIR`) is `../flocks/data`.


### Example


In ``samples/buses`` there are the output files from running the `bin/run_buses.sh` script.
The three _.eps_ files represents the experiments output values. The `dist_buses.eps` shows the 
variation of the distance (ε) variation, `len_buses.eps` the length (δ) variation and 
`nro_buses.eps` the minimum trajectories number (µ) variation.

Obs. These graphics can be built parsing the logs values. Notice there are small divergences in 
the results when compared to results shown in published papers. This is due to some refactoring 
work on original source code and different running environment.
