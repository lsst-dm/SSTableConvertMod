# HOWTO: SSTableConvertMod

*As of 3/20/2021*

## Installation

Clone this repository to your computer,

https://github.com/lsst-dm/SSTableConvertMod.git


Make sure you have the following dependencies also installed,
- numpy
- pandas
- astropy
- sbpy
- click
- cachetools
- *and many more ...*

#### Note,

You may want to add an "outputs" folder to your workspace to simplify things. Make sure this is **right outside** of the github repo folder.

## Running things

These need to be done **IN ORDER** for things to run, since each table feeds off of each other.

Examples of actual function calls with filenames are available in the README file. They may be from older versions of this program, but they are good examples to follow *loosely*.

### 1. DIASource

First in a terminal run the server call,

```
python -m SSTableConvertMod cli-server [name for dia index file].sql
```

In a **separate terminal** run the `dia` call,

```
python -m SSTableConvertMod dia --skip_rows=1 [input_dia_file_name] [output_dia_filename].csv
```
I believe you can process as many files as you want while the server is running, *but this has not been tested*.

After processing all the files needed, ctrl-c the server command from the first terminal. You do not need this terminal anymore.

### 2. MPCORB

Now we can run this for orbit files, thus in a terminal run this command,

```
python -m SSTableConvertMod mpcorb --skip_rows=2  [{glob of orbit files} e.g. "/path/to/file/*.s3m"] [output_mpc_filename].csv
```

This will also generate a "sidecar" file in the same path as the output file and it will be named as, `[output_mpc_filename].sidecar`, you will use this later.

### 3. SSObject

This is where it gets tricky, *pay attention*.

For this call we use the database file for dia which is named, `[name for dia index file].sql`, and the sidecar file for mpcorb discussed earlier, thus we call in the terminal,

```
python -m SSTableConvertMod ssobject --skip_rows=1 [path for dia index file].sql [mpcorb_filepath].sidecar [ssobject_output_filename].csv
```
This should work for right now, **but this may be changed in the future.**

### 4. SSSource

Just run this for an input simulation file, example: `/epyc/projects/jpl_survey_sim/s3c/S0.dat.csv` thus calling the function,

```
python -m SSTableConvertMod sssource --skip_rows=1 [input_simulation_file] [output_sssource_file].csv
```
---

Voila! You have just used SSTableConvertMod. If you have anymore questions, open a issue in the repo or contact me (berres2002) or anyone else who contributes to this server!
