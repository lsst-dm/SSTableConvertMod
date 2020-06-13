# Solar System Table Conversion

This repository is a runnable python package that contains sub commands for the following types of table creation.

- DiaSource
- MPCORB
- SSObject
- SSSource

An example of running dia source table creation is as follows

```
python -m SSTableConvertMod dia --skip_rows 1 /path/to/sim_file /path/to/output.csv
```

A list of the available sub commands can be output by executing

```
pthon -m SSTableConvertMod --help
```

Each of the sub commands have their own options and signatures that can be viewed using `--help` after the sub command name.

## Structure of the module

Base level apis are defined inside the base module and consist of a base schema object, a base file table object, and a builder class for table objects. These are named `TableSchema`, `FileTable`, and `FileTableBuilder` repectively. Each of these are abstract base classes that should not be used directly, but they do provide concrete implementations of shared functionality. Each class is implemented in such a way that children need only implement a few attributes to uniquely define themselves keeping boiler plate to a minimum (outside whatever method overloading is needed for a particular application)

A `FileTable` sublass should correspond to a sigle type of output (ie DIASource Table). The subclasses must contain the following class attributes

- schema - A subclass of `TableSchema` that defines the column names along with their corresponding python type
- index_columns - An iterable of column names (possibly empty) that defines what columns will be indexed if indexing is run
- builder - A subclass of `FileTableBuilder` that defines how a csv file representation of the FileTable will be constructed.

`FileTable`s are the in memory representation of the corresponding csv file, and can be used for row access in other applications (such as `FileTableBuilder`s of other `FileTables`). By default `FileTable`s use a file that has a memory mapping object so that the whole file does not need loaded into memory at one time. There is a special subclass called `FileTableInMem` that is used by `MPCORBFileTable` and `SSObjectFileTable` as the entire contents of the corresponding csv file should fit in memory at once.

Schemas for each of the concrete `FileTable` implementation can be found within the schemas directory. `TableSchema` objects not only define the columns in the corresponding `FileTable` but also act like a registry for functions that can convert inputs into an output for the corresponding row. These registered functions will likely be where most people make additions or modifications to this package, and so there is an example section below.

`FileTableBuilder` classes define how inputs are turned into a csv representation for the corresponding `FileTable` object. Subclasses must implement a `input_schema` class attribute that describes the columns in an input file. These will be used to create a dictonary for each row (`SSObjectFileTableBuilder` is slightly unique in that it has multiple inputs) that map input values to column names. This mapping is then passed to each of the functions which have been registered with the `FileTable`'s schema to produce a row in the generated csv file. If there are columns defined in a `FileTable`'s schema that have no registered conversion function a MySQL null value (\N) is inserted for that column.

`FileTable` and `FileTableBuilder` subclasses are defined in modules in the top level of this package.

The command line interface is defined in the cli module. The `__main__` module provides the command line entry point when run with `python -m`.

## Adding conversion function

Conversion functions need to be defined inside `schemas/columnConversions.py`. This ensures that function registration will happen at the appropriate time. It is possible to put function inside a differet pythnon module that is imported within `schemas/__init__.py`, but I find having them within one file allows for functions to be shared between different `TableSchema` subclasses.

To add a new column conversion function, the corresponding `TableSchema` subclass must be imported at the top of the file (this is likely already done at this point). The imported object then contains a classmethod, named `register`, that can be used as a decorator for a conversion function. The `register` decorator takes a single argument that names what column the decorated function will be called for. Let's look at what one of these looks like.

```python
from .DiaSourceSchema import DIASource

@DIASource.register(ColumnName("ra"))
def return_ra(row: Mapping) -> str:
    return row["AstRA(deg)"]
```

Before discussing the registered function, I want to mention some syntax you will see througout the package you may not be familar with. I have made use of python typing in most places in the codebase. These can be used together with a static type checker, like mypy, to provide a lot of bug checking as well as soundness of code checking that you normally do not get in python. This helps protect against randomly finding errors durring runtime that would otherwise crash your application. I also find that it helps me reason what types of objects and what behaviors they will have when actually creating the application. In the above example you will see things like a type annotation on row indicating that it will be a Mapping (like a dict), and that the function will be returning a str. Additionally There is a special defined type called `ColumnName` that woul will see when I register the function. This type is simply a wrapper around string, but one that indicates this is not any old string, it is one that corresponds to a column name. This helps gaurd against errors where you might pass a random string as an argument. It is both a visual clue to the programmer as well as something that the type checker can keep track of for you. This was a long (but actually short) introduction to python typing. If you are unfamilar with this, or just cant be bothered it is entirely optional, as long as you keep track of all the information and ensure the code runs as you expect. The only place where typing is not optional is when defining a subclass of `TableSchema` as these types are used to map the strings stored in the csv files into the corresponding python type.

Now looking at the example we see that we have imported the DiaSoure schema, and have called the register decorator with the argument `ra` this indicates that this function will produce outputs to fill the `ra` column in the `DIAFileTable`. The function name itself can be whatever you want, as it will only ever be called from within the registry.

The funciton recieves a single argument which will be a dictonary with keys corresponding to the input_schem defined in `DiaFileTableBuilder` and values corresponding to a single row of the input file. The values may still be encoded as a `str` so that in the case of a simple return there is no wasted time converting from a string to an object and back to a string. Functions are responsible for converting to another type i.e. `float(ra)` if needed.

In the case of this function there is no need to do any computation. The function simple looks up the `ra` key from the input row and returns that. Some function may need to do more complicated computations based on multiple values in the input, but should still only return a single value. Whatever output value is created it should be converted to a string prior to returning the value. The easiest way to do this is returning an f-string.

### Inputs of conversion functions by table type
#### DiaSource
The keys of the input mapping will be:
- ObjID
- observationId
- FieldMJD
- AstRange(km)
- AstRangeRate(km/s)
- AstRA(deg)
- AstRARate(deg/day)
- AstDec(deg)
- AstDecRate(deg/day)
- Ast-Sun(J2000x)(km)
- Ast-Sun(J2000y)(km)
- Ast-Sun(J2000z)(km)
- Sun-Ast-Obs(deg)
- V
- FiltermagV(H=0)
- Filter
- AstRASigma(mas)
- AstDecSigma(mas)
- PhotometricSigma(mag)

#### MPCORB
The keys of the input mapping will be:
- S3MID
- FORMAT
- q
- e
- i
- Omega
- argperi
- t_p
- H
- t_0
- INDEX
- N_PAR
- MOID
- COMPCODE

#### SSObject
SSObject conversion functions are a bit different. Instead of getting a dictonary as an input, they recieve an instance of `SSObjectRow` defined in `SSObjectFileTable.py`. This object has two attributes, `dia_list` and `mpc_entry`. `dia_list` is a list of dictionaries each with the following keys (corresponding to a row in an already converted DIAFileTable). The length of this list corresponds to the number of times a given ssObjectId has been observed. Each time a conversion function is called for SSObjects, it will be a unique ssObjectId.
- diaSourceId
- ccdVisitId
- diaObjectId
- ssObjectId
- parentDiaSourceId
- prv_procOrder
- ssObjectReassocTime: datetime
- midPointTai
- ra
- raSigma
- decl
- declSigma
- ra_decl_Cov
- x
- xSigma
- y
- ySigma
- x_y_Cov    # MPCORB: Date of last observation included in orbit solution

- psRa
- psRaSigma
- psDecl
- psDeclSigma
- psFlux_psRa_Cov
- psFlux_psDecl_Cov
- psRa_psDecl_Cov
- psLnL
- psChi2
- psNdata
- trailFlux
- trailFluxSigma
- trailRa
- trailRaSigma
- trailDecl
- trailDeclSigma
- trailLength
- trailLengthSigma
- trailAngle
- trailAngleSigma
- trailFlux_trailRa_Cov
- trailFlux_trailDecl_Cov
- trailFlux_trailLength_Cov
- trailFlux_trailAngle_Cov
- trailRa_trailDecl_Cov
- trailRa_trailLength_Cov
- trailRa_trailAngle_Cov
- trailDecl_trailLength_Cov
- trailDecl_trailAngle_Cov
- trailLength_trailAngle_Cov
- trailLnL
- trailChi2
- trailNdata
- dipMeanFlux
- dipMeanFluxSigma
- dipFluxDiff
- dipFluxDiffSigma
- dipRa
- dipRaSigma
- dipDecl
- dipDeclSigma
- dipLength
- dipLengthSigma
- dipAngle
- dipAngleSigma
- dipMeanFlux_dipFluxDiff_Cov
- dipMeanFlux_dipRa_Cov
- dipMeanFlux_dipDecl_Cov
- dipMeanFlux_dipLength_Cov
- dipMeanFlux_dipAngle_Cov
- dipFluxDiff_dipRa_Cov
- dipFluxDiff_dipDecl_Cov
- dipFluxDiff_dipLength_Cov
- dipFluxDiff_dipAngle_Cov
- dipRa_dipDecl_Cov
- dipRa_dipLength_Cov
- dipRa_dipAngle_Cov
- dipDecl_dipLength_Cov
- dipDecl_dipAngle_Cov
- dipLength_dipAngle_Cov
- dipLnL
- dipChi2
- dipNdata
- totFlux
- totFluxErr
- diffFlux
- diffFluxErr
- fpBkgd
- fpBkgdErr
- ixx
- ixxSigma
- iyy
- iyySigma
- ixy
- ixySigma
- ixx_iyy_Cov
- ixx_ixy_Cov
- iyy_ixy_Cov
- ixxPSF
- iyyPSF
- ixyPSF
- extendedness
- spuriousness
- flags
- htmId20

The mpc_entry attribute will be a dictonary with corresponding to a row in an alreay processed `MPCORBFileTable`, unless a given ssObjectId could not be found (which should not be possible) and then a NoIndexError object will be returned to signal that. A dict with the right schema and values of '\\N' can be gotten from a NoIndexError via the row attribute:
- mpcDesignation
- mpcNumber
- ssObjectId
- mpcH
- mpcG
- epoch
- M
- peri
- node
- incl
- e
- n
- a
- uncertaintyParameter
- reference
- nobs
- nopp
- arc
- arcStart
- arcEnd
- rms
- pertsShort
- pertsLong
- computer
- flags
- fullDesignation
- lastIncludedObservation
- covariance

#### SSSource
Functions registered with SSSource will get a dictonary containing the following keys:
- ObjID
- observationId
- FieldMJD
- AstRange(km)
- AstRangeRate(km/s)
- AstRA(deg)
- AstRARate(deg/day)
- AstDec(deg)
- AstDecRate(deg/day)
- Ast-Sun(J2000x)(km)
- Ast-Sun(J2000y)(km)
- Ast-Sun(J2000z)(km)
- Sun-Ast-Obs(deg)
- V
- FiltermagV(H=0)
- Filter
- AstRASigma(mas)
- AstDecSigma(mas)
- PhotometricSigma(mag)
- mpcDesignation
- mpcNumber
- ssObjectId
- mpcH
- mpcG
- epoch
- M
- peri
- node
- incl
- e
- n
- a
- uncertaintyParameter
- reference
- nobs
- nopp
- arc
- arcStart
- arcEnd
- rms
- pertsShort
- pertsLong
- computer
- flags
- fullDesignation
- lastIncludedObservation
- covariance
- ssObjectId
- discoverySubmissionDate
- firstObservationDate
- arc
- numObs
- lcPeriodic
- MOID
- MOIDTrueAnomaly
- MOIDEclipticLongitude
- MOIDDeltaV
- uH
- uG12
- uHErr
- uG12Err
- uH_uG12_Cov
- uChi2
- uNdata
- gH
- gG12
- gHErr
- gG12Err
- gH_gG12_Cov
- gChi2
- gNdata
- rH
- rG12
- rHErr
- rG12Err
- rH_rG12_Cov
- rChi2
- rNdata
- iH
- iG12
- iHErr
- iG12Err
- iH_iG12_Cov
- iChi2
- iNdata
- zH
- zG12
- zHErr
- zG12Err
- zH_zG12_Cov
- zChi2
- zNdata
- yH
- yG12
- yHErr
- yG12Err
- yH_yG12_Cov
- yChi2
- yNdata
- maxExtendedness
- minExtendedness
- medianExtendedness
- flags

## Running Conversion sub commands
Sub commands need to be run in the following order to ensure all the information needed for each command has been generated.

1. dia
2. mpcorb
3. ssobject
4. sssource

The dia command takes in simulated inputs and generates an output file. An example of running this is:

```
python -m SSTableConvertMod dia --skip_rows=1 /epyc/projects/jpl_survey_sim/s3c/S0.dat.csv /epyc/users/nlust/outputs/dias/dia0.csv
```
Repeat this for all the files, it should use little memory and be safe to launch in parallel (though be kind to others with cpu useage)


The mpcorb command takes in a glob that is used to run on a batch of files all at once to produce one output like this (note the quoted glob):

```
python -m SSTableConvertMod mpcorb --skip_rows=2 "/epyc/projects/jpl_survey_sim/S3M_v09.05.15/*.s3m" /epyc/users/nlust/outputs/mpcorb.csv
```

The ssobject sub command takes in a glob that points to converted dia file outputs and a file path that points to the output of the mpcorb subcommand.

```
python -m SSTableConvertMod ssobject --skip_rows=1 "/epyc/users/nlust/outputs/dias/*.csv" /epyc/users/nlust/outputs/mpcorb.csv /epyc/users/nlust/outputs/ssobject.csv
```

Finally the ssource subcommand takes in a filepath corresponding to a simulated inputs, the filepath to the output of the ssobject command, and the filepath to the output of the mpcorb command.

```
python -m SSTableConvertMod sssource --skip_rows=1 /epyc/projects/jpl_survey_sim/s3c/S0.dat.csv /epyc/users/nlust/outputs/ssobject.csv /epyc/users/nlust/outputs/mpcorb.csv /epyc/users/nlust/outputs/sssources/sssource1.csv
```

All of these commands support `--skip_rows` which can be used to skip a given number of lines (normally the length of the header at the top of a file), `--stop_after` which can be used to limit the number of lines produced, useful in debugging before running a long job with many rows. The dia subcommand supports a `--do_index` option, but that should be left as the default `True` for now.