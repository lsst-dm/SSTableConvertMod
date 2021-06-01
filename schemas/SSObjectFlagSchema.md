# SSObject Table Flags Schema
## Created by Aidan Berres

*As of 5/31/2021*

This is a 22 bit flag system paraphrasing this schema from SDSS SkyServer,

 http://skyserver.sdss.org/dr16/en/help/browser/browser.aspx#&&history=enum+PhotoFlags+E

Here is the table of data values,

|    | Name                   | Value (Binary)           | Value (Hex)   | Description                                                                 |
|---:|:-----------------------|:-------------------------|:--------------|:----------------------------------------------------------------------------|
|  0 | TOO_FEW_NDATA_u        | 0b1                      | 0x1           | Less than 2 observations in band u for the given row, cannot be fit         |
|  1 | TOO_FEW_NDATA_g        | 0b10                     | 0x2           | Less than 2 observations in band g for the given row, cannot be fit         |
|  2 | TOO_FEW_NDATA_r        | 0b100                    | 0x4           | Less than 2 observations in band r for the given row, cannot be fit         |
|  3 | TOO_FEW_NDATA_i        | 0b1000                   | 0x8           | Less than 2 observations in band i for the given row, cannot be fit         |
|  4 | TOO_FEW_NDATA_z        | 0b10000                  | 0x10          | Less than 2 observations in band z for the given row, cannot be fit         |
|  5 | TOO_FEW_NDATA_y        | 0b100000                 | 0x20          | Less than 2 observations in band y for the given row, cannot be fit         |
|  6 | FIT_FAIL_u             | 0b1000000                | 0x40          | Catastrophic failure of the fitting procedure for band u for the given row  |
|  7 | FIT_FAIL_g             | 0b10000000               | 0x80          | Catastrophic failure of the fitting procedure for band g for the given row  |
|  8 | FIT_FAIL_r             | 0b100000000              | 0x100         | Catastrophic failure of the fitting procedure for band r for the given row  |
|  9 | FIT_FAIL_i             | 0b1000000000             | 0x200         | Catastrophic failure of the fitting procedure for band i for the given row  |
| 10 | FIT_FAIL_z             | 0b10000000000            | 0x400         | Catastrophic failure of the fitting procedure for band z for the given row  |
| 11 | FIT_FAIL_y             | 0b100000000000           | 0x800         | Catastrophic failure of the fitting procedure for band y for the given row  |
| 12 | FIT_PARAM_FAIL_u       | 0b1000000000000          | 0x1000        | Fit parameters returned as Nan values for band u for the given row          |
| 13 | FIT_PARAM_FAIL_g       | 0b10000000000000         | 0x2000        | Fit parameters returned as Nan values for band g for the given row          |
| 14 | FIT_PARAM_FAIL_r       | 0b100000000000000        | 0x4000        | Fit parameters returned as Nan values for band r for the given row          |
| 15 | FIT_PARAM_FAIL_i       | 0b1000000000000000       | 0x8000        | Fit parameters returned as Nan values for band i for the given row          |
| 16 | FIT_PARAM_FAIL_z       | 0b10000000000000000      | 0x10000       | Fit parameters returned as Nan values for band z for the given row          |
| 17 | FIT_PARAM_FAIL_y       | 0b100000000000000000     | 0x20000       | Fit parameters returned as Nan values for band y for the given row          |
| 18 | MOID_FAIL              | 0b1000000000000000000    | 0x40000       | Failure of MOID procedure, cannot return MOID value                         |
| 19 | MOID_TRUE_ANOMALY_FAIL | 0b10000000000000000000   | 0x80000       | Failure of MOID True Anomaly procedure, cannot return MOIDTrueAnomaly value |
| 20 | MOID_DELTAV_FAIL       | 0b100000000000000000000  | 0x100000      | Failure of MOID Delta V procedure, cannot return MOIDDeltaV value           |
| 21 | RESERVED               | 0b1000000000000000000000 | 0x200000      | Not used                                                                    |
