# Snowflake Graph Analytics
## Worked Example: Patient Journey

## Dataset
We use [Synthea](https://github.com/synthetichealth/synthea) to simulate realistic patient data. 

## Prerequisites

### Get Source Data
Get source csvs either from [here](https://drive.google.com/drive/folders/14bbDMnLU7beW1f79Rjr4J2b7hw3mCIQJ?usp=sharing) or by running [Synthea](https://github.com/synthetichealth/synthea)  per directions below:

Change `exporter.csv.export = true` in `synthea.properties` then use the following options
```bash
./run_synthea -s 7474 -p 1000
```
### Load Source Data
Crete a `PATIENT_DB` database and load the following csvs into `PUBLIC` schema:
1. PATIENTS
2. PROCEDURES