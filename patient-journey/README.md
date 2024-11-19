# Snowflake Graph Analytics
## Worked Example: Patient Journey

## Dataset
Uses [Synthea](https://github.com/synthetichealth/synthea)

## Prerequisites
Run Synthea above.  Change `exporter.csv.export = true` in `synthea.properties` then use the following options
```bash
./run_synthea -s 7474 -p 1000
```

Crete a `PATIENT_DB` database and load the following csvs into `PUBLIC` schema: 
1. CAREPLANS
2. CONDITIONS
3. ENCOUNTERS
4. MEDICATIONS
5. PATIENTS
6. PROCEDURES