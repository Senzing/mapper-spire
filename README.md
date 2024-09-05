# mapper-spire

## Overview

This mapper converts the Spire enhanced_vessel_master.csv file into a json file ready to load into Senzing where it can be matched against other data sources.  You can purchase Spire data at [https://spire.com/maritime/](https://spire.com/maritime/)

Full Usage:

```console
python3 spire-mapper.py --help
usage: spire-mapper.py [-h] [-i INPUT_FILE] [-o OUTPUT_FILE] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
                        the name of the input file
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        the name of the output file
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file
```

Typical Use:

```console
python3 nomino-mapper.py -i input/enhanced_vessel_master.csv -o output/spire_vessel_master.json
```

- You can add the -l parameter to get stats and examples of the mapped file.

Configuring Senzing:

Go into the G2ConfigTool.py and add the data source code(s) you decide to use.

```console
/opt/senzing/g2/python/G2ConfigTool.py 

Welcome to the Senzing configuration tool! Type help or ? to list commands

(g2cfg) addDataSource SPIRE

Data source successfully added!

(g2cfg) save

Are you certain you wish to proceed and save changes? (y/n) y

Configuration changes saved!


Initializing Senzing engines ...

(g2cfg) quit

```

You are now ready to load the json output file into Senzing using your desired method!
