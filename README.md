# CROP: Robust Query Processing for Linked Data Fragments

A query engine to execute SPARQL queries over [Triple Pattern Fragments](https://linkeddatafragments.org) using a cost- and robustness-based query plan optimization strategy.
The engine also supports Polymorphic Bind Join operators to additionally support robust query execution.
The implementation of the engine is based on the [Network of Linked Data Eddies](https://github.com/maribelacosta/nlde).

## Installation

The engine is implemented in Python 2.7.
It is advised to use a virtual environment for the installation in the following way:

```bash
# Install virtualenv
[sudo] python2 -m pip install virtualenv 

# Create a virtual environment
[sudo] python2 -m virtualenv venv

# Activate the environment
source venv/bin/activate

# Install the requirements
python -m pip install -r requirements.txt
```

Once the requirements are installed, the engine can be executed using the command line.


## Usage

Required Parameters:
- `-f`: SAPRQL Query Filepath
- ``-s``: (TPF) Server to be contacted
- ``-c``: Configuration file for the query planner. (e.g. CROP, nLDE, LDP)
Example: 
```
venv/bin/python crop.py -f example.rq -s http://fragments.dbpedia.org/2014/en -c configs/crop_default.yaml
```

Note that the engine currently only supports conjunctive SPARQL queries (BGPs).

## Configuration 

The configuration file allows for specifying the parameter for the exectution engine as well as the query planning approach.
A set of basic configurations for different planning approaches is provided in the ``configs`` directory.
The following options can be configured (Note that some combinations might be exclusive):

### Execution engine (```nLDE```)

- ``eddies``: Number of eddies
- ``timeout``: Timeout of the execution in seconds

### Planner (```crop```)

- ``optimizer``: Query planning approach: crop, nlde, left-linear  

- ``height_discount``: Cost model: delta value (crop only)
- ``cost_threshold``: Planner: cost threshold gamma (crop only)
- ``robust_threshold``: Planner: robustness threshold rho (crop only)
- ``top_t``: Planner: Top t plans to consider in planner
- ``k``: IDP: block size parameter (crop only)
- ``adaptive_k``: IDP: enable adatptive block size in the planner (crop only)

### Operators (```operators```)

- ``hash_join_only``: HJ: Only place HJ operators (left-linear only)
- ``poly_hash_join``: PHJ: Enable PHJ operator
- ``hj_request_cost_factor``: PHJ: epsilon parameter value of the PHJ 

- ``bind_join_only``: BJ: Only place BJ operators (left-linear only)
- ``poly_bind_join``: PBJ: Enable PBJ operator 
- ``poly_bind_rule``: PBJ: Set the switch rule of the operator (1/sqrt(T) = 2) 


## Help

You can find the help using:
```bash
venv/bin/python crop.py -h
```
and get the usage options:
```bash
usage: crop.py [-h] -s SOURCES [SOURCES ...] -c CONFIG -f QUERYFILE
               [-o {y,f,dief,all}] [-l {INFO,DEBUG}] [-p]

CROP: An nLDE-based TPF Client supporting different query planning approaches
and polymorphic join operators.

optional arguments:
  -h, --help            show this help message and exit
  -s SOURCES [SOURCES ...], --sources SOURCES [SOURCES ...]
                        URL of the triple pattern fragment servers (required)
  -c CONFIG, --config CONFIG
                        Configuration YAML file. Including options for
                        execution engine, planner, and operators (required)
  -f QUERYFILE, --queryfile QUERYFILE
                        file name of the SPARQL query (required)
  -o {y,f,dief,all}, --output {y,f,dief,all}
                        Format of the output (optional)
  -l {INFO,DEBUG}, --log {INFO,DEBUG}
                        Logging configuration (optional)
  -p, --planning        Just obtain the plan. Do not execute the plan
                        (optional)
```


## How to Cite
```
Lars Heling, Maribel Acosta. 
"Cost- and Robustness-based Query Optimization for Triple Pattern Fragment Clients" 
International Semantic Web Conference 2020.
```

## License

This project is licensed under the MIT License.


