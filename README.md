# StackStorm pack with actions for interacting with Scout

## Installation

```bash
st2 pack install https://github.com/gmc-norr/st2-scout.git
st2 pack config scout
```

## Config

The config parameters that need to be defined are:

- `scout_host`: Host machine running scout
- `scout_cwd`: Working directory on scout host
- `owners_map`: Mapping pipeline/analysis type to scout owner/institute
- `genomes_map`: Mapping pipeline/analysis type to scout genome build
- `scout_case_file_suffixes`: file suffixes for case files used in the scout load config
- `scout_sample_file_suffixes`: file suffixes for sample files used in the scout load config
- `scout_chromograph_file_prefixes`: file prefixes for sample chromograph files used in the scout load config
- `loqusdb_config_map`: mapping analysis pipeline to config file mounted in docker container
- `loqusdb_ped`: subdir and suffix for ped file to be used by loqusdb
- `loqusdb_vcf`: subdir and suffix for vcf file to be used by loqusdb     


## Actions

ref                                               | description
--------------------------------------------------|------------------------------------------
scout.generate_load_config                        | Generate the scout load config content
scout.write_load_config                           | Write the load config content to a .yaml file
scout.get_loqusdb_files                           | Get necessary files for loqusdb
scout.load_case                                   | Load a case from a load config into scout
scout.load_variants                               | Load variants from a case into loqusdb
scout.generate_and_load_case                      | Action triggering the generate_and_load_case workflow

## Workflows
ref                                               | description
--------------------------------------------------|------------------------------------------
scout.generate_and_load_case                      | Workflow that loads finished analysis into scout and loqusdb

## Rules
No rules at the moment

## Sensors
No sensors at the moment


## Known issues


