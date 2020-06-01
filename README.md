# SwarmForm

SwarmForm is a distributed workflow management system for High Performance Computing environments. SwarmForm introduces task clustering to reduce the makespan of workflows executed using it by minimizing the overheads. SwarmForm follows a distributed architecture with independent programs controlling different functional layers in workflow management.

## Getting Started

These instructions will get you familiar with the installation and basic functions of the SwarmForm.

### Prerequisites

Following prerequisites should be installed to run SwarmForm.

* MongoDB
* Python 3.3+
* pip

### Installation

To install SwarmForm, simply type

```
pip install swarmform
```

### Basic usage

Following is a step by step series of examples that tell you how to use the basic system functions.

Initialize a SwarmForm launchpad YAML file
```
sform init
```

Insert a SwarmFlow from file
```
sform add -sf <file path>
```

Get SwarmFlow from SwarmPad
```
sform get_sf -id <SwarmFlow ID>
```

Cluster the fireworks in the SwarmFlow and save the new SwarmFlow to the database
```
sform cluster -sf <SwarmFlow ID>
```

Reset and re-initialize the SwarmForm database
```
sform reset
```

Launch a single rocket to execute a firework
```
rlaunch singleshot
```

Launch a multiple rockets sequentially to execute all the fireworks
```
rlaunch rapidfire
```

Launch parallel m nummber of rockets to execute all the fireworks parallely.(Usually m equals to number of available resources) 
```
mlaunch <m>
```

## Built With

* [Python](http://www.dropwizard.io/1.0.2/docs/) 
* [FireWorks](https://github.com/materialsproject/fireworks)

## Authors

* **Kalana Dananjaya** - https://github.com/KalanaDananjaya
* **Ayesh Weerasinghe** - https://github.com/AyeshW
* **Randika Jayasekara** - https://github.com/rpjayasekara


## License

[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](http://badges.mit-license.org)

- **[MIT license](https://github.com/SwarmForm/SwarmForm/blob/master/LICENSE)**
- Copyright 2020 © SwarmForm.
