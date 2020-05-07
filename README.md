# SwarmFlow

A distributed workflow management system for High Performance Computing environments with task clustering

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them

```
Give examples
```

### Setting up the project

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
Give the example
```

### Basic usage

A step by step series of examples that tell you how to use the basic system functions.

```
Give the example
```

## Built With

* [Python](http://www.dropwizard.io/1.0.2/docs/) 
* [FireWorks](https://github.com/materialsproject/fireworks)


## Contributing

How to contribute guide

## Versioning

about Versioning


## Authors

* **Kalana Dananjaya** - https://github.com/KalanaDananjaya
* **Ayesh Malindun** - https://github.com/AyeshW
* **Randika Jayasekara** - https://github.com/rpjayasekara


## License

This project is licensed under the <liscene name> License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* 

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
pip install swarmflow
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

## Built With

* [Python](http://www.dropwizard.io/1.0.2/docs/) 
* [FireWorks](https://github.com/materialsproject/fireworks)

## Authors

* **Kalana Dananjaya** - https://github.com/KalanaDananjaya
* **Ayesh Malindun** - https://github.com/AyeshW
* **Randika Jayasekara** - https://github.com/rpjayasekara


## License

[![License](http://img.shields.io/:license-mit-blue.svg?style=flat-square)](http://badges.mit-license.org)

- **[MIT license](https://github.com/SwarmForm/SwarmForm/blob/master/LICENSE)**
- Copyright 2015 © SwarmForm.
