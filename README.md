# SwarmForm

SwarmForm is a distributed workflow management system for High Performance Computing environments. SwarmForm introduces task clustering to reduce the makespan of workflows executed using it by minimizing the overheads. SwarmForm follows a distributed architecture with independent programs controlling different functional layers in workflow management.
## Getting Started

These instructions will get you familiar with the installation and basic functions of the SwarmForm.

### Prerequisites

What things you need to install to run the SwarmForm.

* MongoDB
* Python 3.3+
* pip

### Installation

To install SwarmForm, simply type

```
pip install swarmflow
```

### Basic usage

A step by step series of examples that tell you how to use the basic system functions.

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

MIT License

Copyright (c) 2020 SwarmForm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
