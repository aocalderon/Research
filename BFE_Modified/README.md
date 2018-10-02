# Algorithms for Flock Pattern Discovery


## Introduction

This project contains the source code of two algorithms for flock pattern discovery: BFE (Basic 
Flock Evaluation) [(Vieira & Bakalov & Tsotras 2009)](#markdown-header-referencespublications) 
and PSI (Plane sweeping, Signatures and Indexes)[(Tanaka & Vieira & Kaster 2015](#markdown-header-referencespublications) 
and  [Tanaka & Vieira & Kaster 2016)](#markdown-header-referencespublications). And it's main 
purpose is provide, mainly to the academic community, the source code so the experiments from the 
previous articles can be reproduced and to facilitate future comparisons and works.


## BFE (Basic Flock Evaluation)

In Vieira's work [(Vieira & Bakalov & Tsotras 2009)](#markdown-header-referencespublications) 
were proposed several algorithms to find flock patterns that basically share the same 4 common 
steps, which are:

1. For each time tᵢ, process points using a grid-based index with grid edges of ε 
distance:

    1. For each grid cell gₓ,y, only the 9 adjacent grid cells, including itself, are 
    analyzed. The algorithm first process every point in gₓ,y and every point in 
    _[gₓ₋₁,y₋₁...gₓ₊₁,y₊₁]_ in order to find pair of points pᵣ, pₛ whose distances satisfy: _d
    (pᵣ, pₛ) ≤ ε_;
    2. Pairs that have not been processed yet and are within ε to each other are further used to 
    compute the two disks c₁ and c₂;
    3. For each point _pᵣ ∈ gₓ,y_, a range query with radius ε is performed 
    over all 9 grids _[gₓ₋₁,y₋₁...gₓ₊₁,y₊₁]_ to find points that can be 
    “paired” with pᵣ, that is _d(pᵣ, pₛ) ≤ ε_ holds. The result 
    of such range search with more or equal points than µ (_|H| ≥ µ_) is stored in the list _H_ 
    that is used to check for each disk computed. For those valid pairs, at most 2 disks are 
    generated. For each of them, points in the list _H_ are checked if they are inside the disk. 
    Disks that have at least µ points (_|cₖ| ≥ µ_ ) are kept.
    4. The process that BFE employs to keep only the maximal disks is based on the center of the 
    disk and the total number of common elements that each disk has. Disks are checked only with 
    the ones that are “close” to each other, that is, disk c₁ is checked with 
    c₂ only if _d(c₁,c₂) ≤ ε_. Therefore, we only need to count 
    common elements in both disks by scanning each entry in each disk, keeping just those maximal
    disks;

2. For each qualifying disk found in the current timestamp, BFE joins with other disks found in 
the previous timestamp;

3. Report the flocks with δ consecutive time instances.

## PSI (Plane sweeping, Signatures and Indexes)


Since the aforementioned steps (1) and (3) are the most time consuming, in Tanaka's work [(Tanaka
 & Vieira & Kaster 2015](#markdown-header-referencespublications) and  [Tanaka & Vieira & Kaster 
 2016)](#markdown-header-referencespublications) are proposed a different strategy to evaluate 
 flock patterns.
The authors should be noticed that the worst case complexity of finding flock patterns are the 
same, but here we are interested in improving the average case.

This new extension is based on the plane-sweeping technique to find disks with at least number of 
points inside them. 
Furthermore, for the steps (3) and (4), Bloom filters and inverted indexes are used to fast search 
for supersets/subsets. 
In summary, PSI method works in the following way:

1. For each time tᵢ, process points using a plane-sweeping with band of size 2ε on 
x-axis:

    1. Order points according to the x-axis;
    2. Sweeps the plane (from left to right in x-axis) using a band of size 2ε along the x-axis 
    centered at a point pᵣ. The algorithm selects all the points inside the band that are in the 
    range _[pr.x − ε, pr.x + ε]_ and _[pr.y − ε, pr.y + ε]_;
    3. After selecting the points in the 2ε × 2ε box defined by pᵣ, we then check for pairs of 
    points that qualify for new flock disks. Thus, we generate disks defined by pᵣ and any point 
    p inside the right half of box such that the distance between pᵣ and p is at most ε. Points 
    in the left half of box were checked in previous steps;
    4. If a candidate disk contains at least µ entities inside it, then the underlying entity set
    is reported as a candidate set and the box is set active in the timestamp. Every active box 
    is represented through the Minimum Bounding Rectangle (MBR) enclosing its elements;
    5. The next step is to check for disks that are subsets or supersets. Firstly, PSI uses spatial 
    properties of boxes (before using properties of disks as in BFE) to prune subsets without 
    executing (expensive) set intersection operations.
    Iterating on each active box (i.e., a box that has a candidate set) in the current timestamp,
    and checks if there is intersection between the MBR of a box and the MBRs of boxes near it. 
    In this case, it is necessary to check whether there is duplicate or subsets between these 
    boxes;
    6. However, before performing the set intersection operation, PSI applies a second filtering 
    step using binary signatures. As entities get inserted in a candidate set, a set of hash 
    functions are used to generate a signature for it. In this algorithm a set of Bloom filters is 
    used to represent subsets of a universe, and then these filters, which essentially are binary 
    vectors, are used to check for subsets amongst the sets;
    7. How it is well-known that Bloom filters can generate false-positives, but most 
    importantly, no false-negatives. This is the reason that, after we check one disk is subset 
    of another using binary signatures, we still need to verify using set intersection whether 
    the result is a false-negative.

2. For each new time instance tᵢ evaluated, we have to join with the previous one tᵢ₋₁. To find 
possible disks dᵢ₋₁ in tᵢ₋₁ to join to a disk *d* for timestamp tᵢ PSI builds an inverted index 
for each point that belong to a flock disk.
Each point has a list of flock disks that it belongs to. We then check each point that belong to 
the disk *d* in the inverted index. Let's say that pᵢ in *d* are the set of points in d.
Then, flocks(pᵢ) is the set of flocks that contain pᵢ for tᵢ₋₁. If the total number of flocks 
that have the same points in *d* are at least μ, then we must join disk *d* with it. 
Otherwise, we don't need to join, since the number of common points are below μ.

3. Report the flocks with δ consecutive time instances.


## References/Publications

1. [Marcos R. Vieira, Petko Bakalov, Vassilis J. Tsotras (2009) - On-Line Discovery of Flock 
Patterns in Spatio-Temporal Data](http://dl.acm.org/citation.cfm?doid=1653771.1653812)
2. [Pedro Sena Tanaka, Marcos R. Vieira, Daniel S. Kaster (2015) - Efficient Algorithms to 
Discover Flock Patterns in Trajectories]()
3. [Pedro Sena Tanaka, Marcos R. Vieira, Daniel S. Kaster (2016) - An Improved Base Algorithm for
 Online Discovery of Flock Patterns in Trajectories](https://seer.ufmg.br/index.php/jidm/article/view/1371)


## Compiling the project

This project is CMake powered, so in order to compile it you must have CMake in version 3.1+ then you can simply run:
```
#!bash
bin/build.sh
```

from the root folder of the repo. The CMake _--clean-first_ option can also be passed and its use
 is encouraged.

Since the project uses some features of C++11, G++ should be version 4.9+.


## Compiling on Ubuntu (12.04+)

If you're using Ubuntu be sure to add the following ppa (not necessary in Ubuntu 16.04+):


```
:::bash
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-add-repository -y ppa:george-edison55/cmake-3.x
sudo apt-get update
sudo apt-get install -y cmake g++
```


## Working on the code

Since this project depends on CMake, it's highly recommended that you use a IDE with full support to CMake.

One IDE that is crazy good is [CLion](https://www.jetbrains.com/clion/) from JetBrains.
This IDE is free for students and teachers/professors (or anybody with an educational email), and have great support 
for CMake.

Others IDE's that can be used include: [NetBeans](http://www.netbeans.org/), 
[QtCreator](https://www.qt.io/download-open-source/), 
TexMate with [CMake Bundle](https://github.com/textmate/cmake.tmbundle), etc.

### Working with GIT in this repository

In order to check detailed instructions about how to use git and versioning of the 
project please check the documentation at: [CONTRIBUTING](CONTRIBUTING.md).

### Unit testing

There are some tests written using [GoogleTest](https://github.com/google/googletest) framework. The CLion IDE have full 
compatibility with GTest and also provides a runner for the tests.
There are not much tests, but it is important to write new ones as new code is being written.

## Running experiments

See [README](samples/README.md) file in `samples/` for more information.

## Checking the documentation (work in progress)

For now there's not much to see here, but you can build the docs with [doxygen](http://www.stack.nl/~dimitri/doxygen/) by running:

```
:::bash
doxygen
```

on the root level of the repository. Then, you must go to the ```doc``` folder and fire up a browser to navigate the docs.
