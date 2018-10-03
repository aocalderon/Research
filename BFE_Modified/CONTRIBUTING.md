# Contributing

This repository uses two main tools/frameworks in software development: 
[git flow](http://nvie.com/posts/a-successful-git-branching-model/) and 
[Semantic Versioning](http://semver.org/).
In order to properly use those tools this file describe the development 
workflow which must be used when developing new features.

## TL;DR

If you are **already used** to work with semantic versioning + git-flow 
here is a compiled version of the repository guidelines.

Git flow configuration:
 * master=master
 * development=develop
 * release candidates (prefix)=rc/
 * feature (prefix)=feat/
 
Semantic versioning conventions:
 * Whenever finishing a release candidate run the script ```bin/bumpversion``` 
 from the root of the repository.
 * If the release introduces really big changes (or changes in the runner), 
 change the major version otherwise change minor.
 

## Git Flow

This repository uses a mid-lightweight version of git-flow which only uses
branches for:

 * Features
 * Release Candidates
 
And the traditional main branches:

 * master
 * develop

To get more info about how to use git-flow refer to the 
[blog post](http://nvie.com/posts/a-successful-git-branching-model/) written
by the creator of the workflow (branching strategy).
You can also install the git flow command-line utility (learn how to install 
[here](https://github.com/nvie/gitflow/wiki/Installation)) in order to get 
shortcuts to create/finish features, hot fixes and releases.

### Creating a new feature

Whenever adding new features, developing new methods or changing considerably
the way something works one must create a new branch for its feature.
In order to do that first you ensure your develop branch is up-to-date with the
remote running the following command on terminal:

```
:::bash

# Make sure you're in develop
git checkout develop 
# Update the branch from origin
git pull origin develop
```

Then to create the branch of the release run the following command:

```
:::bash

git checkout -b feat/<name_of_the_feature> develop

# Or if you have git-flow installed:

git flow feature start <name_of_the_feature>
```

Where <name_of_the_feature> is a readable name and that really says
in a couple of words what you are going to do in this feature. 
Then, you can work normally, committing your changes in atomic 
 commits.
Whenever you're ready to incorporate the feature in to develop run the 
following the commands:

```
:::bash

git checkout develop
git pull origin develop
git merge --no-ff feat/<name_of_the_feature>
git branch -d feat/<name_of_the_feature>

# If you pushed the feature branch to the remote, remove the branch there as well 
git branch -D feat/<name_of_the_feature>


### Or if you have git-flow installed:

git flow finish feature <name_of_the_feature>
```

### Creating a new release

Once you developed a nice set of features and the develop branch reflects what you
would want in a new version of the application you must create a release.
To create the release branch just run the following commands:

```
:::bash 

git checkout develop 
git pull origin develop
git checkout -b release/<major>.<minor>.0 develop

### Or if you have git-flow installed:
git flow release start <major>.<minor>.0
```

Where major and minor refer to numeration of the SemVer.

Work on your modifications and then before you finish the release candidate run the 
script ```bin/bumpversion``` like this (make sure you're in the root folder of the 
repository):

```
:::bash

bin/bumpversion
```

Insert the tag number for the next version of the application. Then the script will
create the changelog in the file CHANGES.

To really finish your release run:

```
:::bash

git checkout master
git merge --no-ff release/<major>.<minor>.0 
git tag -a <major>.<minor>.0
git checkout develop
git merge --no-ff release/<major>.<minor>.0 
git branch -d release/<major>.<minor>.0

### Or if you have git-flow installed:
git flow release finish <major>.<minor>.0
```


### That's it. ###
