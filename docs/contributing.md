# How to contribute
Thanks for taking time to contribute!

This document outlines the contribution guidelines for Saint Louis Regional Data Alliance's projects. These are mostly guidelines; feel free to reach out to one of the STLRDA staff if you have questions, or  you'd like to contribute in ways unrelated to code.

Please note that this is a living document, and subject to frequent expansion and change.

## Table of Contents
* [Code of Conduct](#code-of-conduct)
* [Important Resources](#important-resources)
* [Project Overview](#project-overview)
* [How to Contribute](#how-to-contribute)
* [Style Guides](#style-guides)

## Code of Conduct
This is under construction, and will be updated when ready. For now, be respectful and do right by your community.

## Important Resources
* [stldata.org](https://stldata.org) tells you about our organization
* [This google drive folder](https://drive.google.com/drive/folders/1dBwWpALR4q5Z_3X-S5O00uWi9SEGmgJO?usp=sharing) contains powerpoint pitch decks about our projects.


## Project Overview
### General
While availability is a critical first step in using data to impact communities, it is just as important, if not more so, that the data be _useable_. Data that is highly fragmented, lacks standardization, in difficult to use formats, and/or lacking context and documentation is extremely difficult to build tools and analysis off of.

To address this need, the Saint Louis Regional Data Alliance is building the Regional Entity Database (REDB), a unified database of land parcel data. Pulling data from numerous local government departments and with a schema designed to be understandable by the average citizen, REDB will serve as single source of truth for parcel data.

REDB is being built entirely in AWS, with an eye on being reproducible by other organizations and municipalities. To that end, all code associated with this project is being made available on Github, in three repositories.

* [REDB-Platform](https://github.com/stlrda/REDB-Platform) contains a Terraform module that builds an Airflow cluster that will run and manage the ETL processes that will create, maintain, and update REDB. Run in isolation, this will create a functioning Airflow cluster and nothing more.

* [REDB-Infrastructure](https://github.com/stlrda/REDB-Infrastructure) builds on the above. It contains Terraform code that pulls in the module above, and adds an S3 bucket (for the Airflow workers to use in their processes) and an Aurora PostGreSQL database (REDB). Run in isolation, this will creating everything an organization needs to start building out their own data workflows.

* [REDB-Workflows](https://github.com/stlrda/REDB-Workflows) contains the DAGs, scripts, and other files that the Airflow cluster will run. This piece is the most Saint Louis specific, but is being provided to serve as an example for other municipalities, and to provide visibility into REDB's working processes.
### This Repository
This repository contains the DAG Files and Scripts used to automate DKAN. The contents of this repository are meant to be within the infrastructure outlined above.

Automating the processing of data from a wide variety of government sources into a format that can be added to this database involves many steps. These steps can be saved in script files to be run on new data, but the running of these script files is still a complex task. To effectively import the data, these steps must applied to the data periodically and in sequences. Task scheduling services such as chron, which are provided with operating systems, are very limited. Apache Airflow provides a flexible and effective method to automate this workflow of menial data processing tasks.
Apache Airflow is a workflow manager based on DAGs, directed acyclic graphs. In plain English, these are structures akin to flow charts which define the tasks that must be performed and specifically map out the path of flow from one task to another within the process. The entire workflow's DAG, though it is also called a Pipeline in Airflow terminology, is defined within a Python script file. You can learn to make this file with Apache's tutorial here:

[https://airflow.apache.org/docs/stable/tutorial.html](https://airflow.apache.org/docs/stable/tutorial.html)

## How Can I Contribute?
1. Check the issues to see what needs to be done. Feel free to add additional issues if you think of additional work that should be done.
2. Once you've identified an issue you would like to tackle, assign it to yourself.
3. Create a branch off of Development, and name it after your issue number (issues/16, for example). This branch is where you will do your work. WE WILL NOT ACCEPT PULL REQUESTS FROM FORKS.
4. Once you have completed your work, submit a pull request from your branch to the Release-Candidate branch. Pull requests will be reviewed and merged on a regular basis (at least once a week).
5. Once all pull requests are merged, Release-Candidate will be pulled and merged into Master and Development branches for the next cycle.

## Style Guides
### Terraform
The Saint Louis Regional Data Alliance follows some simple style rules when writing Terraform modules:

* Code is broken up into types of resources (EC2, IAM, etc), with files names after the resource type they are addressing. There is not a hard and fast system for what constitutes a resource type.

* If a resource can be tagged in AWS, it should be tagged with the Project and Organization

* All configurable options should be setup in the terraform.tfvars.example file. Variables should be clearly labeled and described.

* The assumption is that future users of all of our modules will not be proficient in AWS, Terraform, or the platforms we are building. As such, readability and clarity are paramount.
### Python
STLRDA follows [pep8](https://www.python.org/dev/peps/pep-0008/). There are many tools that will help with enforcement of this standard.