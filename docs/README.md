#REDB-Workflows
While availability is a critical first step in using data to impact communities, it is just as important, if not more so, that the data be _useable_. Data that is highly fragmented, lacks standardization, in difficult to use formats, and/or lacking context and documentation is extremely difficult to build tools and analysis off of.

To address this need, the Saint Louis Regional Data Alliance is building the Regional Entity Database (REDB), a unified database of land parcel data. Pulling data from numerous local government departments and with a schema designed to be understandable by the average citizen, REDB will serve as single source of truth for parcel data.

REDB is being built entirely in AWS, with an eye on being reproducible by other organizations and municipalities. To that end, all code associated with this project is being made available on Github, in three repositories.

<img src="REDB ELT Workflow.jpg" width="900">

* [REDB-Platform](https://github.com/stlrda/REDB-Platform) contains a Terraform module that builds an Airflow cluster that will run and manage the ETL processes that will create, maintain, and update REDB. Run in isolation, this will create a functioning Airflow cluster and nothing more.

* [REDB-Infrastructure](https://github.com/stlrda/REDB-Infrastructure) builds on the above. It contains Terraform code that pulls in the module above, and adds an S3 bucket (for the Airflow workers to use in their processes) and an Aurora PostGreSQL database (REDB). Run in isolation, this will creating everything an organization needs to start building out their own data workflows.

* [REDB-Workflows](https://github.com/stlrda/REDB-Workflows) contains the DAGs, scripts, and other files that the Airflow cluster will run. This piece is the most Saint Louis specific, but is being provided to serve as an example for other municipalities, and to provide visibility into REDB's working processes.
### This Repository
This repository contains the DAG Files and Scripts used to automate DKAN. The contents of this repository are meant to be within the infrastructure outlined above.

Automating the processing of data from a wide variety of government sources into a format that can be added to this database involves many steps. These steps can be saved in script files to be run on new data, but the running of these script files is still a complex task. To effectively import the data, these steps must applied to the data periodically and in sequences. Task scheduling services such as chron, which are provided with operating systems, are very limited. Apache Airflow provides a flexible and effective method to automate this workflow of menial data processing tasks.
Apache Airflow is a workflow manager based on DAGs, directed acyclic graphs. In plain English, these are structures akin to flow charts which define the tasks that must be performed and specifically map out the path of flow from one task to another within the process. The entire workflow's DAG, though it is also called a Pipeline in Airflow terminology, is defined within a Python script file. You can learn to make this file with Apache's tutorial here:

[https://airflow.apache.org/docs/stable/tutorial.html](https://airflow.apache.org/docs/stable/tutorial.html)
