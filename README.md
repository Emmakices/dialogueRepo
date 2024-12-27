# Data Engineering Code Challenge

Thank you for your interest in Dialogue Engineering! This code challenge is an opportunity for you to show us what you can do, and will be used as a jumping-off point for later interviews. 

## Setup

#### Prerequisites

Required: A recent version of [Docker Desktop](https://docs.docker.com/desktop/) should be installed and running on your computer, including `docker` and `docker compose` commands.

Optional:
- `make`, so that you can take advantage of the example commands in the Makefile. This challenge was tested with GNU Make 3.81, but other versions should also work.
- [DuckDB](https://duckdb.org/docs/installation/index), so that you can interact with the duckdb file as you develop your solution.

#### Toubib API

You are provided with the Toubib API, a fake Electronic Medical Record (EMR) system. It is used to manage a collection of "patients" with create, update, read, and list operations.

You'll need to run the system locally in order to develop and test your solution. The included `docker-compose.yml` encodes everything you need to run it locally. 

To start the API server, use:

```
docker compose up
```

Once the container is created and attached, double check that everything is working by navigating to:
- the API documentation: http://0.0.0.0:8000/docs 
- the OpenAPI specification: http://0.0.0.0:8000/openapi.json


## Challenge

Write a connector that replicates the `patients` resource from the API to a local duckdb database for use in an analytics workload. The connector should meet the following requirements:

1. It can be run on a periodic basis (such as daily or hourly) in order to continuously update the data in the destination.
2. When run on a periodic basis, it captures historical changes to the source API data over time in the destination database. 
3. It continues to run efficiently as the size of the data set scales into the millions of records.

Package the connector as a Docker image by providing a Dockerfile that incorporates all dependencies of your solution.

#### Template Dockerfile

A template Dockerfile and example build/run commands have been provided for you to start with. To check that it's working:

**Build:**
```
make build
```
You should be able to see the image, for example:
```
$ docker image list
REPOSITORY         TAG       IMAGE ID       CREATED          SIZE
toubib_connector   latest    180e37132e0b   14 seconds ago   204MB
```

**Initialize the duckdb database:**
```
make init-db
```

You should be able to see the example table created by `init-db.py`:
```
Successfully connected to the destination DB.
SHOW ALL TABLES
┌─────────────┬─────────┬─────────┬───┬──────────────────────┬───────────┐
│  database   │ schema  │  name   │ … │     column_types     │ temporary │
│   varchar   │ varchar │ varchar │   │      varchar[]       │  boolean  │
├─────────────┼─────────┼─────────┼───┼──────────────────────┼───────────┤
│ destination │ main    │ example │ … │ [INTEGER, VARCHAR,…  │ false     │
├─────────────┴─────────┴─────────┴───┴──────────────────────┴───────────┤
│ 1 rows                                             6 columns (5 shown) │
└────────────────────────────────────────────────────────────────────────┘
```

**Run the toubib-connector:**
```
make run
```

You should be able to see that the container was able to reach the API successfully over the docker bridge network:
```
GET http://toubib:8000/health:
"OK"
```



#### Evaluation

We expect to be able to:
1. Run `make build` to build your docker image.
2. Run `make init-db` to instantiate the DB.
3. Run `make run` to run your connector against the API, then inspect the results in the database.
4. Update the source data, then re-do step 3.

You're welcome to make changes to the make commands or to provide different ones; if you do, please provide us with instructions about how to execute each of the above steps 1-3.

We will evaluate:
- whether the solution meets the challenge requirements;
- the maintainability of the code that implements the connector;
- how you've chosen to implement regression tests for your solution; and
- the database schema of the resulting duckdb instance.


#### Scope

You are welcome to use whatever tools, libraries, or other resources that are available to you to solve the challenge, as long as the whole solution can be submitted to the git repository and we can run it using docker after you submit. If you know Python please use it; otherwise you can use whatever language you're most comfortable with!
