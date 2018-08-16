# speedy-gonzales

Spark-based (kind of) framework to execute DE jobs faster.

![Ariba, Ariba!](logo.png)

The idea (for now) is to create a RESTful webserver that receives orders from an Airflow task and
executes a combination of PySpark scripts.


## REST API

The REST API is provided by a tiny server. To start it, just do:

```bash
$ cd server
$ ./startup.sh
```

### Endpoints

- /spark - Starts and stops the Spark process;
- /jobs/submit - Submits a job.

#### Starting and stopping Spark process

```bash
POST /spark
{
    "command": <COMMAND>
}
```

where `<COMMAND>` may be **start** or **stop**.

#### Submitting a job

```bash
POST /jobs/submit
{
    "steps": [
        ["function_0", "arg_0", "arg_1", ..., "arg_K"],
        ["function_1", "arg_0", "arg_1", ..., "arg_T"],
        ...
        ["function_N", "arg_0", "arg_1", ..., "arg_Z"]
    ]
}
```

### Testing

Execute `$ ./testup.sh` and choose one of the options:

- start
- stop
- submit

## TODO

### Study the following sources

- [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver)
- [Spark Server](https://github.com/spark-server/spark-server)