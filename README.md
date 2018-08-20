# Speedy Gonzales

Spark-based application to execute jobs faster and economically.

![Ariba, Ariba!](logo.png)

The idea (for now) is to create a webserver exposing a RESTful API that receives requests and
executes a combination of previously written PySpark scripts, all in a single EC2 instance.


## REST API

The REST API is provided by a tiny server built with Falcon. To start it, just do:

```bash
$ cd server
$ ./start.sh
```

### Endpoints and requests

#### Submitting a job

```php
POST /jobs/submit
{
    "options": {
        "s3": {
            "aws_access_key_id": "<AWS_ACCESS_KEY_ID>",
            "aws_secret_access_key": "<AWS_SECRET_ACCESS_KEY>"
        }
    },
    "steps": [
        ["function_0", "arg_0", "arg_1", ..., "arg_K"],
        ["function_1", "arg_0", "arg_1", ..., "arg_T"],
        ...
        ["function_N", "arg_0", "arg_1", ..., "arg_Z"]
    ]
}
```

As the name says, the field **options** is optional. The field **s3** in the example is needed only when interacting with S3. The field **steps** is a list of lists. Each of the internal lists has as first element a function name, with the arguments following. A unitary internal list represents a functions without arguments (e.g., collect). All functions are preoviously written, making the job completely parameterized.

#### Getting an element

```php
GET /jobs/pop
```

Reads the first element of the list, removing it. This endpoint must be used only by the Spark process.

#### Checking job status

```php
GET /jobs/status/{job_id}
```

Returns the status of the job given by the ID. These are:

- QUEUED
- RUNNING
- NOT_FOUND

### Testing

Execute `$ ./start.sh` and watch while two jobs are submitted and executed (one after the other). At the end, the script prints the files' contents.
