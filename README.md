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

##### Request

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
        ["function_0", arg_0, arg_1, ..., arg_K],
        ["function_1", arg_0, arg_1, ..., arg_T],
        ...
        ["function_N", arg_0, arg_1, ..., arg_Z]
    ]
}
```

As the name says, the field **options** is optional. The field **s3** in the example is needed only when interacting with S3. The field **steps** is a list of lists. Each of the internal lists has as first element a function name, with the arguments following. A unitary internal list represents a function without arguments (e.g., collect). All functions are preoviously written, making the job completely parameterized. This allows the client to execute a Spark job using the functions provided by the server, without sending any code (and avoiding security issues and incompatibilities).

##### Response

```php
{
    "job_id": "<JOB_ID_IN_UUID4_FORMAT>"
}
```

#### Getting an element

##### Request

```php
GET /jobs/pop
```

Reads the first element of the list, removing it. This endpoint must be used only by the Spark process.

##### Response

```php
{
    "job_id": "<JOB_ID_IN_UUID4_FORMAT>",
    "options": <OBJECT>,
    "steps": <LIST_OF_LISTS>
}
```

The response for this request is the body of the job submission described above.

#### Checking job status

##### Request

```php
GET /jobs/status/{job_id}
```

##### Response

```php
{
    "status": "<JOB_STATUS>"
}
```

where the status is one of:

- **QUEUED**
- **RUNNING**
- **NOT_FOUND**

### Testing

Execute `$ ./start.sh` and watch while two jobs are submitted and executed (one after the other). At the end, the script prints the files' contents.
