# Log Analyzer CLI

A command-line tool to analyze Squid Proxy access logs and extract useful metrics such as:

* Most frequent IP
* Least frequent IP
* Events per second
* Total amount of bytes exchanged

The tool is designed with **extensibility, fault tolerance, and scalability** in mind and follows a modular architecture so that new analytics operations can easily be added.

---

# Features

* CLI interface
* Streaming log parser (memory efficient)
* Multiple input files
* JSON output
* Optional Spark support for large datasets
* Docker support

---

# Requirements

* Python >= 3.11
* Java JDK >= 11 (17 recommended) — required only if using `--use_spark`

---

# Log Format

The tool expects **Squid Proxy access logs** like the dataset provided in the exercise:

https://www.secrepo.com/squid/access.log.gz

Example log entry:

```
1157689324.156   1372 10.105.21.199 TCP_MISS/200 399 GET http://www.google-analytics.com/__utm.gif? badeyek DIRECT/66.102.9.147 image/gif
```

Parsed fields:

| Field | Description                     |
| ----- | ------------------------------- |
| 1     | Timestamp (seconds since epoch) |
| 2     | Response header size            |
| 3     | Client IP                       |
| 4     | HTTP response code              |
| 5     | Response size                   |
| 6     | HTTP method                     |
| 7     | URL                             |
| 8     | Username                        |
| 9     | Access type / destination IP    |
| 10    | Response content type           |

---

# Installation

Clone the repository:

```
git clone <repository-url>
cd log-analyzer
```

Install dependencies:

```
pip install -r requirements.txt
```

Or install as a package:

```
pip install .
```

### Java setup (only if using `--use_spark`)

It is recommended to verify that:

* `JAVA_HOME` points to your JDK installation directory
* Your `PATH` includes `%JAVA_HOME%\\bin` (Windows)

Quick checks:

```
java -version
```

---

# Usage

Basic CLI structure:

```
log-analyzer \
  --input <log_file> \
  --output <output_file> \
  [operations]
```

Example:

```
log-analyzer \
  --input access.log \
  --output result.json \
  --mfip \
  --eps
```



---

# CLI Arguments

### Required

| Argument   | Description                                                                |
| ---------- |----------------------------------------------------------------------------|
| `--input`  | Path to one or more log files (the .log or .log.gz extensions are accepted |
| `--output` | Output JSON file                                                           |

Example:

```
--input file1.log file2.log.gz
```

---

### Operations

| Option    | Description           |
| --------- | --------------------- |
| `--mfip`  | Most frequent IP      |
| `--lfip`  | Least frequent IP     |
| `--eps`   | Events per second     |
| `--bytes` | Total bytes exchanged |

Example:

```
--mfip --bytes
```

### Optional Spark Support

| Argument      | Description |
|---------------|-------------|
| `--use_spark` | Enable Apache Spark for data processing instead of the default local processing. |


---

# Output

Results are written in **JSON format** and include only operations marked to be executed.

Example:

```json
{
    "status_code": 200,
    "message": "Proccesing Successful",
    "processing_time": "0:03:43.347829",
    "operations": [
        "mfip",
        "lfip",
        "eps",
        "bytes"
    ],
    "result": {
        "MFIP": {
            "value": "127.0.0.1",
            "count": 100
        },
        "LFIP": {
            "value": "127.0.0.1",
            "count": 1
        },
        "EPS": {
            "max": 12,
            "mean": 3.35325,
            "min": 1
        },
        "BYTES": {
            "count": 36343
        },
        "data_process_time": "0:03:25.851998"
    }
}
```

---

# Running with Docker

Build the image:

```
docker build -t log-analyzer .
```

Run the container:

```
docker run log-analyzer \
  --input /data/access.log \
  --output /data/result.json \
  --mfip
```

You may mount volumes to provide log files:

```
docker run -v $(pwd):/data log-analyzer \
  --input /data/access.log \
  --output /data/result.json \
  --mfip
```

---

# Assumptions

* Log files follow the Squid Proxy access log format.
* Log lines that cannot be parsed are skipped.
* Large files can optionally be processed with Spark.
