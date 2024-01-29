# Dump Buckets

## Overview

Dump Buckets is a database cloud backup & retention solution.
The project includes a set of utilities to back up data from different sources and pipe the output to a storage bucket.

# Exporters

Exporters are used for various types of exports, which the container allows.

### Execute

The `execute` command is used to wrap underlying libraries and to execute commands directly on the container.
The output is then piped to the storage bucket.

### Contentful

Exports contentful data for the selected workspaces

### Github

Export github repositories using the web API with HTTP requests.

### MongoDB

Exports mongo databases to the specified bucket.

## Usage

- To run the `execute` command: ``/dumpb execute -- echo "hello"``
- To run the `contentful` command: (Add usage details)
- To run the `github` command: (Add usage details)
- To run the `mongo` command: (Add usage details)

## License

You can check out the full license [here](LICENSE).

## Contact

If you have any questions, feel free to [contact](mailto:stefan.martinov@bestbytes.com) me.