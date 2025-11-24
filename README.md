# mock-graph-events-generator

This script generates mock event data from a simulated Directed Acyclic Graph (DAG) execution for testing purposes. The output is a `.ndjson` file containing a sequence of events that mimic a real graph execution lifecycle.

## Features

- Generates a series of graph-related events such as `GRAPH_ACCEPTED`, `VERTEX_ACCEPTED`, `EDGE_CALCULATED`, `VERTEX_INVOKED`, `GRAPH_COMPLETED`, and more.
- Simulates complex scenarios like vertex and edge remoting, including serialization and deserialization events.
- Uses realistic-looking mock data for event properties, powered by the `Faker` library.
- Allows customization of the graph size through command-line arguments.

## Prerequisites

The script requires the following CSV files to be in the same directory:
- `vertex_values.csv`: Contains possible types and string representations for graph vertices.
- `edge_values.csv`: Contains possible types and string representations for graph edges.

You will also need to install the `Faker` python package:
```bash
pip install Faker
```

## Usage

To run the script, execute it from your terminal.

You can specify the number of vertices to generate in the graph using the `--num-vertices` argument:
```bash
python events_generator.py --num-vertices 500
```

If you run the script without any arguments, it will generate a graph with a random number of vertices between 100 and 10,000.
```bash
python events_generator.py
```

The script will produce a file named `graph_events_<timestamp>.ndjson` in the same directory, where `<timestamp>` is the current date and time.
