# AccumulatingTransport

The `AccumulatingTransport` is a sophisticated OpenLineage transport that aggregates events before emitting them through an underlying transport. It provides powerful event aggregation capabilities with configurable strategies and emission policies.

## Overview

The AccumulatingTransport addresses several common use cases in lineage tracking:

1. **Parent-Child Run Aggregation**: Aggregate inputs and outputs from child runs into parent run events
2. **Dataset Merging**: Combine multiple events that update the same datasets with facet merging
3. **Batch Processing**: Collect events and emit them in batches to reduce transport overhead

## Architecture

### Core Components

- **Aggregation Strategies**: Define how events are grouped and combined
- **Emission Policies**: Determine when accumulated events should be emitted
- **Event Buffer**: Thread-safe storage for accumulated events with size limits
- **Underlying Transport**: Final destination for aggregated events

### Thread Safety

The AccumulatingTransport is fully thread-safe and can handle concurrent event emission from multiple threads. All internal state is protected by locks.

## Aggregation Strategies

### ParentChildStrategy (`parent_child`)

Aggregates child run inputs/outputs into parent run events based on the ParentRunFacet.

**Use Case**: You have a parent job that spawns multiple child jobs, and you want to see the complete lineage (all inputs and outputs) at the parent level.

**Configuration**:
```python
strategy_config = {
    "parent_run_facet_key": "parent",      # Key for ParentRunFacet in run.facets
    "preserve_child_events": False,        # Whether to also emit original child events
    "merge_duplicate_datasets": True,      # Merge datasets with same namespace/name
}
```

**How it works**:
1. Events with a ParentRunFacet are identified as child events
2. Child events are grouped by their parent run ID
3. When parent events are emitted, child datasets are merged into them
4. Duplicate datasets (same namespace/name) can be merged

**Example**:
```python
# Child events accumulate inputs/outputs
child_event = RunEvent(
    eventType=RunState.COMPLETE,
    run=Run(
        runId="child-123",
        facets={
            "parent": ParentRunFacet(
                run=Run(runId="parent-456"),
                job=Job(namespace="ns", name="parent-job")
            )
        }
    ),
    inputs=[InputDataset(namespace="db", name="raw_data")],
    outputs=[OutputDataset(namespace="db", name="processed_data")]
)

# Parent event gets enhanced with child data
parent_event = RunEvent(
    eventType=RunState.COMPLETE,
    run=Run(runId="parent-456"),
    job=Job(namespace="ns", name="parent-job"),
    inputs=[],  # Will include child inputs: raw_data
    outputs=[]  # Will include child outputs: processed_data
)
```

### MergeDatasetsStrategy (`merge_datasets`)

Merges dataset facets and metadata across events that operate on the same datasets.

**Use Case**: Multiple events update different facets of the same datasets, and you want a consolidated view.

**Configuration**:
```python
strategy_config = {
    "group_by_job": True,          # Group events by job
    "merge_run_facets": False,     # Whether to merge run facets too
}
```

**How it works**:
1. Events are grouped by job and the datasets they reference
2. Dataset facets are merged across events (last-writer-wins for conflicts)
3. The latest event serves as the base for the merged result

### BatchStrategy (`batch`)

Simple event collection without modification.

**Use Case**: Reduce transport overhead by sending events in batches.

**Configuration**:
```python
strategy_config = {
    "group_all": True,  # Group all events together
}
```

## Emission Policies

### TimeBasedPolicy (`time_based`)

Emit based on time intervals and event age.

```python
emission_config = {
    "trigger_interval": 30.0,  # Emit every 30 seconds
    "max_age": 300.0,          # Force emit if oldest event > 5 minutes
}
```

### CountBasedPolicy (`count_based`)

Emit when event count reaches threshold.

```python
emission_config = {
    "max_events": 100,     # Emit when 100 events accumulated
    "min_events": 1,       # Minimum events before considering emission
}
```

### SizeBasedPolicy (`size_based`)

Emit when accumulated data size reaches threshold.

```python
emission_config = {
    "max_size_bytes": 1024 * 1024,  # Emit when 1MB accumulated
}
```

### CompletionBasedPolicy (`completion_based`)

Emit when completion events (COMPLETE, FAIL, ABORT) are received.

```python
emission_config = {
    "completion_event_types": ["COMPLETE", "FAIL", "ABORT"],
    "parent_completion": True,  # For parent-child: emit on parent completion
}
```

### HybridPolicy (`hybrid`)

Combines multiple policies with AND/OR logic.

```python
emission_config = {
    "operator": "OR",  # "AND" or "OR"
    "policies": [
        {"type": "time_based", "trigger_interval": 30.0},
        {"type": "count_based", "max_events": 100},
        {"type": "completion_based", "completion_event_types": ["COMPLETE"]}
    ]
}
```

## Configuration Examples

### Parent-Child Aggregation

```python
from openlineage.client.transport.accumulating import AccumulatingConfig, AccumulatingTransport

config = AccumulatingConfig(
    # Aggregation strategy
    strategy="parent_child",
    strategy_config={
        "parent_run_facet_key": "parent",
        "preserve_child_events": False,
        "merge_duplicate_datasets": True,
    },

    # Emission policy
    emission_policy="completion_based",
    emission_config={
        "completion_event_types": ["COMPLETE", "FAIL", "ABORT"],
        "parent_completion": True
    },

    # Underlying transport
    transport={
        "type": "http",
        "url": "https://lineage.example.com/api/v1/lineage"
    },

    # Buffer management
    max_buffer_size=10000,
    cleanup_interval=60.0,

    # Error handling
    continue_on_aggregation_error=True,
    fallback_to_direct_emission=True
)

transport = AccumulatingTransport(config)
```

### Batch Processing

```python
config = AccumulatingConfig(
    strategy="batch",
    strategy_config={"group_all": True},

    emission_policy="hybrid",
    emission_config={
        "operator": "OR",
        "policies": [
            {"type": "count_based", "max_events": 50},
            {"type": "time_based", "trigger_interval": 60.0},
            {"type": "size_based", "max_size_bytes": 512000}
        ]
    },

    transport={"type": "console"},
    max_buffer_size=1000
)
```

### Dataset Facet Merging

```python
config = AccumulatingConfig(
    strategy="merge_datasets",
    strategy_config={
        "group_by_job": True,
        "merge_run_facets": True
    },

    emission_policy="time_based",
    emission_config={
        "trigger_interval": 10.0,
        "max_age": 60.0
    },

    transport={"type": "file", "file": "/tmp/lineage.jsonl"}
)
```

## Usage Patterns

### With OpenLineageClient

```python
from openlineage.client import OpenLineageClient

# Configure client with accumulating transport
client = OpenLineageClient(transport=transport)

# Emit events normally - they'll be accumulated and aggregated
client.emit(run_event)
```

### Manual Flush

```python
# Force emission of all accumulated events
transport.flush()

# Get accumulation statistics
stats = transport.get_stats()
print(f"Accumulated: {stats['accumulated_events']} events")
```

### Graceful Shutdown

```python
# Close transport, flushing remaining events
success = transport.close(timeout=30.0)
if not success:
    print("Warning: Some events may not have been emitted")
```

## Error Handling

The AccumulatingTransport provides robust error handling:

### Aggregation Errors

```python
config = AccumulatingConfig(
    # ... other config ...
    continue_on_aggregation_error=True,     # Continue processing on errors
    fallback_to_direct_emission=True       # Emit original events if aggregation fails
)
```

### Transport Errors

Errors from the underlying transport are propagated normally. The accumulating transport doesn't add additional error handling for the final emission step.

### Buffer Overflow

```python
config = AccumulatingConfig(
    # ... other config ...
    max_buffer_size=10000,    # Limit memory usage
    cleanup_interval=60.0     # Regular cleanup of old events
)
```

When the buffer exceeds the size limit, the oldest events are automatically removed.

## Performance Considerations

### Memory Usage

- Event buffer size is configurable and enforced
- Events are serialized for size estimation
- Regular cleanup removes old events
- Buffer size should be tuned based on event rate and emission frequency

### CPU Usage

- Aggregation is performed only when emission is triggered
- Thread-safe operations use locks but are optimized for minimal contention
- Size estimation is done once per event

### Network Usage

- Reduces network calls by batching emissions
- Larger payloads per request vs. many small requests
- Underlying transport configuration still applies (timeouts, retries, etc.)

## Monitoring and Observability

### Statistics

```python
stats = transport.get_stats()
# Returns:
# {
#     "accumulated_groups": 5,
#     "accumulated_events": 150,
#     "accumulated_size_bytes": 245760,
#     "strategy": "parent_child",
#     "emission_policy": "hybrid",
#     "underlying_transport": {...}  # If underlying transport provides stats
# }
```

### Logging

The transport provides detailed logging at different levels:

- `INFO`: Emission events, configuration
- `DEBUG`: Event accumulation, group management
- `WARNING`: Buffer overflows, forced emissions
- `ERROR`: Aggregation failures, transport errors

Configure logging to monitor accumulation behavior:

```python
import logging
logging.getLogger("openlineage.client.transport.accumulating").setLevel(logging.DEBUG)
```

## Limitations

1. **Memory Usage**: Events are kept in memory until emitted. Large datasets or high event rates may require careful tuning.

2. **Event Ordering**: While events within a group maintain relative order, ordering across groups is not guaranteed.

3. **Exactly-Once Delivery**: The transport provides at-most-once delivery semantics. If the process crashes, accumulated events may be lost.

4. **Complex Facet Merging**: The current implementation uses simple facet merging (last-writer-wins). Complex semantic merging is not supported.

5. **Run State Consistency**: When aggregating parent-child events, the transport doesn't validate run state transitions.

## Best Practices

1. **Choose Appropriate Buffer Sizes**: Balance memory usage with emission frequency.

2. **Monitor Statistics**: Use `get_stats()` to understand accumulation patterns.

3. **Handle Shutdown Gracefully**: Always call `close()` or `flush()` before shutdown.

4. **Test Aggregation Logic**: Use the provided test utilities to validate your specific use cases.

5. **Configure Error Handling**: Decide whether to fail fast or continue with fallback emission.

6. **Tune Emission Policies**: Start with simple policies and adjust based on observed behavior.

## Examples

See the `examples/accumulating_transport_demo.py` file for complete working examples of all strategies and policies.

## Testing

The transport includes comprehensive tests in `tests/test_accumulating_transport.py`. Run tests with:

```bash
cd client/python
python -m pytest tests/test_accumulating_transport.py -v
```
