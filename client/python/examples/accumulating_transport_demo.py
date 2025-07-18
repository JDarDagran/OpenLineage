#!/usr/bin/env python3
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Example demonstrating the AccumulatingTransport for OpenLineage events.

This example shows how to use the AccumulatingTransport to aggregate child run
events into parent run events, which is useful for scenarios where you want to
combine lineage information from multiple child processes into a single parent view.
"""

import time
import uuid
from datetime import datetime, timezone

from openlineage.client.facet_v2 import parent_run
from openlineage.client.generated.base import Job, Run
from openlineage.client.run import Dataset, InputDataset, OutputDataset, RunEvent, RunState
from openlineage.client.transport.accumulating import AccumulatingConfig, AccumulatingTransport


def create_parent_run_facet(parent_run_id: str, parent_job_name: str, parent_namespace: str):
    """Create a ParentRunFacet for child events."""
    return parent_run.ParentRunFacet(
        run=parent_run.Run(runId=parent_run_id),
        job=parent_run.Job(namespace=parent_namespace, name=parent_job_name),
    )


def create_sample_dataset(name: str, namespace: str = "example") -> Dataset:
    """Create a sample dataset."""
    return Dataset(
        namespace=namespace,
        name=name,
        facets={
            "schema": {
                "_producer": "example",
                "_schemaURL": "https://example.com/schema.json",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "STRING"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                ],
            }
        },
    )


def demo_parent_child_aggregation():
    """Demonstrate parent-child event aggregation."""
    print("=== Parent-Child Aggregation Demo ===")

    # Configure accumulating transport
    config = AccumulatingConfig(
        # Use parent-child aggregation strategy
        strategy="parent_child",
        strategy_config={
            "parent_run_facet_key": "parent",
            "preserve_child_events": False,  # Only emit aggregated parent events
            "merge_duplicate_datasets": True,
        },
        # Emit when completion events are received
        emission_policy="completion_based",
        emission_config={"completion_event_types": ["COMPLETE", "FAIL", "ABORT"], "parent_completion": True},
        # Use console transport for demo (could be HTTP, file, etc.)
        transport={"type": "console"},
        # Error handling
        continue_on_aggregation_error=True,
        fallback_to_direct_emission=True,
    )

    transport = AccumulatingTransport(config)

    # Generate some example data
    parent_run_id = str(uuid.uuid4())
    child_run_id_1 = str(uuid.uuid4())
    child_run_id_2 = str(uuid.uuid4())

    parent_job = Job(namespace="example", name="data_pipeline")
    child_job_1 = Job(namespace="example", name="extract_data")
    child_job_2 = Job(namespace="example", name="transform_data")

    # Create parent run facet for child events
    parent_facet = create_parent_run_facet(parent_run_id, "data_pipeline", "example")

    # Sample datasets
    input_dataset_1 = InputDataset(**create_sample_dataset("raw_data_1").__dict__)
    input_dataset_2 = InputDataset(**create_sample_dataset("raw_data_2").__dict__)
    intermediate_dataset = OutputDataset(**create_sample_dataset("intermediate_data").__dict__)
    final_dataset = OutputDataset(**create_sample_dataset("final_data").__dict__)

    print(f"Parent run ID: {parent_run_id}")
    print(f"Child run IDs: {child_run_id_1}, {child_run_id_2}")
    print()

    # === Child Run 1: Data Extraction ===
    print("Emitting child run 1 events (extract_data)...")

    # Child 1 START
    child_1_start = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_id_1, facets={"parent": parent_facet}),
        job=child_job_1,
        inputs=[input_dataset_1],
        outputs=[],
        producer="example_producer",
    )
    transport.emit(child_1_start)

    # Child 1 COMPLETE
    child_1_complete = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_id_1, facets={"parent": parent_facet}),
        job=child_job_1,
        inputs=[input_dataset_1],
        outputs=[intermediate_dataset],
        producer="example_producer",
    )
    transport.emit(child_1_complete)

    # === Child Run 2: Data Transformation ===
    print("Emitting child run 2 events (transform_data)...")

    # Child 2 START
    child_2_start = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_id_2, facets={"parent": parent_facet}),
        job=child_job_2,
        inputs=[InputDataset(**intermediate_dataset.__dict__), input_dataset_2],
        outputs=[],
        producer="example_producer",
    )
    transport.emit(child_2_start)

    # Child 2 COMPLETE
    child_2_complete = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_id_2, facets={"parent": parent_facet}),
        job=child_job_2,
        inputs=[InputDataset(**intermediate_dataset.__dict__), input_dataset_2],
        outputs=[final_dataset],
        producer="example_producer",
    )
    transport.emit(child_2_complete)

    # === Parent Run Events ===
    print("Emitting parent run events...")

    # Parent START (will accumulate child data when emitted)
    parent_start = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=parent_run_id),
        job=parent_job,
        inputs=[],  # Will be populated with child inputs
        outputs=[],
        producer="example_producer",
    )
    transport.emit(parent_start)

    # Parent COMPLETE (triggers emission of accumulated events)
    print("Emitting parent COMPLETE event (this will trigger aggregation)...")
    parent_complete = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=parent_run_id),
        job=parent_job,
        inputs=[],  # Will be populated with child inputs
        outputs=[],  # Will be populated with child outputs
        producer="example_producer",
    )
    transport.emit(parent_complete)

    print("\n=== Aggregation Complete ===")
    print("The emitted events above show the parent run with aggregated inputs/outputs from children")
    print("Expected aggregated inputs: raw_data_1, intermediate_data, raw_data_2")
    print("Expected aggregated outputs: intermediate_data, final_data")

    # Get stats
    stats = transport.get_stats()
    print(f"\nTransport stats: {stats}")

    # Close transport
    transport.close()


def demo_batch_aggregation():
    """Demonstrate simple batch aggregation."""
    print("\n\n=== Batch Aggregation Demo ===")

    # Configure for batching
    config = AccumulatingConfig(
        strategy="batch",
        strategy_config={"group_all": True},
        # Emit after 3 events or 5 seconds
        emission_policy="hybrid",
        emission_config={
            "operator": "OR",
            "policies": [
                {"type": "count_based", "max_events": 3},
                {"type": "time_based", "trigger_interval": 5.0},
            ],
        },
        transport={"type": "console"},
    )

    transport = AccumulatingTransport(config)

    # Emit several events
    for i in range(5):
        event = RunEvent(
            eventType=RunState.START if i % 2 == 0 else RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace="batch_demo", name=f"job_{i}"),
            inputs=[],
            outputs=[],
            producer="batch_producer",
        )

        print(f"Emitting event {i+1}/5...")
        transport.emit(event)

        if i == 2:  # After 3rd event, should trigger emission
            print("  ^^ Batch should be emitted here (count threshold reached)")

        time.sleep(0.5)

    print("Emitting remaining events...")
    # Close will flush remaining events
    transport.close()


def demo_dataset_merging():
    """Demonstrate dataset merging aggregation."""
    print("\n\n=== Dataset Merging Demo ===")

    config = AccumulatingConfig(
        strategy="merge_datasets",
        strategy_config={"group_by_job": True, "merge_run_facets": True},
        emission_policy="count_based",
        emission_config={"max_events": 3},
        transport={"type": "console"},
    )

    transport = AccumulatingTransport(config)

    # Create events that operate on the same dataset
    shared_dataset = create_sample_dataset("shared_table")
    job = Job(namespace="merge_demo", name="update_table")

    for i in range(3):
        # Each event adds different facets to the same dataset
        dataset_with_facets = OutputDataset(
            namespace=shared_dataset.namespace,
            name=shared_dataset.name,
            facets={
                **shared_dataset.facets,
                f"custom_facet_{i}": {
                    "_producer": "merge_demo",
                    "_schemaURL": "https://example.com/custom.json",
                    "value": f"facet_value_{i}",
                },
            },
        )

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4()), facets={f"run_facet_{i}": {"data": f"run_data_{i}"}}),
            job=job,
            inputs=[],
            outputs=[dataset_with_facets],
            producer="merge_producer",
        )

        print(f"Emitting merge event {i+1}/3 (adds custom_facet_{i})...")
        transport.emit(event)

    print("Events merged - the final dataset should contain all custom facets")
    transport.close()


if __name__ == "__main__":
    print("OpenLineage AccumulatingTransport Demo")
    print("=====================================")

    try:
        demo_parent_child_aggregation()
        demo_batch_aggregation()
        demo_dataset_merging()

        print("\n=== Demo Complete ===")
        print("All aggregation strategies demonstrated successfully!")

    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback

        traceback.print_exc()
