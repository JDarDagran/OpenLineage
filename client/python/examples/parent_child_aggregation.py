#!/usr/bin/env python3
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Parent-Child Run Aggregation Example

This example demonstrates the specific use case mentioned in the request:
"I want to aggregate inputs and outputs of all child runs and emit only
events for parent run (START and COMPLETE)"

This shows how child runs can contribute their lineage data to a parent run,
providing a consolidated view of the entire workflow's data dependencies.
"""

import uuid
from datetime import datetime, timezone

from openlineage.client.facet_v2 import parent_run
from openlineage.client.generated.base import Job, Run
from openlineage.client.run import InputDataset, OutputDataset, RunEvent, RunState
from openlineage.client.transport.accumulating import AccumulatingConfig, AccumulatingTransport


def main():
    """Demonstrate parent-child aggregation for the specific use case."""
    print("=== Parent-Child Run Aggregation Example ===")
    print("Use Case: Aggregate child run lineage into parent events")
    print()

    # Configure the accumulating transport for parent-child aggregation
    config = AccumulatingConfig(
        # Use parent-child strategy
        strategy="parent_child",
        strategy_config={
            "preserve_child_events": False,  # Only emit parent events (not children)
            "merge_duplicate_datasets": True,
        },
        # Emit when parent run completes
        emission_policy="completion_based",
        emission_config={"completion_event_types": ["COMPLETE", "FAIL", "ABORT"], "parent_completion": True},
        # Use console transport to see the results
        transport={"type": "console"},
    )

    transport = AccumulatingTransport(config)

    # === Scenario Setup ===
    # Parent workflow: Data Processing Pipeline
    # Child 1: Extract data from source A
    # Child 2: Extract data from source B
    # Child 3: Transform and combine data

    parent_run_id = str(uuid.uuid4())
    child_run_ids = [str(uuid.uuid4()) for _ in range(3)]

    parent_job = Job(namespace="pipeline", name="data_processing_workflow")
    child_jobs = [
        Job(namespace="pipeline", name="extract_source_a"),
        Job(namespace="pipeline", name="extract_source_b"),
        Job(namespace="pipeline", name="transform_and_combine"),
    ]

    # Create parent run facet for child events
    parent_facet = parent_run.ParentRunFacet(
        run=parent_run.Run(runId=parent_run_id),
        job=parent_run.Job(namespace="pipeline", name="data_processing_workflow"),
    )

    print(f"Parent Run ID: {parent_run_id}")
    print(f"Child Run IDs: {child_run_ids}")
    print()

    # === Child Run 1: Extract from Source A ===
    print("1. Child run 1 (extract_source_a) - START")
    child1_start = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_ids[0], facets={"parent": parent_facet}),
        job=child_jobs[0],
        inputs=[
            InputDataset(
                namespace="external_db",
                name="source_a_table",
                facets={"schema": {"fields": [{"name": "id", "type": "int"}]}},
            )
        ],
        outputs=[],
        producer="pipeline_system",
    )
    transport.emit(child1_start)

    print("   Child run 1 (extract_source_a) - COMPLETE")
    child1_complete = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_ids[0], facets={"parent": parent_facet}),
        job=child_jobs[0],
        inputs=[
            InputDataset(
                namespace="external_db",
                name="source_a_table",
                facets={"schema": {"fields": [{"name": "id", "type": "int"}]}},
            )
        ],
        outputs=[
            OutputDataset(
                namespace="staging",
                name="extracted_a",
                facets={
                    "schema": {
                        "fields": [{"name": "id", "type": "int"}, {"name": "data_a", "type": "string"}]
                    }
                },
            )
        ],
        producer="pipeline_system",
    )
    transport.emit(child1_complete)

    # === Child Run 2: Extract from Source B ===
    print("2. Child run 2 (extract_source_b) - START")
    child2_start = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_ids[1], facets={"parent": parent_facet}),
        job=child_jobs[1],
        inputs=[
            InputDataset(
                namespace="file_system",
                name="source_b_files",
                facets={"schema": {"fields": [{"name": "timestamp", "type": "datetime"}]}},
            )
        ],
        outputs=[],
        producer="pipeline_system",
    )
    transport.emit(child2_start)

    print("   Child run 2 (extract_source_b) - COMPLETE")
    child2_complete = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_ids[1], facets={"parent": parent_facet}),
        job=child_jobs[1],
        inputs=[
            InputDataset(
                namespace="file_system",
                name="source_b_files",
                facets={"schema": {"fields": [{"name": "timestamp", "type": "datetime"}]}},
            )
        ],
        outputs=[
            OutputDataset(
                namespace="staging",
                name="extracted_b",
                facets={
                    "schema": {
                        "fields": [
                            {"name": "timestamp", "type": "datetime"},
                            {"name": "data_b", "type": "string"},
                        ]
                    }
                },
            )
        ],
        producer="pipeline_system",
    )
    transport.emit(child2_complete)

    # === Child Run 3: Transform and Combine ===
    print("3. Child run 3 (transform_and_combine) - START")
    child3_start = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_ids[2], facets={"parent": parent_facet}),
        job=child_jobs[2],
        inputs=[
            InputDataset(namespace="staging", name="extracted_a", facets={}),
            InputDataset(namespace="staging", name="extracted_b", facets={}),
        ],
        outputs=[],
        producer="pipeline_system",
    )
    transport.emit(child3_start)

    print("   Child run 3 (transform_and_combine) - COMPLETE")
    child3_complete = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=child_run_ids[2], facets={"parent": parent_facet}),
        job=child_jobs[2],
        inputs=[
            InputDataset(namespace="staging", name="extracted_a", facets={}),
            InputDataset(namespace="staging", name="extracted_b", facets={}),
        ],
        outputs=[
            OutputDataset(
                namespace="warehouse",
                name="combined_data",
                facets={
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "int"},
                            {"name": "data_a", "type": "string"},
                            {"name": "timestamp", "type": "datetime"},
                            {"name": "data_b", "type": "string"},
                        ]
                    }
                },
            )
        ],
        producer="pipeline_system",
    )
    transport.emit(child3_complete)

    # === Parent Run Events ===
    print("4. Parent run (data_processing_workflow) - START")
    parent_start = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=parent_run_id),
        job=parent_job,
        inputs=[],  # Will be populated with aggregated child inputs
        outputs=[],
        producer="pipeline_system",
    )
    transport.emit(parent_start)

    print("5. Parent run (data_processing_workflow) - COMPLETE")
    print("   >>> This will trigger aggregation and emission <<<")
    print()

    parent_complete = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=parent_run_id),
        job=parent_job,
        inputs=[],  # Will be populated with aggregated child inputs
        outputs=[],  # Will be populated with aggregated child outputs
        producer="pipeline_system",
    )
    transport.emit(parent_complete)

    print()
    print("=== Expected Aggregated Result ===")
    print("The emitted parent events should contain:")
    print("INPUTS (from all children):")
    print("  - external_db.source_a_table (from child 1)")
    print("  - file_system.source_b_files (from child 2)")
    print("  - staging.extracted_a (from child 3)")
    print("  - staging.extracted_b (from child 3)")
    print()
    print("OUTPUTS (from all children):")
    print("  - staging.extracted_a (from child 1)")
    print("  - staging.extracted_b (from child 2)")
    print("  - warehouse.combined_data (from child 3)")
    print()
    print("This gives you a complete lineage view at the parent level!")

    # Get final stats
    stats = transport.get_stats()
    print(f"\nFinal stats: {stats}")

    # Clean shutdown
    transport.close()


if __name__ == "__main__":
    main()
