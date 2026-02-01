#!/usr/bin/env python3
"""
Test script for MockClaimXAPIClient.

Quick verification that the mock API client loads fixtures correctly
and returns schema-valid data.

Usage:
    python -m kafka_pipeline.simulation.test_claimx_api_mock
"""

import asyncio
from pathlib import Path

from kafka_pipeline.simulation.claimx_api_mock import MockClaimXAPIClient


async def test_mock_api():
    """Test the mock API client with both fixture and generated data."""

    print("=" * 80)
    print("Testing MockClaimXAPIClient")
    print("=" * 80)

    # Initialize mock client
    fixtures_dir = Path(__file__).parent / "fixtures"
    client = MockClaimXAPIClient(fixtures_dir=fixtures_dir)

    print(f"\nFixtures directory: {fixtures_dir}")
    print(f"Projects loaded: {len(client._projects)}")
    print(f"Media projects: {len(client._media)}")
    print(f"Tasks loaded: {len(client._tasks)}")
    print(f"Contact projects: {len(client._contacts)}")

    # Test 1: Get project from fixtures
    print("\n" + "-" * 80)
    print("Test 1: Get project from fixtures")
    print("-" * 80)

    project_id = "proj_1A2B3C4D5E6F"
    project = await client.get_project(int(project_id.split("_")[1], 36))
    print(f"\nProject: {project_id}")
    print(f"Name: {project['project_name']}")
    print(f"Policyholder: {project['policyholder_name']}")
    print(f"Status: {project['status']}")

    # Test 2: Get media from fixtures
    print("\n" + "-" * 80)
    print("Test 2: Get media from fixtures")
    print("-" * 80)

    media = await client.get_project_media(int(project_id.split("_")[1], 36))
    print(f"\nMedia count: {len(media)}")
    for m in media[:3]:
        print(f"  - {m['file_name']} ({m['file_size']} bytes)")
        print(f"    URL: {m['download_url']}")

    # Test 3: Get task from fixtures (iTel Cabinet task)
    print("\n" + "-" * 80)
    print("Test 3: Get iTel Cabinet task from fixtures")
    print("-" * 80)

    assignment_id = "task_assign_10001"
    task = await client.get_custom_task(int(assignment_id.split("_")[2]))
    print(f"\nTask: {assignment_id}")
    print(f"Task ID: {task['task_id']} (iTel Cabinet)")
    print(f"Task Name: {task['task_name']}")
    print(f"Status: {task['status']}")

    if "form_response" in task and task["form_response"]:
        form = task["form_response"]
        print(f"Form ID: {form['form_id']}")
        print(f"Form Responses: {len(form['responses'])}")
        print(f"Form Attachments: {len(form['attachments'])}")

        # Show some form responses
        print("\nSample form responses:")
        for resp in form["responses"][:5]:
            print(f"  - {resp['question_key']}: {resp['answer']}")

        # Show attachment URLs
        print("\nForm attachment URLs:")
        for att in form["attachments"][:3]:
            print(f"  - {att['file_name']}: {att['download_url']}")

    # Test 4: Get contacts from fixtures
    print("\n" + "-" * 80)
    print("Test 4: Get contacts from fixtures")
    print("-" * 80)

    contacts = await client.get_project_contacts(int(project_id.split("_")[1], 36))
    print(f"\nContacts count: {len(contacts)}")
    for c in contacts:
        print(f"  - {c['name']} ({c['role']}): {c['email']}")

    # Test 5: Get fake project (not in fixtures)
    print("\n" + "-" * 80)
    print("Test 5: Generate fake project (not in fixtures)")
    print("-" * 80)

    fake_project = await client.get_project(int("123456789"))
    print(f"\nGenerated Project: {fake_project['project_id']}")
    print(f"Name: {fake_project['project_name']}")
    print(f"Policyholder: {fake_project['policyholder_name']}")
    print(f"Address: {fake_project['property_address']}")

    # Test determinism - same ID should produce same data
    fake_project_2 = await client.get_project(int("123456789"))
    assert fake_project == fake_project_2, "Deterministic generation failed!"
    print("\n✓ Deterministic generation verified (same input = same output)")

    # Test 6: Get fake media
    print("\n" + "-" * 80)
    print("Test 6: Generate fake media (not in fixtures)")
    print("-" * 80)

    fake_media = await client.get_project_media(int("987654321"))
    print(f"\nGenerated Media count: {len(fake_media)}")
    for m in fake_media[:3]:
        print(f"  - {m['file_name']}")
        print(f"    URL: {m['download_url']}")
        print(f"    Size: {m['file_size']} bytes")

    # Verify all download URLs point to localhost:8765
    for m in fake_media:
        assert m["download_url"].startswith(
            "http://localhost:8765/files/"
        ), f"Invalid URL: {m['download_url']}"
    print("\n✓ All download URLs point to localhost:8765")

    # Test 7: Circuit breaker status
    print("\n" + "-" * 80)
    print("Test 7: Circuit breaker status")
    print("-" * 80)

    status = client.get_circuit_status()
    print(f"\nCircuit State: {status['state']}")
    print(f"Is Open: {status['is_open']}")
    print(f"Success Count: {status['success_count']}")

    print("\n" + "=" * 80)
    print("All tests passed! ✓")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_mock_api())
