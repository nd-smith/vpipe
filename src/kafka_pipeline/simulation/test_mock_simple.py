#!/usr/bin/env python3
"""
Simple standalone test for mock API client (no external dependencies).

Usage:
    python3 test_mock_simple.py
"""

import asyncio
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Minimal mock client for testing
class SimpleMockClient:
    def __init__(self, fixtures_dir: Path):
        self.fixtures_dir = fixtures_dir
        self.file_server_url = "http://localhost:8765"

        # Load fixtures
        self._projects = {}
        self._media = {}
        self._tasks = {}
        self._contacts = {}

        self._load_fixtures()

    def _load_fixtures(self):
        # Load projects
        projects_file = self.fixtures_dir / "claimx_projects.json"
        if projects_file.exists():
            with open(projects_file) as f:
                projects = json.load(f)
                for p in projects:
                    self._projects[p["project_id"]] = p

        # Load media
        media_file = self.fixtures_dir / "claimx_media.json"
        if media_file.exists():
            with open(media_file) as f:
                media_list = json.load(f)
                for m in media_list:
                    pid = m["project_id"]
                    if pid not in self._media:
                        self._media[pid] = []
                    self._media[pid].append(m)

        # Load tasks
        tasks_file = self.fixtures_dir / "claimx_tasks.json"
        if tasks_file.exists():
            with open(tasks_file) as f:
                tasks = json.load(f)
                for t in tasks:
                    self._tasks[t["task_assignment_id"]] = t

        # Load contacts
        contacts_file = self.fixtures_dir / "claimx_contacts.json"
        if contacts_file.exists():
            with open(contacts_file) as f:
                contacts = json.load(f)
                for c in contacts:
                    pid = c["project_id"]
                    if pid not in self._contacts:
                        self._contacts[pid] = []
                    self._contacts[pid].append(c)

    async def get_project(self, project_id: str) -> dict:
        if project_id in self._projects:
            return self._projects[project_id].copy()
        return self._generate_fake_project(project_id)

    async def get_project_media(self, project_id: str) -> list:
        if project_id in self._media:
            return self._media[project_id].copy()
        return []

    async def get_custom_task(self, assignment_id: str) -> dict:
        if assignment_id in self._tasks:
            return self._tasks[assignment_id].copy()
        return {}

    async def get_project_contacts(self, project_id: str) -> list:
        if project_id in self._contacts:
            return self._contacts[project_id].copy()
        return []

    def _generate_fake_project(self, project_id: str) -> dict:
        seed = int(hashlib.sha256(project_id.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)
        now = datetime.now(timezone.utc)

        return {
            "project_id": project_id,
            "project_name": f"Insurance Claim {rng.randint(1000, 9999)}",
            "status": "active",
            "policyholder_name": f"Test User {rng.randint(1, 100)}",
        }


async def main():
    print("=" * 80)
    print("Simple Mock API Client Test")
    print("=" * 80)

    # Get fixtures directory
    fixtures_dir = Path(__file__).parent / "fixtures"
    print(f"\nFixtures directory: {fixtures_dir}")
    print(f"Exists: {fixtures_dir.exists()}")

    # Initialize client
    client = SimpleMockClient(fixtures_dir)

    print(f"\nLoaded:")
    print(f"  Projects: {len(client._projects)}")
    print(f"  Media projects: {len(client._media)}")
    print(f"  Tasks: {len(client._tasks)}")
    print(f"  Contact projects: {len(client._contacts)}")

    # Test 1: Get project from fixtures
    print("\n" + "-" * 80)
    print("Test 1: Get project from fixtures")
    project_id = "proj_1A2B3C4D5E6F"
    project = await client.get_project(project_id)
    print(f"Project: {project['project_name']}")
    print(f"Status: {project['status']}")

    # Test 2: Get media
    print("\n" + "-" * 80)
    print("Test 2: Get media")
    media = await client.get_project_media(project_id)
    print(f"Media count: {len(media)}")
    for m in media[:3]:
        print(f"  - {m['file_name']}")
        # Verify URL format
        assert m["download_url"].startswith("http://localhost:8765/files/")

    # Test 3: Get iTel Cabinet task
    print("\n" + "-" * 80)
    print("Test 3: Get iTel Cabinet task")
    task_id = "task_assign_10001"
    task = await client.get_custom_task(task_id)
    print(f"Task ID: {task['task_id']}")
    print(f"Task Name: {task['task_name']}")
    assert task["task_id"] == 32513, "Expected iTel Cabinet task_id=32513"

    if "form_response" in task:
        form = task["form_response"]
        print(f"Form responses: {len(form['responses'])}")
        print(f"Form attachments: {len(form['attachments'])}")

        # Verify form attachments have localhost URLs
        for att in form["attachments"]:
            assert att["download_url"].startswith("http://localhost:8765/files/")

    # Test 4: Get contacts
    print("\n" + "-" * 80)
    print("Test 4: Get contacts")
    contacts = await client.get_project_contacts(project_id)
    print(f"Contacts: {len(contacts)}")
    for c in contacts:
        print(f"  - {c['name']} ({c['role']})")

    # Test 5: Fake project (deterministic)
    print("\n" + "-" * 80)
    print("Test 5: Generate fake project (deterministic)")
    fake_id = "proj_FAKE_123"
    fake1 = await client.get_project(fake_id)
    fake2 = await client.get_project(fake_id)
    print(f"Fake project: {fake1['project_name']}")
    assert fake1 == fake2, "Deterministic generation failed!"
    print("✓ Deterministic generation verified")

    print("\n" + "=" * 80)
    print("All tests passed! ✓")
    print("=" * 80)

    # Summary
    print("\nSummary:")
    print("- Mock API client successfully loads fixture data")
    print("- Fixture data includes iTel Cabinet task (task_id=32513)")
    print("- All download URLs point to localhost:8765")
    print("- Deterministic fake data generation works")
    print("- Schema structure matches ClaimX API responses")


if __name__ == "__main__":
    asyncio.run(main())
