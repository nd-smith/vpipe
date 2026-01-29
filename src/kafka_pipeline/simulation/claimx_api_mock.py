"""
Mock ClaimX API client for simulation mode.

Provides a drop-in replacement for ClaimXApiClient that returns realistic
fixture data instead of making actual API calls. Supports deterministic
data generation for project IDs not found in fixtures.

Supports controlled failure injection for testing retry logic via environment variables:
- CLAIMX_API_FAILURE_RATE: Percentage of requests that fail (0-100, default 0)
- CLAIMX_API_FAILURE_DURATION_SEC: How long failures last before recovering (default 0 = permanent)
- CLAIMX_API_FAILURE_STATUS: HTTP status code to return on failure (default 500)
"""

import hashlib
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from kafka_pipeline.common.logging import logged_operation, LoggedClass
from core.types import ErrorCategory

logger = logging.getLogger(__name__)


class MockClaimXAPIClient(LoggedClass):
    """Mock ClaimX API client that returns fixture data.

    Implements the same interface as ClaimXApiClient but returns data from
    fixture files or generates deterministic fake data for unknown projects.

    Features:
    - Loads fixture data from JSON files on initialization
    - Deterministic fake data generation using project_id as seed
    - Download URLs point to dummy file server (localhost:8765)
    - Schema-compatible responses matching production API

    Attributes:
        fixtures_dir: Path to directory containing fixture JSON files
        file_server_url: Base URL for dummy file server
        _projects: Project fixture data indexed by project_id
        _media: Media fixture data indexed by project_id
        _tasks: Task fixture data indexed by assignment_id
        _contacts: Contact fixture data indexed by project_id
    """

    log_component = "claimx_api_mock"

    def __init__(
        self,
        base_url: str = "http://localhost:8765",
        token: str = "mock_token",
        timeout_seconds: int = 30,
        max_concurrent: int = 20,
        sender_username: str = "user@example.com",
        fixtures_dir: Optional[Path] = None,
    ):
        """Initialize mock API client.

        Args:
            base_url: Base URL (ignored, only for interface compatibility)
            token: API token (ignored, only for interface compatibility)
            timeout_seconds: Timeout (ignored, only for interface compatibility)
            max_concurrent: Max concurrent requests (ignored)
            sender_username: Sender username for reports
            fixtures_dir: Path to fixtures directory (defaults to ./fixtures)
        """
        super().__init__()

        self.sender_username = sender_username

        # Determine fixtures directory
        if fixtures_dir is None:
            current_dir = Path(__file__).parent
            fixtures_dir = current_dir / "fixtures"
        self._fixtures_dir = Path(fixtures_dir)

        # File server URL for download links
        self.file_server_url = "http://localhost:8765"

        # Load fixture data
        self._projects: Dict[str, Dict[str, Any]] = {}
        self._media: Dict[str, List[Dict[str, Any]]] = {}
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._contacts: Dict[str, List[Dict[str, Any]]] = {}

        self._load_all_fixtures()

        # Circuit breaker compatibility
        self.is_circuit_open = False

        # Failure injection configuration (load from environment)
        self._failure_rate = float(os.getenv("CLAIMX_API_FAILURE_RATE", "0"))
        self._failure_duration_sec = int(os.getenv("CLAIMX_API_FAILURE_DURATION_SEC", "0"))
        self._failure_status = int(os.getenv("CLAIMX_API_FAILURE_STATUS", "500"))
        self._failure_start_time: Optional[float] = None
        self._total_requests = 0
        self._failed_requests = 0

        self._log(
            logging.INFO,
            "Initialized MockClaimXAPIClient",
            fixtures_dir=str(self._fixtures_dir),
            file_server_url=self.file_server_url,
            projects_loaded=len(self._projects),
            media_projects=len(self._media),
            tasks_loaded=len(self._tasks),
            contact_projects=len(self._contacts),
            failure_rate=self._failure_rate,
            failure_duration_sec=self._failure_duration_sec,
        )

        if self._failure_rate > 0:
            self._log(
                logging.WARNING,
                "ClaimX API failure injection enabled",
                failure_rate=self._failure_rate,
                failure_duration_sec=self._failure_duration_sec,
                failure_status=self._failure_status,
            )

    async def _ensure_session(self) -> None:
        """Mock session initialization (no-op for mock client)."""
        pass

    async def close(self) -> None:
        """Mock session cleanup (no-op for mock client)."""
        pass

    def _should_fail_request(self) -> bool:
        """Determine if this request should fail based on configuration."""
        if self._failure_rate <= 0:
            return False

        # Check if we're within the failure duration window
        if self._failure_duration_sec > 0:
            if self._failure_start_time is None:
                self._failure_start_time = time.time()

            elapsed = time.time() - self._failure_start_time
            if elapsed > self._failure_duration_sec:
                # Recovery period - stop failing
                return False

        # Random failure based on configured rate
        return random.random() * 100 < self._failure_rate

    def _inject_failure(self, operation: str) -> None:
        """Inject a simulated API failure by raising ClaimXApiError.

        Args:
            operation: Name of the operation being performed (for logging)

        Raises:
            ClaimXApiError: Simulated API error with configured status code
        """
        from kafka_pipeline.claimx.api_client import ClaimXApiError

        self._total_requests += 1

        if self._should_fail_request():
            self._failed_requests += 1
            self._log(
                logging.WARNING,
                "Injecting simulated API failure",
                operation=operation,
                status=self._failure_status,
                failure_rate=self._failure_rate,
                total_requests=self._total_requests,
                failed_requests=self._failed_requests,
            )

            raise ClaimXApiError(
                f"Simulated API failure for {operation} (rate: {self._failure_rate}%)",
                status_code=self._failure_status,
                category=ErrorCategory.TRANSIENT,
                is_retryable=True,
            )

    async def __aenter__(self) -> "MockClaimXAPIClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def _load_all_fixtures(self) -> None:
        """Load all fixture files from the fixtures directory."""
        if not self._fixtures_dir.exists():
            self._log(
                logging.WARNING,
                "Fixtures directory not found, will generate all data",
                fixtures_dir=str(self._fixtures_dir),
            )
            return

        # Load projects
        projects_data = self._load_fixture("claimx_projects.json")
        if isinstance(projects_data, list):
            for project in projects_data:
                if "project_id" in project:
                    # Convert project_id to string for consistency
                    project_id = str(project["project_id"])
                    self._projects[project_id] = project

        # Load media
        media_data = self._load_fixture("claimx_media.json")
        if isinstance(media_data, list):
            for media in media_data:
                if "project_id" in media:
                    # Convert to string and group by project_id
                    project_id = str(media["project_id"])
                    if project_id not in self._media:
                        self._media[project_id] = []
                    self._media[project_id].append(media)

        # Load tasks
        tasks_data = self._load_fixture("claimx_tasks.json")
        if isinstance(tasks_data, list):
            for task in tasks_data:
                if "task_assignment_id" in task:
                    # Index by assignment_id for fast lookup
                    assignment_id = str(task["task_assignment_id"])
                    self._tasks[assignment_id] = task

        # Load contacts
        contacts_data = self._load_fixture("claimx_contacts.json")
        if isinstance(contacts_data, list):
            for contact in contacts_data:
                if "project_id" in contact:
                    # Group by project_id
                    project_id = str(contact["project_id"])
                    if project_id not in self._contacts:
                        self._contacts[project_id] = []
                    self._contacts[project_id].append(contact)

    def _load_fixture(self, filename: str) -> Any:
        """Load fixture file and return parsed JSON.

        Args:
            filename: Name of fixture file (e.g., "claimx_projects.json")

        Returns:
            Parsed JSON data, or empty dict if file not found
        """
        path = self._fixtures_dir / filename
        if not path.exists():
            self._log(
                logging.DEBUG,
                "Fixture file not found",
                filename=filename,
                path=str(path),
            )
            return {}

        try:
            with open(path) as f:
                data = json.load(f)
            self._log(
                logging.DEBUG,
                "Loaded fixture file",
                filename=filename,
                records=len(data) if isinstance(data, list) else 1,
            )
            return data
        except Exception as e:
            self._log(
                logging.ERROR,
                "Failed to load fixture file",
                filename=filename,
                error=str(e),
            )
            return {}

    def _get_rng(self, seed_value: str) -> random.Random:
        """Create deterministic random number generator from seed.

        Args:
            seed_value: String to use as seed (e.g., project_id)

        Returns:
            Random instance seeded with hash of seed_value
        """
        seed = int(hashlib.sha256(seed_value.encode()).hexdigest()[:8], 16)
        return random.Random(seed)

    @logged_operation(level=logging.DEBUG)
    async def get_project(self, project_id: int) -> Dict[str, Any]:
        """Get full project details.

        Args:
            project_id: ClaimX project ID (int for API compatibility)

        Returns:
            Project details dict matching ClaimX API schema
        """
        # Failure injection
        self._inject_failure("get_project")

        # Convert to string for lookup
        project_id_str = str(project_id)

        # Check fixtures first
        if project_id_str in self._projects:
            return self._projects[project_id_str].copy()

        # Generate deterministic fake project
        return self._generate_fake_project(project_id_str)

    @logged_operation(level=logging.DEBUG)
    async def get_project_media(
        self,
        project_id: int,
        media_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """Get media metadata for a project.

        Args:
            project_id: ClaimX project ID
            media_ids: Optional list of specific media IDs to fetch

        Returns:
            List of media metadata dicts
        """
        # Failure injection
        self._inject_failure("get_project_media")

        project_id_str = str(project_id)

        # Get media from fixtures
        if project_id_str in self._media:
            media_list = self._media[project_id_str].copy()

            # Filter by media_ids if specified
            if media_ids:
                media_ids_str = [str(mid) for mid in media_ids]
                media_list = [m for m in media_list if str(m.get("media_id")) in media_ids_str]

            return media_list

        # Generate fake media
        return self._generate_fake_media(project_id_str, media_ids)

    @logged_operation(level=logging.DEBUG)
    async def get_project_contacts(self, project_id: int) -> List[Dict[str, Any]]:
        """Get contacts for a project.

        Args:
            project_id: ClaimX project ID

        Returns:
            List of contact dicts
        """
        # Failure injection
        self._inject_failure("get_project_contacts")

        project_id_str = str(project_id)

        # Check fixtures
        if project_id_str in self._contacts:
            return self._contacts[project_id_str].copy()

        # Generate fake contacts
        return self._generate_fake_contacts(project_id_str)

    @logged_operation(level=logging.DEBUG)
    async def get_custom_task(self, assignment_id: int) -> Dict[str, Any]:
        """Get custom task assignment details.

        Args:
            assignment_id: Task assignment ID

        Returns:
            Task assignment dict with form response if applicable
        """
        # Failure injection
        self._inject_failure("get_custom_task")

        assignment_id_str = str(assignment_id)

        # Check fixtures
        if assignment_id_str in self._tasks:
            return self._tasks[assignment_id_str].copy()

        # Generate fake task
        return self._generate_fake_task(assignment_id_str)

    @logged_operation(level=logging.DEBUG)
    async def get_project_tasks(self, project_id: int) -> List[Dict[str, Any]]:
        """Get all tasks for a project.

        Args:
            project_id: ClaimX project ID

        Returns:
            List of task dicts
        """
        project_id_str = str(project_id)

        # Find tasks for this project
        tasks = []
        for task in self._tasks.values():
            if str(task.get("project_id")) == project_id_str:
                tasks.append(task.copy())

        if tasks:
            return tasks

        # Generate fake tasks
        return self._generate_fake_project_tasks(project_id_str)

    @logged_operation(level=logging.DEBUG)
    async def get_video_collaboration(
        self,
        project_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        sender_username: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get video collaboration report.

        Args:
            project_id: ClaimX project ID
            start_date: Optional start date filter
            end_date: Optional end date filter
            sender_username: Optional username filter

        Returns:
            Video collaboration report dict
        """
        return {
            "data": [],
            "projectId": project_id,
            "reportType": "VIDEO_COLLABORATION",
        }

    @logged_operation(level=logging.DEBUG)
    async def get_project_conversations(self, project_id: int) -> List[Dict[str, Any]]:
        """Get conversations for a project.

        Args:
            project_id: ClaimX project ID

        Returns:
            List of conversation dicts
        """
        return []

    def get_circuit_status(self) -> Dict[str, Any]:
        """Get circuit breaker diagnostics (mock always returns healthy).

        Returns:
            Circuit status dict
        """
        return {
            "state": "closed",
            "failure_count": 0,
            "success_count": 1000,
            "is_open": False,
        }

    # =========================================================================
    # Deterministic Fake Data Generation
    # =========================================================================

    def _generate_fake_project(self, project_id: str) -> Dict[str, Any]:
        """Generate deterministic fake project data.

        Args:
            project_id: Project ID to use as seed

        Returns:
            Fake project dict matching ClaimX schema
        """
        rng = self._get_rng(project_id)
        now = datetime.now(timezone.utc)

        # Generate realistic names
        first_names = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"]

        first = rng.choice(first_names)
        last = rng.choice(last_names)

        # Generate realistic address
        streets = ["Main", "Oak", "Maple", "Cedar", "Pine", "Elm"]
        suffixes = ["St", "Ave", "Blvd", "Dr", "Ln"]
        cities = ["Austin", "Denver", "Phoenix", "Seattle", "Portland"]
        states = ["TX", "CO", "AZ", "WA", "OR"]

        city = rng.choice(cities)
        state = rng.choice(states)

        return {
            "project_id": project_id,
            "project_name": f"Insurance Claim {rng.randint(1000, 9999)}",
            "status": rng.choice(["active", "pending", "completed"]),
            "created_at": (now - timedelta(days=rng.randint(1, 30))).isoformat(),
            "updated_at": now.isoformat(),
            "policyholder_name": f"{first} {last}",
            "policyholder_email": f"{first.lower()}.{last.lower()}@example.com",
            "policyholder_phone": f"({rng.randint(200, 999)}) {rng.randint(200, 999)}-{rng.randint(1000, 9999)}",
            "property_address": f"{rng.randint(100, 9999)} {rng.choice(streets)} {rng.choice(suffixes)}, {city}, {state} {rng.randint(10000, 99999)}",
            "date_of_loss": (now - timedelta(days=rng.randint(1, 15))).strftime("%Y-%m-%d"),
        }

    def _generate_fake_media(
        self,
        project_id: str,
        media_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        """Generate deterministic fake media data.

        Args:
            project_id: Project ID to use as seed
            media_ids: Optional specific media IDs to generate

        Returns:
            List of fake media dicts
        """
        rng = self._get_rng(project_id)

        # Generate 2-5 media files if no specific IDs requested
        if media_ids is None:
            num_media = rng.randint(2, 5)
            media_ids = [rng.randint(100000, 999999) for _ in range(num_media)]

        media_list = []
        file_types = [
            ("damage_photo_{}.jpg", "image/jpeg", 1024000, 3072000),
            ("roof_inspection_{}.jpg", "image/jpeg", 1024000, 3072000),
            ("estimate_{}.pdf", "application/pdf", 51200, 512000),
            ("video_{}.mp4", "video/mp4", 5120000, 20480000),
        ]

        for i, media_id in enumerate(media_ids):
            template, content_type, min_size, max_size = rng.choice(file_types)
            file_name = template.format(i + 1)
            file_size = rng.randint(min_size, max_size)

            media_list.append(
                {
                    "media_id": str(media_id),
                    "project_id": project_id,
                    "file_name": file_name,
                    "content_type": content_type,
                    "file_size": file_size,
                    "file_type": file_name.split(".")[-1],
                    "download_url": f"{self.file_server_url}/files/{project_id}/{media_id}/{file_name}",
                    "uploaded_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        return media_list

    def _generate_fake_contacts(self, project_id: str) -> List[Dict[str, Any]]:
        """Generate deterministic fake contact data.

        Args:
            project_id: Project ID to use as seed

        Returns:
            List of fake contact dicts
        """
        rng = self._get_rng(project_id)

        first_names = ["James", "Mary", "Robert", "Patricia"]
        last_names = ["Smith", "Johnson", "Williams", "Brown"]
        roles = ["policyholder", "adjuster", "contractor"]

        contacts = []
        for i, role in enumerate(roles):
            first = rng.choice(first_names)
            last = rng.choice(last_names)

            contacts.append(
                {
                    "contact_id": str(rng.randint(10000, 99999)),
                    "project_id": project_id,
                    "role": role,
                    "name": f"{first} {last}",
                    "email": f"{first.lower()}.{last.lower()}@example.com",
                    "phone": f"({rng.randint(200, 999)}) {rng.randint(200, 999)}-{rng.randint(1000, 9999)}",
                }
            )

        return contacts

    def _generate_fake_task(self, assignment_id: str) -> Dict[str, Any]:
        """Generate deterministic fake task data.

        Args:
            assignment_id: Assignment ID to use as seed

        Returns:
            Fake task dict matching ClaimX schema
        """
        rng = self._get_rng(assignment_id)
        now = datetime.now(timezone.utc)

        task_names = [
            "Review initial photos",
            "Schedule inspection",
            "Verify coverage details",
            "Contact policyholder",
        ]

        return {
            "task_assignment_id": assignment_id,
            "task_id": str(rng.randint(1000, 9999)),
            "task_name": rng.choice(task_names),
            "project_id": str(rng.randint(100000, 999999)),
            "status": rng.choice(["assigned", "completed"]),
            "assigned_at": (now - timedelta(days=rng.randint(1, 5))).isoformat(),
            "completed_at": now.isoformat() if rng.random() > 0.5 else None,
            "assigned_to_user_id": rng.randint(1000, 9999),
            "assigned_by_user_id": rng.randint(1000, 9999),
        }

    def _generate_fake_project_tasks(self, project_id: str) -> List[Dict[str, Any]]:
        """Generate deterministic fake project tasks.

        Args:
            project_id: Project ID to use as seed

        Returns:
            List of fake task dicts
        """
        rng = self._get_rng(project_id)
        num_tasks = rng.randint(1, 3)

        tasks = []
        for i in range(num_tasks):
            assignment_id = f"{project_id}_task_{i}"
            task = self._generate_fake_task(assignment_id)
            task["project_id"] = project_id
            tasks.append(task)

        return tasks


__all__ = ["MockClaimXAPIClient"]
