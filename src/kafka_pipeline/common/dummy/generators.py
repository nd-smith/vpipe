# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Realistic data generators for dummy pipeline testing.

Generates insurance claim data that looks authentic for both XACT and ClaimX domains.
Uses deterministic seeding for reproducibility when needed.
"""

import hashlib
import json
import random
import string
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from enum import Enum

from kafka_pipeline.common.dummy.plugin_profiles import (
    ItelCabinetDataGenerator,
    PluginProfile,
)


# =============================================================================
# Reference Data - Realistic Insurance Terminology
# =============================================================================

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
]

STREET_NAMES = [
    "Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Lake",
    "Hill", "Park", "Forest", "River", "Spring", "Valley", "Sunset", "Highland",
    "Meadow", "Cherry", "Willow", "Birch", "Walnut", "Hickory", "Laurel", "Magnolia",
]

STREET_SUFFIXES = ["St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Way", "Ct", "Pl", "Cir"]

CITIES = [
    ("Austin", "TX", "78701"), ("Denver", "CO", "80202"), ("Phoenix", "AZ", "85001"),
    ("Seattle", "WA", "98101"), ("Portland", "OR", "97201"), ("Atlanta", "GA", "30301"),
    ("Chicago", "IL", "60601"), ("Miami", "FL", "33101"), ("Dallas", "TX", "75201"),
    ("Houston", "TX", "77001"), ("San Diego", "CA", "92101"), ("Nashville", "TN", "37201"),
    ("Charlotte", "NC", "28201"), ("Columbus", "OH", "43201"), ("Indianapolis", "IN", "46201"),
    ("Jacksonville", "FL", "32099"), ("San Antonio", "TX", "78201"), ("Fort Worth", "TX", "76101"),
]

INSURANCE_COMPANIES = [
    "State Farm", "Progressive", "GEICO", "Liberty Mutual",
    "USAA", "Farmers", "Nationwide", "Travelers", "American Family",
    "Erie Insurance", "Auto-Owners", "Chubb", "Hartford", "Safeco",
]

ADJUSTERS = [
    "John Mitchell", "Sarah Thompson", "Michael Chen", "Emily Rodriguez",
    "David Kim", "Jennifer Wilson", "Robert Garcia", "Amanda Martinez",
    "Christopher Lee", "Jessica Brown", "William Davis", "Ashley Taylor",
]

# Damage types and descriptions
DAMAGE_TYPES = [
    "Water Damage", "Fire Damage", "Wind Damage", "Hail Damage", "Theft",
    "Vandalism", "Lightning Strike", "Burst Pipe", "Flood Damage", "Roof Damage",
    "Structural Damage", "Smoke Damage", "Mold Damage", "Foundation Crack",
]

DAMAGE_DESCRIPTIONS = [
    "Water intrusion from roof leak affecting ceiling and walls",
    "Fire damage to kitchen area with smoke damage throughout",
    "Wind damage to siding and roof shingles from recent storm",
    "Hail damage to roof, gutters, and exterior surfaces",
    "Burst pipe in bathroom caused water damage to flooring",
    "Storm damage with fallen tree impacting garage roof",
    "Lightning strike caused electrical damage and small fire",
    "Flooding in basement from heavy rainfall",
    "Vandalism to exterior windows and entry door",
    "Ice dam caused water intrusion in attic space",
]

ROOMS = [
    "Living Room", "Master Bedroom", "Kitchen", "Bathroom", "Garage",
    "Basement", "Attic", "Dining Room", "Guest Bedroom", "Home Office",
    "Laundry Room", "Hallway", "Foyer", "Patio", "Deck",
]

# File types commonly found in insurance claims
FILE_TYPES = {
    "photos": [
        ("exterior_damage_{n}.jpg", "image/jpeg", (500_000, 5_000_000)),
        ("interior_damage_{n}.jpg", "image/jpeg", (500_000, 5_000_000)),
        ("roof_inspection_{n}.jpg", "image/jpeg", (500_000, 5_000_000)),
        ("water_damage_{n}.jpg", "image/jpeg", (500_000, 5_000_000)),
        ("before_repair_{n}.jpg", "image/jpeg", (500_000, 5_000_000)),
        ("after_repair_{n}.jpg", "image/jpeg", (500_000, 5_000_000)),
    ],
    "documents": [
        ("estimate_{n}.pdf", "application/pdf", (50_000, 500_000)),
        ("invoice_{n}.pdf", "application/pdf", (50_000, 300_000)),
        ("police_report.pdf", "application/pdf", (100_000, 500_000)),
        ("contractor_agreement.pdf", "application/pdf", (100_000, 400_000)),
        ("proof_of_loss.pdf", "application/pdf", (50_000, 200_000)),
        ("coverage_summary.pdf", "application/pdf", (50_000, 150_000)),
    ],
    "reports": [
        ("inspection_report.pdf", "application/pdf", (200_000, 1_000_000)),
        ("engineering_assessment.pdf", "application/pdf", (500_000, 2_000_000)),
        ("moisture_reading.pdf", "application/pdf", (100_000, 300_000)),
        ("scope_of_work.pdf", "application/pdf", (150_000, 500_000)),
    ],
}

# ClaimX event types
CLAIMX_EVENT_TYPES = [
    "PROJECT_CREATED",
    "PROJECT_FILE_ADDED",
    "PROJECT_MFN_ADDED",
    "CUSTOM_TASK_ASSIGNED",
    "CUSTOM_TASK_COMPLETED",
    "POLICYHOLDER_INVITED",
    "POLICYHOLDER_JOINED",
    "VIDEO_COLLABORATION_INVITE_SENT",
    "VIDEO_COLLABORATION_COMPLETED",
]

TASK_NAMES = [
    "Review initial photos",
    "Schedule inspection",
    "Verify coverage details",
    "Contact policyholder",
    "Request additional documentation",
    "Review contractor estimate",
    "Approve repair scope",
    "Process payment",
    "Close claim file",
    "Document site conditions",
]


class EventFrequency(Enum):
    """Relative frequency of different event types."""
    VERY_COMMON = 50
    COMMON = 30
    OCCASIONAL = 15
    RARE = 5


# Event type frequencies for realistic distribution
EVENT_TYPE_WEIGHTS = {
    "PROJECT_CREATED": EventFrequency.OCCASIONAL,
    "PROJECT_FILE_ADDED": EventFrequency.VERY_COMMON,
    "PROJECT_MFN_ADDED": EventFrequency.RARE,
    "CUSTOM_TASK_ASSIGNED": EventFrequency.COMMON,
    "CUSTOM_TASK_COMPLETED": EventFrequency.COMMON,
    "POLICYHOLDER_INVITED": EventFrequency.OCCASIONAL,
    "POLICYHOLDER_JOINED": EventFrequency.OCCASIONAL,
    "VIDEO_COLLABORATION_INVITE_SENT": EventFrequency.RARE,
    "VIDEO_COLLABORATION_COMPLETED": EventFrequency.RARE,
}


@dataclass
class GeneratorConfig:
    """Configuration for data generators."""
    seed: Optional[int] = None
    base_url: str = "http://localhost:8765"  # Dummy file server URL
    include_failures: bool = False  # Generate some failing scenarios
    failure_rate: float = 0.05  # 5% failure rate when enabled
    events_per_second: float = 1.0  # Target event rate
    plugin_profile: Optional[str] = None  # Plugin profile to use (e.g., "itel_cabinet_api")
    # Mixed mode settings - when plugin_profile is None or "mixed"
    itel_trigger_percentage: float = 0.3  # 30% of ClaimX events trigger itel plugin
    include_itel_triggers: bool = True  # Include itel-triggering events in mixed mode


@dataclass
class GeneratedClaim:
    """A complete generated insurance claim with all associated data."""
    claim_id: str
    project_id: str
    policy_number: str
    policyholder: Dict[str, str]
    property_address: Dict[str, str]
    damage_type: str
    damage_description: str
    date_of_loss: datetime
    reported_date: datetime
    assigned_adjuster: str
    insurance_company: str
    estimated_loss: float
    deductible: float
    affected_rooms: List[str]
    attachments: List[Dict[str, Any]] = field(default_factory=list)


class RealisticDataGenerator:
    """
    Generates realistic insurance claim data for pipeline testing.

    Provides deterministic generation when seeded, making tests reproducible.
    """

    def __init__(self, config: Optional[GeneratorConfig] = None):
        self.config = config or GeneratorConfig()
        self._rng = random.Random(self.config.seed)
        self._claim_counter = 0
        self._event_counter = 0
        self._active_claims: Dict[str, GeneratedClaim] = {}

        # Initialize plugin-specific generators
        self._itel_generator = ItelCabinetDataGenerator(self._rng)

    def _generate_id(self, prefix: str, length: int = 8) -> str:
        """Generate a unique ID with prefix."""
        chars = string.ascii_uppercase + string.digits
        suffix = ''.join(self._rng.choices(chars, k=length))
        return f"{prefix}{suffix}"

    def _generate_deterministic_id(self, *components: str) -> str:
        """Generate a deterministic ID from components (for event_id)."""
        combined = "|".join(str(c) for c in components)
        return hashlib.sha256(combined.encode()).hexdigest()[:24]

    def generate_person(self) -> Dict[str, str]:
        """Generate a realistic person with contact info."""
        first = self._rng.choice(FIRST_NAMES)
        last = self._rng.choice(LAST_NAMES)
        email_domains = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com"]

        return {
            "first_name": first,
            "last_name": last,
            "full_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}@{self._rng.choice(email_domains)}",
            "phone": f"({self._rng.randint(200, 999)}) {self._rng.randint(200, 999)}-{self._rng.randint(1000, 9999)}",
        }

    def generate_address(self) -> Dict[str, str]:
        """Generate a realistic US address."""
        city, state, base_zip = self._rng.choice(CITIES)
        street_num = self._rng.randint(100, 9999)
        street = self._rng.choice(STREET_NAMES)
        suffix = self._rng.choice(STREET_SUFFIXES)
        zip_suffix = self._rng.randint(0, 99)

        return {
            "street": f"{street_num} {street} {suffix}",
            "city": city,
            "state": state,
            "zip": f"{base_zip}-{zip_suffix:02d}",
            "full": f"{street_num} {street} {suffix}, {city}, {state} {base_zip}",
        }

    def generate_claim(self) -> GeneratedClaim:
        """Generate a complete insurance claim."""
        self._claim_counter += 1

        # Generate base IDs
        claim_id = self._generate_id("CLM-")
        project_id = self._generate_id("proj_", 12)
        policy_number = f"POL-{self._rng.randint(100000, 999999)}-{self._rng.randint(10, 99)}"

        # Date of loss (within last 30 days)
        days_ago = self._rng.randint(1, 30)
        date_of_loss = datetime.now(timezone.utc) - timedelta(days=days_ago)
        # Reported 0-3 days after loss
        reported_date = date_of_loss + timedelta(days=self._rng.randint(0, 3))

        # Generate loss amount based on damage type
        damage_type = self._rng.choice(DAMAGE_TYPES)
        if damage_type in ["Fire Damage", "Structural Damage", "Flood Damage"]:
            estimated_loss = self._rng.uniform(25000, 150000)
        elif damage_type in ["Water Damage", "Roof Damage", "Wind Damage"]:
            estimated_loss = self._rng.uniform(5000, 50000)
        else:
            estimated_loss = self._rng.uniform(1000, 25000)

        # Standard deductibles
        deductible = self._rng.choice([500, 1000, 1500, 2000, 2500])

        claim = GeneratedClaim(
            claim_id=claim_id,
            project_id=project_id,
            policy_number=policy_number,
            policyholder=self.generate_person(),
            property_address=self.generate_address(),
            damage_type=damage_type,
            damage_description=self._rng.choice(DAMAGE_DESCRIPTIONS),
            date_of_loss=date_of_loss,
            reported_date=reported_date,
            assigned_adjuster=self._rng.choice(ADJUSTERS),
            insurance_company=self._rng.choice(INSURANCE_COMPANIES),
            estimated_loss=round(estimated_loss, 2),
            deductible=deductible,
            affected_rooms=self._rng.sample(ROOMS, k=self._rng.randint(1, 4)),
            attachments=[],
        )

        # Store for later reference (e.g., adding files to existing claims)
        self._active_claims[project_id] = claim
        return claim

    def generate_attachment(
        self,
        claim: GeneratedClaim,
        file_category: str = "photos",
    ) -> Dict[str, Any]:
        """Generate an attachment for a claim."""
        file_templates = FILE_TYPES.get(file_category, FILE_TYPES["photos"])
        template, content_type, (min_size, max_size) = self._rng.choice(file_templates)

        # Generate unique file name
        file_num = len(claim.attachments) + 1
        file_name = template.format(n=file_num)
        file_size = self._rng.randint(min_size, max_size)

        media_id = self._generate_id("media_", 12)

        attachment = {
            "media_id": media_id,
            "file_name": file_name,
            "content_type": content_type,
            "file_size": file_size,
            "file_type": file_name.split(".")[-1],
            "download_url": f"{self.config.base_url}/files/{claim.project_id}/{media_id}/{file_name}",
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }

        claim.attachments.append(attachment)
        return attachment

    # =========================================================================
    # XACT Domain Event Generation
    # =========================================================================

    def generate_xact_event(
        self,
        claim: Optional[GeneratedClaim] = None,
        event_subtype: str = "documentsReceived",
    ) -> Dict[str, Any]:
        """
        Generate an XACT domain event message.

        Returns dict compatible with EventMessage schema.
        """
        if claim is None:
            claim = self.generate_claim()

        self._event_counter += 1
        trace_id = self._generate_id("TRC-", 12)

        # Generate 1-5 attachments
        num_attachments = self._rng.randint(1, 5)
        for _ in range(num_attachments):
            category = self._rng.choices(
                ["photos", "documents", "reports"],
                weights=[60, 30, 10]
            )[0]
            self.generate_attachment(claim, category)

        attachment_urls = [a["download_url"] for a in claim.attachments[-num_attachments:]]

        # Build the data payload
        data_payload = {
            "assignmentId": claim.claim_id,
            "claim_id": claim.claim_id,
            "policyNumber": claim.policy_number,
            "insuredName": claim.policyholder["full_name"],
            "insuredEmail": claim.policyholder["email"],
            "insuredPhone": claim.policyholder["phone"],
            "propertyAddress": claim.property_address["full"],
            "lossType": claim.damage_type,
            "lossDescription": claim.damage_description,
            "dateOfLoss": claim.date_of_loss.strftime("%Y-%m-%d"),
            "adjusterName": claim.assigned_adjuster,
            "carrierName": claim.insurance_company,
            "estimatedLoss": claim.estimated_loss,
            "deductible": claim.deductible,
            "attachments": attachment_urls,
        }

        return {
            "type": f"verisk.claims.property.xn.{event_subtype}",
            "version": 1,
            "utcDateTime": datetime.now(timezone.utc).isoformat(),
            "traceId": trace_id,
            "data": json.dumps(data_payload),
        }

    # =========================================================================
    # ClaimX Domain Event Generation
    # =========================================================================

    def generate_claimx_event(
        self,
        event_type: Optional[str] = None,
        claim: Optional[GeneratedClaim] = None,
    ) -> Dict[str, Any]:
        """
        Generate a ClaimX domain event message.

        Returns dict compatible with ClaimXEventMessage schema.
        """
        # Select event type based on weights if not specified
        if event_type is None:
            types = list(EVENT_TYPE_WEIGHTS.keys())
            weights = [w.value for w in EVENT_TYPE_WEIGHTS.values()]
            event_type = self._rng.choices(types, weights=weights)[0]

        # Use existing claim or generate new one
        if claim is None:
            if event_type == "PROJECT_CREATED" or not self._active_claims:
                claim = self.generate_claim()
            else:
                # Use an existing claim for continuity
                claim = self._rng.choice(list(self._active_claims.values()))

        self._event_counter += 1
        now = datetime.now(timezone.utc)

        # Generate deterministic event_id
        event_id = self._generate_deterministic_id(
            claim.project_id, event_type, str(now.timestamp()), str(self._event_counter)
        )

        # Base event structure
        event = {
            "event_id": f"evt_{event_id}",
            "event_type": event_type,
            "project_id": claim.project_id,
            "ingested_at": now.isoformat(),
            "media_id": None,
            "task_assignment_id": None,
            "video_collaboration_id": None,
            "master_file_name": None,
            "raw_data": {},
        }

        # Build event-specific data
        raw_data = {
            "projectId": claim.project_id,
            "eventType": event_type,
            "claimNumber": claim.claim_id,
            "policyNumber": claim.policy_number,
        }

        if event_type == "PROJECT_CREATED":
            raw_data.update({
                "projectName": f"Claim {claim.claim_id}",
                "createdAt": now.isoformat(),
                "insuredName": claim.policyholder["full_name"],
                "propertyAddress": claim.property_address["full"],
                "lossType": claim.damage_type,
                "dateOfLoss": claim.date_of_loss.strftime("%Y-%m-%d"),
                "assignedAdjuster": claim.assigned_adjuster,
            })

        elif event_type == "PROJECT_FILE_ADDED":
            attachment = self.generate_attachment(claim, "photos")
            event["media_id"] = attachment["media_id"]
            raw_data.update({
                "mediaId": attachment["media_id"],
                "fileName": attachment["file_name"],
                "fileSize": attachment["file_size"],
                "contentType": attachment["content_type"],
                "uploadedBy": claim.policyholder["email"],
            })

        elif event_type == "PROJECT_MFN_ADDED":
            mfn = f"MFN-{self._rng.randint(100000, 999999)}.xm8"
            event["master_file_name"] = mfn
            raw_data.update({
                "masterFileName": mfn,
                "version": self._rng.randint(1, 5),
                "lineItemCount": self._rng.randint(10, 200),
            })

        elif event_type in ["CUSTOM_TASK_ASSIGNED", "CUSTOM_TASK_COMPLETED"]:
            task_id = self._generate_id("task_", 8)
            event["task_assignment_id"] = task_id
            raw_data.update({
                "taskAssignmentId": task_id,
                "taskName": self._rng.choice(TASK_NAMES),
                "assignee": claim.assigned_adjuster,
                "assigneeEmail": f"{claim.assigned_adjuster.lower().replace(' ', '.')}@insurance.com",
                "dueDate": (now + timedelta(days=self._rng.randint(1, 7))).strftime("%Y-%m-%d"),
            })
            if event_type == "CUSTOM_TASK_COMPLETED":
                raw_data["completedAt"] = now.isoformat()

        elif event_type == "POLICYHOLDER_INVITED":
            raw_data.update({
                "inviteeEmail": claim.policyholder["email"],
                "inviteeName": claim.policyholder["full_name"],
                "invitedBy": claim.assigned_adjuster,
            })

        elif event_type == "POLICYHOLDER_JOINED":
            raw_data.update({
                "userEmail": claim.policyholder["email"],
                "userName": claim.policyholder["full_name"],
                "joinedAt": now.isoformat(),
            })

        elif event_type in ["VIDEO_COLLABORATION_INVITE_SENT", "VIDEO_COLLABORATION_COMPLETED"]:
            video_id = self._generate_id("vid_", 8)
            event["video_collaboration_id"] = video_id
            raw_data.update({
                "videoCollaborationId": video_id,
                "scheduledFor": (now + timedelta(hours=self._rng.randint(1, 48))).isoformat(),
                "participants": [
                    claim.policyholder["email"],
                    f"{claim.assigned_adjuster.lower().replace(' ', '.')}@insurance.com",
                ],
            })
            if event_type == "VIDEO_COLLABORATION_COMPLETED":
                raw_data["duration_minutes"] = self._rng.randint(5, 45)
                raw_data["recordingAvailable"] = self._rng.choice([True, False])

        event["raw_data"] = raw_data
        return event

    def generate_itel_cabinet_event(
        self,
        claim: Optional[GeneratedClaim] = None,
        event_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a ClaimX event for itel Cabinet Repair Form (task_id 32513).

        This generates events that will trigger the itel Cabinet API plugin workflow.
        The event includes realistic form data that mimics what ClaimX API returns.

        Args:
            claim: Existing claim or None to generate new one
            event_type: CUSTOM_TASK_ASSIGNED or CUSTOM_TASK_COMPLETED (defaults to weighted choice)

        Returns:
            Dict compatible with ClaimXEventMessage schema, enriched with itel form data
        """
        # Select event type if not specified
        if event_type is None:
            # Bias towards completed tasks (70% completed, 30% assigned)
            event_type = self._rng.choices(
                ["CUSTOM_TASK_ASSIGNED", "CUSTOM_TASK_COMPLETED"],
                weights=[30, 70]
            )[0]

        # Use existing claim or generate new one
        if claim is None:
            claim = self.generate_claim()

        self._event_counter += 1
        now = datetime.now(timezone.utc)

        # Generate itel Cabinet form data
        form_data = self._itel_generator.generate_form_data(claim)

        # Build ClaimX task details (mimics API response)
        task_details = self._itel_generator.build_claimx_task_details(form_data, claim)

        # Generate deterministic event_id
        event_id = self._generate_deterministic_id(
            claim.project_id, event_type, str(now.timestamp()), str(self._event_counter)
        )

        # Base event structure
        event = {
            "event_id": f"evt_{event_id}",
            "event_type": event_type,
            "project_id": claim.project_id,
            "ingested_at": now.isoformat(),
            "media_id": None,
            "task_assignment_id": form_data.assignment_id,
            "video_collaboration_id": None,
            "master_file_name": None,
            "raw_data": {},
        }

        # Build event-specific raw data
        raw_data = {
            "projectId": claim.project_id,
            "eventType": event_type,
            "claimNumber": claim.claim_id,
            "policyNumber": claim.policy_number,
            "taskAssignmentId": form_data.assignment_id,
            "taskName": self._itel_generator.ITEL_TASK_NAME,
            "taskId": self._itel_generator.ITEL_TASK_ID,
            "assignee": claim.assigned_adjuster,
            "assigneeEmail": form_data.assignor_email,
        }

        if event_type == "CUSTOM_TASK_ASSIGNED":
            raw_data.update({
                "assignedAt": form_data.date_assigned,
                "dueDate": (now + timedelta(days=self._rng.randint(3, 7))).strftime("%Y-%m-%d"),
            })
        elif event_type == "CUSTOM_TASK_COMPLETED":
            raw_data.update({
                "completedAt": form_data.date_completed or now.isoformat(),
                "assignedAt": form_data.date_assigned,
            })

        # Add the complete task details that would be fetched by ClaimX API lookup
        # This is what the enrichment handler will use
        raw_data["claimx_task_details"] = task_details

        event["raw_data"] = raw_data
        return event

    def generate_claimx_event_mixed(
        self,
        event_type: Optional[str] = None,
        claim: Optional[GeneratedClaim] = None,
    ) -> Dict[str, Any]:
        """
        Generate a ClaimX event with configurable itel plugin trigger rate.

        In mixed mode, a percentage of task events (CUSTOM_TASK_ASSIGNED,
        CUSTOM_TASK_COMPLETED) will be itel Cabinet triggers (task_id=32513).

        Args:
            event_type: Optional specific event type
            claim: Optional existing claim

        Returns:
            Dict compatible with ClaimXEventMessage schema
        """
        # If itel triggers are disabled, generate standard event
        if not self.config.include_itel_triggers:
            return self.generate_claimx_event(event_type=event_type, claim=claim)

        # Select event type if not specified
        if event_type is None:
            types = list(EVENT_TYPE_WEIGHTS.keys())
            weights = [w.value for w in EVENT_TYPE_WEIGHTS.values()]
            event_type = self._rng.choices(types, weights=weights)[0]

        # Check if this is a task event and should trigger itel
        is_task_event = event_type in ["CUSTOM_TASK_ASSIGNED", "CUSTOM_TASK_COMPLETED"]

        if is_task_event and self._rng.random() < self.config.itel_trigger_percentage:
            # Generate itel-triggering event
            return self.generate_itel_cabinet_event(
                claim=claim,
                event_type=event_type,
            )

        # Generate standard ClaimX event
        return self.generate_claimx_event(event_type=event_type, claim=claim)

    def get_or_create_claim(self, project_id: Optional[str] = None) -> GeneratedClaim:
        """Get existing claim by project_id or create a new one."""
        if project_id and project_id in self._active_claims:
            return self._active_claims[project_id]
        return self.generate_claim()

    def get_active_claims_count(self) -> int:
        """Return count of active claims."""
        return len(self._active_claims)

    def clear_old_claims(self, max_claims: int = 100) -> int:
        """Remove oldest claims if exceeding max. Returns number removed."""
        if len(self._active_claims) <= max_claims:
            return 0

        to_remove = len(self._active_claims) - max_claims
        keys_to_remove = list(self._active_claims.keys())[:to_remove]
        for key in keys_to_remove:
            del self._active_claims[key]
        return to_remove
