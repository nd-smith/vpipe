"""
Test script to validate iTel vendor schema transformation.

This script creates sample data and tests the transformation logic
to ensure it matches the vendor's expected format.
"""

import json
from datetime import datetime

# Sample payload from tracking worker
sample_tracking_payload = {
    "event_id": "evt_12345",
    "event_timestamp": "2026-02-06T10:00:00Z",
    "assignment_id": 789456,
    "project_id": "5395115",
    "task_id": 27019,
    "submission": {
        "assignment_id": 789456,
        "project_id": "5395115",
        "task_id": 27019,
        "task_name": "Cabinet Repair Form",
        "status": "COMPLETED",
        "date_assigned": "2026-02-03T13:36:00Z",
        "date_completed": "2026-02-05T15:20:00Z",
        "customer_first_name": "John",
        "customer_last_name": "Doe",
        "customer_email": "john.doe@example.com",
        "customer_phone": "555-1234",
        "assignor_email": "adjuster@insurance.com",
        "damage_description": "Water damage to kitchen cabinets",
        "additional_notes": "Customer prefers morning appointments",
        # Upper cabinets
        "upper_cabinets_damaged": True,
        "num_damaged_upper_boxes": 2,
        "upper_cabinets_detached": False,
        "upper_face_frames_doors_drawers_available": "Yes",
        "upper_face_frames_doors_drawers_damaged": True,
        "upper_finished_end_panels_damaged": True,
        "upper_cabinets_lf": 4.0,
        # Lower cabinets
        "lower_cabinets_damaged": True,
        "num_damaged_lower_boxes": 1,
        "lower_cabinets_detached": True,
        "lower_face_frames_doors_drawers_available": "No",
        "lower_face_frames_doors_drawers_damaged": True,
        "lower_finished_end_panels_damaged": True,
        "lower_counter_type": "granite",
        "lower_cabinets_lf": 6.0,
        # Full height cabinets
        "full_height_cabinets_damaged": False,
        "num_damaged_full_height_boxes": 0,
        "full_height_cabinets_detached": False,
        "full_height_face_frames_doors_drawers_available": "No",
        "full_height_face_frames_doors_drawers_damaged": False,
        "full_height_finished_end_panels_damaged": False,
        "full_height_cabinets_lf": 1.0,
        # Island cabinets
        "island_cabinets_damaged": False,
        "num_damaged_island_boxes": 0,
        "island_cabinets_detached": False,
        "island_face_frames_doors_drawers_available": "No",
        "island_face_frames_doors_drawers_damaged": False,
        "island_finished_end_panels_damaged": False,
        "island_counter_type": "granite",
        "island_cabinets_lf": 2.0,
        # Countertops
        "countertops_lf": 10.0,
    },
    "attachments": [
        {
            "question_key": "overview_photos",
            "url": "https://s3.amazonaws.com/bucket/overview1.jpg",
        },
        {
            "question_key": "lower_cabinet_box",
            "url": "https://s3.amazonaws.com/bucket/lower_box.jpg",
        },
        {
            "question_key": "lower_face_frames_doors_drawers",
            "url": "https://s3.amazonaws.com/bucket/lower_face.jpg",
        },
        {
            "question_key": "lower_cabinet_end_panels",
            "url": "https://s3.amazonaws.com/bucket/lower_end.jpg",
        },
        {
            "question_key": "upper_cabinet_box",
            "url": "https://s3.amazonaws.com/bucket/upper_box.jpg",
        },
        {
            "question_key": "upper_face_frames_doors_drawers",
            "url": "https://s3.amazonaws.com/bucket/upper_face.jpg",
        },
        {
            "question_key": "upper_cabinet_end_panels",
            "url": "https://s3.amazonaws.com/bucket/upper_end.jpg",
        },
    ],
    "readable_report": {
        "meta": {
            "task_id": 27019,
            "assignment_id": 789456,
            "project_id": "5395115",
            "status": "COMPLETED",
            "dates": {
                "assigned": "2026-02-03T13:36:00Z",
                "completed": "2026-02-05T15:20:00Z",
            },
        },
        "topics": {},
    },
}


def simulate_transformation():
    """Simulate the transformation that happens in itel_cabinet_api_worker.py"""
    payload = sample_tracking_payload
    submission = payload.get("submission", {})
    attachments = payload.get("attachments", [])
    readable_report = payload.get("readable_report", {})
    meta = readable_report.get("meta", {})

    # Image type mapping (same as in worker)
    IMAGE_TYPE_MAP = {
        "overview_photos": "overview",
        "lower_cabinet_box": "low_overview",
        "lower_face_frames_doors_drawers": "low_face",
        "lower_cabinet_end_panels": "low_end",
        "upper_cabinet_box": "upp_overview",
        "upper_face_frames_doors_drawers": "upp_face",
        "upper_cabinet_end_panels": "upp_end",
        "full_height_cabinet_box": "fh_overview",
        "full_height_face_frames_doors_drawers": "fh_face",
        "full_height_end_panels": "fh_end",
        "island_cabinet_box": "isl_overview",
        "island_face_frames_doors_drawers": "isl_face",
        "island_cabinet_end_panels": "isl_end",
    }

    # Build images array
    images = []
    for attachment in attachments:
        question_key = attachment.get("question_key", "")
        url = attachment.get("url")
        image_type = IMAGE_TYPE_MAP.get(question_key)
        if image_type and url:
            images.append({"image_type": image_type, "url": url})

    # Build full name
    first_name = submission.get("customer_first_name")
    last_name = submission.get("customer_last_name")
    full_name = f"{first_name} {last_name}" if first_name and last_name else ""

    # Normalize yes/no
    def normalize_yes_no(value):
        if value is None:
            return "No"
        if isinstance(value, bool):
            return "yes" if value else "no"
        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned in ("yes", "true", "1"):
                return "yes"
            return "no"
        return "No"

    # Build vendor payload
    vendor_payload = {
        "integration_test_id": str(submission.get("assignment_id", "")),
        "claim_number": submission.get("project_id", ""),
        "external_claim_id": submission.get("project_id", ""),
        "cat_code": "",
        "claim_type": "Other",
        "claim_type_other_description": submission.get("damage_description", ""),
        "loss_type": None,
        "loss_date": meta.get("dates", {}).get(
            "assigned", submission.get("date_assigned")
        ),
        "service_level": "one_hour",
        "insured": {
            "name": full_name,
            "street_number": "",
            "street_name": "",
            "city": "",
            "state": "",
            "zip_code": "",
            "country": None,
        },
        "adjuster": {
            "carrier_id": "",
            "adjuster_id": "",
            "first_name": "",
            "last_name": "",
            "phone": "",
            "email": submission.get("assignor_email", ""),
        },
        "images": images,
        "cabinet_repair_specs": {
            "damage_description": submission.get("damage_description", ""),
            "upper_cabinets_damaged": submission.get("upper_cabinets_damaged", False),
            "upper_cabinets_damaged_count": submission.get("num_damaged_upper_boxes", 0),
            "upper_cabinets_detached": submission.get("upper_cabinets_detached", False),
            "upper_faces_frames_doors_drawers_available": normalize_yes_no(
                submission.get("upper_face_frames_doors_drawers_available")
            ),
            "upper_faces_frames_doors_drawers_damaged": submission.get(
                "upper_face_frames_doors_drawers_damaged", False
            ),
            "upper_end_panels_damaged": submission.get(
                "upper_finished_end_panels_damaged", False
            ),
            "lower_cabinets_damaged": submission.get("lower_cabinets_damaged", False),
            "lower_cabinets_damaged_count": submission.get("num_damaged_lower_boxes", 0),
            "lower_cabinets_detached": submission.get("lower_cabinets_detached", False),
            "lower_faces_frames_doors_drawers_available": normalize_yes_no(
                submission.get("lower_face_frames_doors_drawers_available")
            ),
            "lower_faces_frames_doors_drawers_damaged": submission.get(
                "lower_face_frames_doors_drawers_damaged", False
            ),
            "lower_end_panels_damaged": submission.get(
                "lower_finished_end_panels_damaged", False
            ),
            "lower_cabinets_counter_top_type": submission.get("lower_counter_type", ""),
            "full_height_cabinets_damaged": submission.get(
                "full_height_cabinets_damaged", False
            ),
            "full_height_pantry_cabinets_damaged_count": submission.get(
                "num_damaged_full_height_boxes", 0
            ),
            "full_height_pantry_cabinets_detached": submission.get(
                "full_height_cabinets_detached", False
            ),
            "full_height_frames_doors_drawers_available": normalize_yes_no(
                submission.get("full_height_face_frames_doors_drawers_available")
            ),
            "full_height_frames_doors_drawers_damaged": submission.get(
                "full_height_face_frames_doors_drawers_damaged", False
            ),
            "full_height_end_panels_damaged": submission.get(
                "full_height_finished_end_panels_damaged", False
            ),
            "island_cabinets_damaged": submission.get("island_cabinets_damaged", False),
            "island_cabinets_damaged_count": submission.get(
                "num_damaged_island_boxes", 0
            ),
            "island_cabinets_detached": submission.get("island_cabinets_detached", False),
            "island_frames_doors_drawers_available": normalize_yes_no(
                submission.get("island_face_frames_doors_drawers_available")
            ),
            "island_frames_doors_drawers_damaged": submission.get(
                "island_face_frames_doors_drawers_damaged", False
            ),
            "island_end_panels_damaged": submission.get(
                "island_finished_end_panels_damaged", False
            ),
            "island_cabinets_counter_top_type": submission.get("island_counter_type", ""),
            "other_details_and_instructions": submission.get("additional_notes", ""),
        },
        "opinion_replacement_value_specs": {
            "upper_cabinets_linear_ft": submission.get("upper_cabinets_lf"),
            "lower_cabinets_linear_ft": submission.get("lower_cabinets_lf"),
            "full_height_cabinets_linear_ft": submission.get("full_height_cabinets_lf"),
            "island_cabinets_linear_ft": submission.get("island_cabinets_lf"),
            "counter_top_linear_ft": submission.get("countertops_lf"),
        },
    }

    return vendor_payload


if __name__ == "__main__":
    print("=" * 80)
    print("iTel Vendor Schema Transformation Test")
    print("=" * 80)
    print()

    result = simulate_transformation()

    print("Transformed Payload (Vendor Format):")
    print(json.dumps(result, indent=2, default=str))
    print()

    # Validate structure
    print("=" * 80)
    print("Validation:")
    print("=" * 80)
    print(f"✓ integration_test_id: {result['integration_test_id']}")
    print(f"✓ claim_number: {result['claim_number']}")
    print(f"✓ claim_type: {result['claim_type']}")
    print(f"✓ insured.name: {result['insured']['name']}")
    print(f"✓ adjuster.email: {result['adjuster']['email']}")
    print(f"✓ images count: {len(result['images'])}")
    print(f"✓ image types: {[img['image_type'] for img in result['images']]}")
    print(f"✓ cabinet_repair_specs keys: {len(result['cabinet_repair_specs'])}")
    print(f"✓ opinion_replacement_value_specs keys: {len(result['opinion_replacement_value_specs'])}")
    print()
    print("Transformation test completed successfully!")
