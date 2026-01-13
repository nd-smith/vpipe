import json
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field, fields, is_dataclass
from typing import List, Optional, Any, Dict, Union
from datetime import datetime
from collections import defaultdict

# ==========================================
# 1. PARSER UTILITIES & SCHEMA
# ==========================================

def camel_to_snake(name: str) -> str:
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def parse_date(date_str: str) -> Optional[datetime]:
    if not date_str or not isinstance(date_str, str): return None
    try: return datetime.fromisoformat(date_str)
    except ValueError: return None

def from_dict(data_class, data):
    if data is None: return None
    if is_dataclass(data_class):
        field_types = {f.name: f.type for f in fields(data_class)}
        kwargs = {}
        for key, value in data.items():
            snake_key = camel_to_snake(key)
            if snake_key in field_types:
                field_type = field_types[snake_key]
                origin = getattr(field_type, '__origin__', None)
                args = getattr(field_type, '__args__', [])
                if origin is list:
                    item_type = args[0]
                    kwargs[snake_key] = [from_dict(item_type, item) for item in value]
                elif origin is Union and type(None) in args:
                    non_none_type = next(t for t in args if t is not type(None))
                    kwargs[snake_key] = from_dict(non_none_type, value)
                else:
                    kwargs[snake_key] = from_dict(field_type, value)
        return data_class(**kwargs)
    elif data_class is datetime:
        return parse_date(data)
    return data

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime): return o.isoformat()
        return super().default(o)

# --- Minimal Schema Classes ---
@dataclass
class OptionAnswer: name: str
@dataclass
class NumberAnswer: value: float
@dataclass
class ResponseAnswerExport:
    type: str
    text: Optional[str] = None
    option_answer: Optional[OptionAnswer] = None
    number_answer: Optional[NumberAnswer] = None
    claim_media_ids: Optional[List[int]] = None
@dataclass
class FormControl: id: str
@dataclass
class QuestionAndAnswer:
    question_text: str
    component: str
    response_answer_export: ResponseAnswerExport
    form_control: FormControl
@dataclass
class Group:
    name: str
    group_id: str
    question_and_answers: List[QuestionAndAnswer] = field(default_factory=list)
@dataclass
class ResponseData:
    groups: List[Group] = field(default_factory=list)
@dataclass
class ExternalLinkData:
    url: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
@dataclass
class ApiResponse:
    assignment_id: int
    task_id: int
    task_name: str
    project_id: int
    form_id: str
    status: str
    assignor_email: Optional[str] = None 
    form_response_id: Optional[str] = None
    date_assigned: Optional[datetime] = None
    date_completed: Optional[datetime] = None
    external_link_data: Optional[ExternalLinkData] = None
    response: Optional[ResponseData] = None

# ==========================================
# 2. COLUMN MAPPING CONFIGURATION
# ==========================================

# NOTE: Keys are stripped of trailing spaces to ensure matching works
COLUMN_MAP = {
    # Measurements
    ("Linear Feet Capture", "Countertops (linear feet)"): "countertops_lf",
    ("Linear Feet Capture", "Lower Cabinets (linear feet)"): "lower_cabinets_lf",
    ("Linear Feet Capture", "Upper Cabinets (linear feet)"): "upper_cabinets_lf",
    ("Linear Feet Capture", "Full-Height Cabinets (linear feet)"): "full_height_cabinets_lf",
    ("Linear Feet Capture", "Island Cabinets (linear feet)"): "island_cabinets_lf",

    # Lower Cabinets
    ("Cabinet Types Damaged", "Lower Cabinets Damaged?"): "lower_cabinets_damaged",
    ("Lower Cabinets", "Enter Number of Damaged Lower Boxes"): "num_damaged_lower_boxes",
    ("Lower Cabinets", "Are The Lower Cabinets Detached?"): "lower_cabinets_detached",
    ("Lower Cabinets", "Are All The Face Frames, Doors, And Drawers Available?"): "lower_face_frames_doors_drawers_available",
    ("Lower Cabinets", "Are All The Face Frames, Doors, And Drawer Fronts Damaged?"): "lower_face_frames_doors_drawers_damaged",
    ("Lower Cabinets", "Are The Lower Finished End Panels Damaged?"): "lower_finished_end_panels_damaged",
    ("Capture Lower End Panel", "Is there lower cabinet end panel damage?"): "lower_end_panel_damage_present",
    ("Lower Cabinet Counter Type", "Select Lower Cabinet Counter Type"): "lower_counter_type",

    # Upper Cabinets
    ("Cabinet Types Damaged", "Upper Cabinets Damaged?"): "upper_cabinets_damaged",
    ("Upper Cabinets", "Enter Number of Damaged Upper Boxes"): "num_damaged_upper_boxes",
    ("Upper Cabinets", "Are The Upper Cabinets Detached?"): "upper_cabinets_detached",
    ("Upper Cabinets", "Are All The Face Frames, Doors, And Drawers Available?"): "upper_face_frames_doors_drawers_available",
    ("Upper Cabinets", "Are All The Face Frames, Doors, And Drawer Fronts Damaged?"): "upper_face_frames_doors_drawers_damaged",
    ("Upper Cabinets", "Are The Upper Finished End Panels Damaged?"): "upper_finished_end_panels_damaged",
    ("Capture Upper End Panel", "Is there upper cabinet end panel damage?"): "upper_end_panel_damage_present",

    # Full Height
    ("Cabinet Types Damaged", "Full Height Cabinets Damaged?"): "full_height_cabinets_damaged",
    ("Full Height/Pantry Cabinets", "Enter Number of Damaged Full Height Boxes"): "num_damaged_full_height_boxes",
    ("Full Height/Pantry Cabinets", "Are The Full Height Cabinets Detached?"): "full_height_cabinets_detached",
    ("Full Height/Pantry Cabinets", "Are All The Face Frames, Doors, And Drawers Available?"): "full_height_face_frames_doors_drawers_available",
    ("Full Height/Pantry Cabinets", "Are All The Face Frames, Doors, And Drawer Fronts Damaged?"): "full_height_face_frames_doors_drawers_damaged",
    ("Full Height/Pantry Cabinets", "Are the Full Height Finished End Panels Damaged?"): "full_height_finished_end_panels_damaged",

    # Island
    ("Cabinet Types Damaged", "Island Cabinets Damaged?"): "island_cabinets_damaged",
    ("Island Cabinets", "Enter Number of Damaged Island Boxes"): "num_damaged_island_boxes",
    ("Island Cabinets", "Are The Island Cabinets Detached?"): "island_cabinets_detached",
    ("Island Cabinets", "Are All The Face Frames, Doors, And Drawers Available?"): "island_face_frames_doors_drawers_available",
    ("Island Cabinets", "Are All The Face Frames, Doors, And Drawer Fronts Damaged?"): "island_face_frames_doors_drawers_damaged",
    ("Island Cabinets", "Are the Island Finished End Panels Damaged?"): "island_finished_end_panels_damaged",
    ("Capture Island End Panel", "Is there island cabinet end panel damage?"): "island_end_panel_damage_present",
    ("Island Cabinet Counter Type", "Select Island Cabinet Counter Type"): "island_counter_type",

    # General
    ("Damage Description", "Enter Damaged Description"): "damage_description",
    ("Other Details and Information", "Now that you have been through the process, please provide any other details that you think would be relevant to the cabinet damage"): "additional_notes"
}

# ==========================================
# 3. BUILDER LOGIC
# ==========================================

class DataBuilder:
    @staticmethod
    def extract_value(export_data: ResponseAnswerExport):
        """Extracts and normalizes value (Yes/No -> True/False)."""
        if not export_data: return None
        
        val = None
        if export_data.type == 'text': val = export_data.text
        elif export_data.type == 'number': val = export_data.number_answer.value if export_data.number_answer else None
        elif export_data.type == 'option': val = export_data.option_answer.name if export_data.option_answer else None
        elif export_data.type == 'image': return export_data.claim_media_ids if export_data.claim_media_ids else []

        # Normalize Boolean
        if isinstance(val, str):
            clean_val = val.lower().strip()
            if clean_val == "yes": return True
            if clean_val == "no": return False
            if clean_val.startswith("yes,"): return True
            if clean_val.startswith("there is no"): return False
            
        return val

    @staticmethod
    def get_topic_category(group_name: str, question_text: str) -> str:
        text = (group_name + " " + question_text).lower()
        if 'island' in text: return 'Island Cabinets'
        if 'lower' in text: return 'Lower Cabinets'
        if 'upper' in text: return 'Upper Cabinets'
        if 'full height' in text or 'pantry' in text: return 'Full Height / Pantry'
        if 'countertop' in text: return 'Countertops'
        return 'General'

    @staticmethod
    def process(api_obj: ApiResponse):
        if not api_obj: return {}, [], {}

        # --- A. Setup Base Objects ---
        
        # 1. DB Row
        form_row = {
            "assignment_id": api_obj.assignment_id,
            "task_id": api_obj.task_id,
            "project_id": str(api_obj.project_id),
            "form_id": api_obj.form_id,
            "form_response_id": api_obj.form_response_id,
            "task_name": api_obj.task_name,
            "status": api_obj.status,
            "date_assigned": api_obj.date_assigned,
            "date_completed": api_obj.date_completed,
            "ingested_at": datetime.now(),
            "assignor_email": api_obj.assignor_email,
            "external_link_url": api_obj.external_link_data.url if api_obj.external_link_data else None,
            "customer_first_name": api_obj.external_link_data.first_name if api_obj.external_link_data else None,
            "customer_last_name": api_obj.external_link_data.last_name if api_obj.external_link_data else None,
            "customer_email": api_obj.external_link_data.email if api_obj.external_link_data else None,
            "customer_phone": api_obj.external_link_data.phone if api_obj.external_link_data else None,
            **{v: None for v in COLUMN_MAP.values()} # Init mapped cols
        }

        attachments_rows = []
        
        # 2. Readable/Raw Data Accumulators
        readable_report = {
            "meta": {
                "task_id": api_obj.task_id,
                "project_id": str(api_obj.project_id),
                "status": api_obj.status,
                "dates": {"assigned": api_obj.date_assigned, "completed": api_obj.date_completed}
            },
            "topics": defaultdict(list)
        }
        
        # For the raw_data column (preserves original group structure)
        raw_data_flat = defaultdict(list)

        # --- B. Iterate & Populate ---
        if api_obj.response and api_obj.response.groups:
            for group in api_obj.response.groups:
                # Normalize group name for safer matching (remove trailing spaces)
                clean_group_name = group.name.strip()

                for qa in group.question_and_answers:
                    clean_question_text = qa.question_text.strip()
                    answer_val = DataBuilder.extract_value(qa.response_answer_export)
                    
                    # 1. Map to Form Table
                    map_key = (clean_group_name, clean_question_text)
                    if map_key in COLUMN_MAP:
                        col_name = COLUMN_MAP[map_key]
                        form_row[col_name] = answer_val
                    
                    # 2. Map to Attachments Table
                    topic = DataBuilder.get_topic_category(clean_group_name, clean_question_text)
                    if qa.response_answer_export.type == 'image' and isinstance(answer_val, list):
                        for idx, media_id in enumerate(answer_val):
                            attachments_rows.append({
                                "assignment_id": api_obj.assignment_id,
                                "event_id": None, # Populate if available in source
                                "control_id": qa.form_control.id,
                                "question_text": clean_question_text,
                                "topic_category": topic,
                                "claim_media_id": media_id,
                                "display_order": idx + 1,
                                "created_at": datetime.now(),
                                "is_active": True,
                                "media_type": "image/jpeg", # Default assumption, update if logic exists
                                "blob_path": None # To be populated by downloader
                            })

                    # 3. Populate Readable Report (Categorized)
                    readable_item = {
                        "question": clean_question_text,
                        "answer": answer_val,
                        "type": qa.response_answer_export.type,
                        "control_id": qa.form_control.id
                    }
                    readable_report["topics"][topic].append(readable_item)

                    # 4. Populate Raw Data Blob (Grouped by original section)
                    raw_data_flat[clean_group_name].append({
                        "q": clean_question_text,
                        "a": answer_val,
                        "id": qa.form_control.id
                    })

        # Finalize Form Row with raw_data blob
        form_row["raw_data"] = json.dumps(raw_data_flat, cls=DateTimeEncoder)

        return form_row, attachments_rows, readable_report

# ==========================================
# 4. MAIN EXECUTION
# ==========================================

def process_file(source_file_path: str):
    if not source_file_path:
        print("Error: No file path provided.")
        return

    path = Path(source_file_path)
    if not path.exists() or not path.is_file():
        print(f"Error: Invalid file {path}")
        return

    print(f"Processing: {path.name}...")

    try:
        with open(path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        api_object = from_dict(ApiResponse, raw_data)
        form_row, attachment_rows, readable_json = DataBuilder.process(api_object)

        # Output 1: Forms Table (DB Load)
        forms_path = path.parent / f"{path.stem}_forms.json"
        with open(forms_path, 'w', encoding='utf-8') as f:
            json.dump([form_row], f, cls=DateTimeEncoder, indent=2)

        # Output 2: Attachments Table (DB Load)
        attach_path = path.parent / f"{path.stem}_attachments.json"
        with open(attach_path, 'w', encoding='utf-8') as f:
            json.dump(attachment_rows, f, cls=DateTimeEncoder, indent=2)

        # Output 3: Readable JSON (API Consumption)
        readable_path = path.parent / f"{path.stem}_readable.json"
        with open(readable_path, 'w', encoding='utf-8') as f:
            json.dump(readable_json, f, cls=DateTimeEncoder, indent=2)

        print("------------------------------------------------")
        print("SUCCESS")
        print(f"1. Forms Data:       {forms_path}")
        print(f"2. Attachments Data: {attach_path}")
        print(f"3. Readable API:     {readable_path}")
        print("------------------------------------------------")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        process_file(sys.argv[1])
    else:
        user_input = input("Enter source JSON path: ").strip().strip('"')
        process_file(user_input)