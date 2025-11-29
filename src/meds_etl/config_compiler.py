"""
Config compiler - converts templated config to pre-parsed format.

This ensures templates are parsed ONCE at startup, not repeatedly during ETL.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from meds_etl.config_integration import extract_column_names_from_template, is_template_syntax


def _smart_split_pipes(s: str) -> List[str]:
    """Split on pipes while respecting quotes/parentheses (used for transforms)."""
    parts = []
    current = ""
    depth = 0
    in_quotes = False
    quote_char = None
    escaped = False

    for char in s:
        if escaped:
            current += char
            escaped = False
            continue

        if char == "\\":
            current += char
            escaped = True
            continue

        if char in ('"', "'"):
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
                quote_char = None
            current += char
            continue

        if not in_quotes:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            elif char == "|" and depth == 0:
                parts.append(current.strip())
                current = ""
                continue

        current += char

    if current.strip():
        parts.append(current.strip())

    return parts


def parse_template_expression(expr: str) -> Tuple[str, List[Dict[str, str]]]:
    """
    Parse a template expression and extract column name + transforms.

    Examples:
        "@note_title | regex_replace('\\s+', '-')"
        → ("note_title", [{"type": "regex_replace", "pattern": "\\s+", "replacement": "-"}])

        "@column | upper()"
        → ("column", [{"type": "upper"}])

    Returns:
        (column_name, list of transform dicts in old format)
    """
    # Split by pipes to get base and transforms
    parts = _smart_split_pipes(expr)

    # First part is the column reference
    base = parts[0]
    if base.startswith("@"):
        column_name = base[1:]
    else:
        column_name = base

    # Rest are transforms
    transforms = []
    for transform_str in parts[1:]:
        # Parse function call: function_name(args)
        match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]*)\)$", transform_str)
        if match:
            func_name = match.group(1)
            args_str = match.group(2).strip()

            # Parse arguments
            args = []
            if args_str:
                # Simple quote-aware splitting
                current = ""
                in_quotes = False
                quote_char = None

                for char in args_str:
                    if char in ('"', "'") and not in_quotes:
                        in_quotes = True
                        quote_char = char
                    elif char == quote_char and in_quotes:
                        in_quotes = False
                        quote_char = None
                    elif char == "," and not in_quotes:
                        args.append(current.strip().strip("\"'"))
                        current = ""
                    else:
                        current += char

                if current:
                    args.append(current.strip().strip("\"'"))

            # Convert to old transform format
            if func_name == "regex_replace" and len(args) == 2:
                transforms.append({"type": "regex_replace", "pattern": args[0], "replacement": args[1]})
            elif func_name == "replace" and len(args) == 2:
                transforms.append({"type": "replace", "pattern": args[0], "replacement": args[1]})
            elif func_name == "split":
                if len(args) == 3:
                    transforms.append(
                        {"type": "split", "delimiter": args[0], "index": int(args[1]), "default": args[2]}
                    )
                elif len(args) == 2:
                    transforms.append({"type": "split", "delimiter": args[0], "index": int(args[1])})
                elif len(args) == 1:
                    transforms.append({"type": "split", "delimiter": args[0]})
            elif func_name in ["upper", "lower", "strip", "trim"]:
                transforms.append({"type": func_name})

    return column_name, transforms


def convert_new_template_to_old_with_concept_mapping(template: str) -> Optional[Dict[str, Any]]:
    """
    Convert new template with $omop: lookups to old code_mappings format.

    New: "Gender/{$omop:@gender_concept_id}"
    Old: {
        "code_mappings": {
            "concept_id": {
                "concept_id_field": "gender_concept_id",
                "template": "Gender/{gender_concept_id}"
            }
        }
    }

    Also handles transforms:
    New: "NOTE/{@note_title | regex_replace('\\s+', '-')}"
    Old: {
        "code_mappings": {
            "source_value": {
                "template": "NOTE/{note_title}",
                "transforms": [{"type": "regex_replace", "pattern": "\\s+", "replacement": "-"}]
            }
        }
    }

    Returns dict with code_mappings if conversion successful, else None.
    """
    # Check if this is a simple field reference (not a template)
    # Simple field references should be handled by the simple parser in convert_code_expression
    if re.fullmatch(r"\$omop:@[a-zA-Z_][a-zA-Z0-9_]*", template):
        return None  # Let simple parser handle it
    if re.fullmatch(r"@[a-zA-Z_][a-zA-Z0-9_]*", template):
        return None  # Let simple parser handle it

    # Check if template has $omop: lookups
    has_omop = "$omop:" in template

    # Extract concept_id fields from $omop:@field patterns
    concept_fields = set()
    for match in re.finditer(r"\$omop:@([a-zA-Z_][a-zA-Z0-9_]*)", template):
        concept_fields.add(match.group(1))

    # Convert template to old format and extract transforms per column
    old_template = template
    column_transforms: Dict[str, List[Dict[str, Any]]] = {}  # Map column -> transforms
    template_columns: List[str] = []

    # Handle expressions inside braces with transforms: {@col | func()}
    def replace_braced_expr(match):
        expr = match.group(1)
        # Parse the expression to extract column and transforms
        col_name, transforms = parse_template_expression(expr)
        if transforms:
            column_transforms[col_name] = transforms
        template_columns.append(col_name)
        return "{" + col_name + "}"

    # Replace {@...} patterns
    old_template = re.sub(r"\{(@[^}]+)\}", replace_braced_expr, old_template)

    # Replace $omop:@field with {field} (inside or outside braces)
    old_template = re.sub(r"\$omop:@([a-zA-Z_][a-zA-Z0-9_]*)", r"{\1}", old_template)
    # Already in braces: {$omop:@field} → {{field}} → {field}
    old_template = re.sub(r"\{\{([^}]+)\}\}", r"{\1}", old_template)

    # Build result
    if has_omop and concept_fields:
        # Use concept_id mapping
        main_field = list(concept_fields)[0]
        field_role = infer_field_role(main_field)
        result = {"code_mappings": {"concept_id": {field_role: main_field, "template": old_template}}}
        if column_transforms:
            result["code_mappings"]["concept_id"]["column_transforms"] = column_transforms
    elif is_template_syntax(template):
        # Determine if template should map via concept_id or source_value
        concept_field = None
        for col in template_columns:
            if col.endswith("_concept_id"):
                concept_field = col
                break
        if concept_field:
            result = {"code_mappings": {"concept_id": {"concept_id_field": concept_field, "template": old_template}}}
            if column_transforms:
                result["code_mappings"]["concept_id"]["column_transforms"] = column_transforms
        else:
            result = {"code_mappings": {"source_value": {"template": old_template}}}
            if column_transforms:
                result["code_mappings"]["source_value"]["column_transforms"] = column_transforms
    else:
        return None

    return result


def infer_field_role(field: str) -> str:
    """Infer whether a concept field is source or standard."""
    if "source" in field and field.endswith("_concept_id"):
        return "source_concept_id_field"
    if field.endswith("_source_concept_id"):
        return "source_concept_id_field"
    return "concept_id_field"


def convert_code_expression(code: str) -> Optional[Dict[str, Any]]:
    """
    Convert code expression with fallbacks into standard code_mappings.
    Supports expressions like "$omop:@source || $omop:@concept || @source_value".
    """
    parts = [part.strip() for part in code.split("||")]
    if len(parts) > 1:
        concept_config: Dict[str, Any] = {}
        source_value_config: Dict[str, Any] = {}
        for part in parts:
            # Remove surrounding parentheses if present
            expr = part.strip()
            if expr.startswith("$omop:@"):
                field = expr[len("$omop:@") :]
                role = infer_field_role(field)
                concept_config[role] = field
            elif expr.startswith("$omop:"):
                # Literal fallback concept id
                try:
                    concept_config["fallback_concept_id"] = int(expr[len("$omop:") :])
                except ValueError:
                    pass
            elif expr.startswith("@"):
                source_value_config["field"] = expr[1:]

        result: Dict[str, Any] = {}
        if concept_config:
            result.setdefault("code_mappings", {})["concept_id"] = concept_config
        if source_value_config:
            result.setdefault("code_mappings", {})["source_value"] = source_value_config
        if result:
            return result

    # Fallback to template converter
    conversion = convert_new_template_to_old_with_concept_mapping(code)
    if conversion:
        return conversion

    if code.startswith("$omop:@"):
        field = code[len("$omop:@") :]
        return {"code_mappings": {"concept_id": {infer_field_role(field): field}}}

    if code.startswith("@"):
        if "|" in code:
            column_name, transforms = parse_template_expression(code)
            result = {"code_mappings": {"source_value": {"field": column_name}}}
            if transforms:
                result["code_mappings"]["source_value"]["transforms"] = transforms
            return result
        return {"code_mappings": {"source_value": {"field": code[1:]}}}

    return None


def compile_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compile templated config to old format for performance.

    Converts:
    - "@col1 || @col2" → time_start: "col1", time_start_fallbacks: ["col2"]
    - "value": "@col" → removes "value", keeps "name"
    - "value": "literal" → converts to "literal"

    This ensures zero performance overhead by compiling templates once.

    Args:
        config: Configuration (may use new or old format)

    Returns:
        Compiled configuration in old format
    """
    compiled = config.copy()

    # Process canonical_events
    if "canonical_events" in compiled:
        compiled["canonical_events"] = {}
        for event_name, event_config in config["canonical_events"].items():
            compiled["canonical_events"][event_name] = _compile_table_config(event_config)

    # Process tables
    if "tables" in compiled:
        compiled["tables"] = {}
        for table_name, table_config in config["tables"].items():
            compiled["tables"][table_name] = _compile_table_config(table_config)

    return compiled


def _compile_table_config(table_config: Dict[str, Any]) -> Dict[str, Any]:
    """Compile a single table/event config."""
    compiled = table_config.copy()

    # Compile time_start
    if "time_start" in compiled:
        time_start = compiled["time_start"]
        if isinstance(time_start, str) and is_template_syntax(time_start):
            # Extract columns from template and convert to old format
            columns = extract_column_names_from_template(time_start)
            # Split by || to get fallback order
            parts = [p.strip() for p in time_start.split("||")]
            if len(parts) > 1:
                # Extract column name from first part
                first_cols = extract_column_names_from_template(parts[0])
                if first_cols:
                    compiled["time_start"] = list(first_cols)[0]
                    # Rest become fallbacks
                    fallbacks = []
                    for part in parts[1:]:
                        cols = extract_column_names_from_template(part)
                        fallbacks.extend(cols)
                    compiled["time_start_fallbacks"] = fallbacks
            elif columns:
                # Single column
                compiled["time_start"] = list(columns)[0]

    # Compile time_end
    if "time_end" in compiled:
        time_end = compiled["time_end"]
        if isinstance(time_end, str) and is_template_syntax(time_end):
            # Extract columns from template
            columns = extract_column_names_from_template(time_end)
            parts = [p.strip() for p in time_end.split("||")]
            if len(parts) > 1:
                first_cols = extract_column_names_from_template(parts[0])
                if first_cols:
                    compiled["time_end"] = list(first_cols)[0]
                    fallbacks = []
                    for part in parts[1:]:
                        cols = extract_column_names_from_template(part)
                        fallbacks.extend(cols)
                    compiled["time_end_fallbacks"] = fallbacks
            elif columns:
                compiled["time_end"] = list(columns)[0]

    # Compile code field - convert to code_mappings format
    if "code" in compiled:
        code = compiled["code"]
        if isinstance(code, str) and not is_template_syntax(code):
            # Literal code like "MEDS_BIRTH" - keep as-is for fixed_code handling
            pass
        elif isinstance(code, str):
            conversion = convert_code_expression(code)
            if conversion:
                compiled.update(conversion)
                del compiled["code"]

    # Compile properties
    if "properties" in compiled:
        new_properties = []
        for prop in compiled["properties"]:
            new_prop = prop.copy()

            # Convert "value" field to old format
            if "value" in new_prop:
                value = new_prop["value"]
                if isinstance(value, str):
                    omop_match = re.match(r"^\$omop:@([a-zA-Z_][a-zA-Z0-9_]*)$", value)
                    simple_column = value.startswith("@") and not any(c in value for c in ["||", "$", "{", "}", "|"])

                    if omop_match:
                        col_name = omop_match.group(1)
                        new_prop["concept_lookup_field"] = col_name
                        del new_prop["value"]
                    elif simple_column:
                        col_name = value[1:]
                        new_prop["source"] = col_name
                        del new_prop["value"]
                    elif not is_template_syntax(value):
                        # Literal value - convert to old "literal" format
                        new_prop["literal"] = value
                        del new_prop["value"]
                    # else: complex template - leave as-is
                else:
                    # Literal value - convert to old "literal" format
                    new_prop["literal"] = value
                    del new_prop["value"]

            new_properties.append(new_prop)
        compiled["properties"] = new_properties

    # Compile numeric/text value fields
    def compile_value_field(key: str, target_key: str):
        if key in compiled:
            value = compiled.pop(key)
            if isinstance(value, str) and value.startswith("@") and not any(c in value for c in ["||", "$", "{", "|"]):
                compiled[target_key] = value[1:]

    compile_value_field("numeric_value", "numeric_value_field")
    compile_value_field("text_value", "text_value_field")

    return compiled
