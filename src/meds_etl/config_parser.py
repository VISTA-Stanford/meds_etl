"""
Template parser for OMOP ETL configuration files.

This module provides parsing for the new templated configuration syntax:
- @column_name: Column references
- $vocab:@column: Vocabulary lookups from column values
- $vocab:123: Vocabulary lookups from literal values
- {template}: Template expressions
- ||: Fallback chains
- | function(): Pipe-style transforms

Examples:
    "@drug_exposure_start_datetime || @drug_exposure_start_date"
    "$omop:@drug_type_concept_id"
    "IMAGE/{@modality_source_value}|{@anatomic_site_source_value}"
    "{@note_title | regex_replace('\\s+', '-')}"
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import polars as pl


@dataclass
class ColumnRef:
    """Reference to a column in the source table."""

    column_name: str


@dataclass
class LiteralValue:
    """A literal value (string or number)."""

    value: Union[str, int, float]


@dataclass
class VocabLookup:
    """Vocabulary lookup expression."""

    vocab_name: str  # e.g., "omop"
    source: Union[ColumnRef, LiteralValue]  # What to look up
    attribute: Optional[str] = None  # What to return (concept_name, concept_code, etc.)


@dataclass
class Transform:
    """A transform function to apply."""

    function_name: str
    args: List[str]


@dataclass
class Expression:
    """A single expression (can be column ref, vocab lookup, or literal)."""

    expr: Union[ColumnRef, VocabLookup, LiteralValue]
    transforms: List[Transform]


@dataclass
class TemplateString:
    """A template string with multiple parts."""

    parts: List[Union[str, Expression]]  # Mix of literals and expressions


@dataclass
class FallbackChain:
    """A chain of expressions with fallback logic."""

    expressions: List[Union[Expression, TemplateString]]


class TemplateParser:
    """
    Parser for template expressions in configuration files.

    Supports:
    - Column references: @column_name
    - Vocabulary lookups: $vocab:@column or $vocab:123 or $vocab:@column:attribute
    - Templates: {expr} where expr can contain column refs, vocab lookups
    - Fallbacks: expr1 || expr2 || expr3
    - Transforms: @column | upper() | regex_replace('\\s+', '-')
    """

    # Regular expressions for parsing
    FALLBACK_SPLIT = re.compile(r"\s*\|\|\s*")

    # Match column reference: @column_name
    COLUMN_REF = re.compile(r"^@([a-zA-Z_][a-zA-Z0-9_]*)$")

    # Match vocabulary lookup: $vocab:source:attribute
    # Examples: $omop:@drug_concept_id, $omop:123, $omop:@col:concept_name
    VOCAB_LOOKUP = re.compile(r"^\$([a-zA-Z_][a-zA-Z0-9_]*):([^:]+)(?::([a-zA-Z_][a-zA-Z0-9_]*))?$")

    # Match template expressions: {anything}
    TEMPLATE_EXPR = re.compile(r"\{([^}]+)\}")

    # Match transform pipes: | function(args)
    TRANSFORM_SPLIT = re.compile(r"\s*\|\s*")
    TRANSFORM_FUNC = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]*)\)$")

    def __init__(self, default_vocab: str = "omop", default_attribute: str = "concept_name"):
        """
        Initialize parser.

        Args:
            default_vocab: Default vocabulary for lookups (default: "omop")
            default_attribute: Default attribute to return from vocab lookups (default: "concept_name")
        """
        self.default_vocab = default_vocab
        self.default_attribute = default_attribute

    def parse(self, template: str) -> Union[Expression, TemplateString, FallbackChain, LiteralValue]:
        """
        Parse a template string.

        Args:
            template: Template string to parse

        Returns:
            Parsed template representation
        """
        # Handle empty or whitespace-only strings
        template = template.strip()
        if not template:
            return LiteralValue("")

        # Check for fallback chain (||)
        if "||" in template:
            parts = self.FALLBACK_SPLIT.split(template)
            expressions = [self._parse_single(part.strip()) for part in parts]
            return FallbackChain(expressions)

        return self._parse_single(template)

    def _parse_single(self, s: str) -> Union[Expression, TemplateString, LiteralValue]:
        """
        Parse a single expression (no fallbacks).

        Args:
            s: String to parse

        Returns:
            Parsed expression
        """
        # Check if this contains template braces
        if "{" in s and "}" in s:
            return self._parse_template_string(s)

        # Otherwise try to parse as a single expression
        return self._parse_expression(s)

    def _parse_template_string(self, s: str) -> TemplateString:
        """
        Parse a template string like "PREFIX/{@col1}|{@col2}".

        Args:
            s: Template string

        Returns:
            TemplateString object
        """
        parts = []
        last_end = 0

        for match in self.TEMPLATE_EXPR.finditer(s):
            # Add literal text before this match
            if match.start() > last_end:
                literal = s[last_end : match.start()]
                if literal:
                    parts.append(literal)

            # Parse the expression inside braces
            expr_text = match.group(1)
            expr = self._parse_expression(expr_text)
            parts.append(expr)

            last_end = match.end()

        # Add any remaining literal text
        if last_end < len(s):
            literal = s[last_end:]
            if literal:
                parts.append(literal)

        return TemplateString(parts)

    def _parse_expression(self, s: str) -> Union[Expression, LiteralValue]:
        """
        Parse a single expression (may include transforms).

        Args:
            s: Expression string

        Returns:
            Expression or LiteralValue
        """
        # Split by pipes (for transforms), respecting quotes and parentheses
        parts = self._smart_split_pipes(s)
        base_expr_str = parts[0].strip()
        transform_strs = [p.strip() for p in parts[1:]]

        # Parse the base expression
        base_expr = self._parse_base_expression(base_expr_str)

        # Parse transforms
        transforms = [self._parse_transform(t) for t in transform_strs]

        # If it's already a LiteralValue and no transforms, return as is
        if isinstance(base_expr, LiteralValue) and not transforms:
            return base_expr

        return Expression(expr=base_expr, transforms=transforms)

    def _smart_split_pipes(self, s: str) -> List[str]:
        """Split on pipes, respecting quotes and parentheses."""
        parts = []
        current = ""
        depth = 0
        in_quotes = False
        quote_char = None

        for char in s:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
                current += char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
                current += char
            elif char == "(" and not in_quotes:
                depth += 1
                current += char
            elif char == ")" and not in_quotes:
                depth -= 1
                current += char
            elif char == "|" and not in_quotes and depth == 0:
                parts.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            parts.append(current.strip())

        return parts

    def _parse_base_expression(self, s: str) -> Union[ColumnRef, VocabLookup, LiteralValue]:
        """
        Parse base expression (column ref, vocab lookup, or literal).

        Args:
            s: Expression string

        Returns:
            Parsed base expression
        """
        # Try column reference: @column_name
        match = self.COLUMN_REF.match(s)
        if match:
            return ColumnRef(match.group(1))

        # Try vocabulary lookup: $vocab:source:attribute
        match = self.VOCAB_LOOKUP.match(s)
        if match:
            vocab_name = match.group(1)
            source_str = match.group(2)
            attribute = match.group(3) or self.default_attribute

            # Parse the source (could be @column or literal)
            if source_str.startswith("@"):
                source = ColumnRef(source_str[1:])
            else:
                # Try to parse as number
                try:
                    source = LiteralValue(int(source_str))
                except ValueError:
                    try:
                        source = LiteralValue(float(source_str))
                    except ValueError:
                        source = LiteralValue(source_str)

            return VocabLookup(vocab_name, source, attribute)

        # Otherwise, treat as literal
        # Try to parse as number
        try:
            return LiteralValue(int(s))
        except ValueError:
            try:
                return LiteralValue(float(s))
            except ValueError:
                return LiteralValue(s)

    def _parse_transform(self, s: str) -> Transform:
        """
        Parse a transform function like "upper()" or "regex_replace('\\s+', '-')".

        Args:
            s: Transform string

        Returns:
            Transform object
        """
        match = self.TRANSFORM_FUNC.match(s)
        if not match:
            raise ValueError(f"Invalid transform syntax: {s}")

        func_name = match.group(1)
        args_str = match.group(2)

        # Parse arguments - properly handle quoted strings
        args = []
        if args_str:
            current_arg = ""
            in_quotes = False
            quote_char = None
            escaped = False

            for char in args_str:
                if escaped:
                    current_arg += char
                    escaped = False
                    continue

                if char == "\\":
                    escaped = True
                    current_arg += char
                    continue

                if char in ('"', "'"):
                    if not in_quotes:
                        in_quotes = True
                        quote_char = char
                    elif char == quote_char:
                        in_quotes = False
                        quote_char = None
                    else:
                        current_arg += char
                elif char == "," and not in_quotes:
                    # Strip only whitespace outside quotes
                    args.append(current_arg)
                    current_arg = ""
                elif in_quotes:
                    # Inside quotes, keep everything
                    current_arg += char
                elif char != " " or current_arg:  # Skip leading spaces outside quotes
                    current_arg += char

            # Add the last argument
            if current_arg or args:  # Keep even empty args if there were previous args
                args.append(current_arg)

        return Transform(func_name, args)


class PolarsExpressionBuilder:
    """
    Build Polars expressions from parsed templates.

    This class converts the parsed template objects into Polars expressions
    that can be executed efficiently on DataFrames.
    """

    def __init__(self, concept_df: Optional[pl.DataFrame] = None, default_vocab: str = "omop"):
        """
        Initialize expression builder.

        Args:
            concept_df: DataFrame containing vocabulary mappings
            default_vocab: Default vocabulary name
        """
        self.concept_df = concept_df
        self.default_vocab = default_vocab

    def build(
        self,
        parsed: Union[Expression, TemplateString, FallbackChain, LiteralValue],
        df_schema: Optional[Dict[str, pl.DataType]] = None,
    ) -> pl.Expr:
        """
        Build a Polars expression from parsed template.

        Args:
            parsed: Parsed template object
            df_schema: Schema of the input DataFrame (for validation)

        Returns:
            Polars expression
        """
        if isinstance(parsed, LiteralValue):
            return pl.lit(parsed.value)

        elif isinstance(parsed, Expression):
            return self._build_expression(parsed, df_schema)

        elif isinstance(parsed, TemplateString):
            return self._build_template_string(parsed, df_schema)

        elif isinstance(parsed, FallbackChain):
            return self._build_fallback_chain(parsed, df_schema)

        else:
            raise ValueError(f"Unknown parsed type: {type(parsed)}")

    def _build_expression(self, expr: Expression, df_schema: Optional[Dict[str, pl.DataType]] = None) -> pl.Expr:
        """Build expression from Expression object."""
        # Build base expression
        if isinstance(expr.expr, ColumnRef):
            base = pl.col(expr.expr.column_name)

        elif isinstance(expr.expr, VocabLookup):
            base = self._build_vocab_lookup(expr.expr, df_schema)

        elif isinstance(expr.expr, LiteralValue):
            base = pl.lit(expr.expr.value)

        else:
            raise ValueError(f"Unknown expression type: {type(expr.expr)}")

        # Apply transforms
        for transform in expr.transforms:
            base = self._apply_transform(base, transform)

        return base

    def _build_template_string(
        self, template: TemplateString, df_schema: Optional[Dict[str, pl.DataType]] = None
    ) -> pl.Expr:
        """Build expression from TemplateString object."""
        parts = []

        for part in template.parts:
            if isinstance(part, str):
                # Literal string
                parts.append(pl.lit(part))
            elif isinstance(part, Expression):
                expr = self._build_expression(part, df_schema)
                # Ensure it's a string for concatenation
                parts.append(expr.cast(pl.Utf8).fill_null(""))
            elif isinstance(part, LiteralValue):
                parts.append(pl.lit(str(part.value)))
            else:
                raise ValueError(f"Unknown template part type: {type(part)}")

        # Concatenate all parts
        if len(parts) == 1:
            return parts[0]
        return pl.concat_str(parts)

    def _build_fallback_chain(
        self, chain: FallbackChain, df_schema: Optional[Dict[str, pl.DataType]] = None
    ) -> pl.Expr:
        """Build expression from FallbackChain object."""
        exprs = []

        for item in chain.expressions:
            if isinstance(item, (Expression, TemplateString, LiteralValue)):
                exprs.append(self.build(item, df_schema))
            else:
                raise ValueError(f"Unknown fallback item type: {type(item)}")

        # Build coalesce expression
        return pl.coalesce(exprs)

    def _build_vocab_lookup(self, lookup: VocabLookup, df_schema: Optional[Dict[str, pl.DataType]] = None) -> pl.Expr:
        """
        Build vocabulary lookup expression.

        This creates a join with the concept DataFrame to map concept IDs to names/codes.
        """
        # Get the source value (column or literal)
        if isinstance(lookup.source, ColumnRef):
            source_expr = pl.col(lookup.source.column_name)
        elif isinstance(lookup.source, LiteralValue):
            source_expr = pl.lit(lookup.source.value)
        else:
            raise ValueError(f"Unknown lookup source type: {type(lookup.source)}")

        # If concept_df is not provided, we can't do the lookup
        # For now, just return the source expression
        # In a real implementation, this would do a join with the concept table
        if self.concept_df is None:
            # Return source as-is if no concept_df
            # This allows parsing to work even without vocabulary data
            return source_expr

        # TODO: Implement actual vocabulary lookup with join
        # This would join with concept_df on concept_id and return the requested attribute
        return source_expr

    def _apply_transform(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        """Apply a transform function to an expression."""
        func_name = transform.function_name.lower()
        args = transform.args

        if func_name == "upper":
            return expr.str.to_uppercase()

        elif func_name == "lower":
            return expr.str.to_lowercase()

        elif func_name == "strip" or func_name == "trim":
            return expr.str.strip_chars()

        elif func_name == "regex_replace":
            if len(args) != 2:
                raise ValueError(f"regex_replace requires 2 arguments, got {len(args)}")
            pattern, replacement = args
            return expr.str.replace_all(pattern, replacement)

        elif func_name == "replace":
            if len(args) != 2:
                raise ValueError(f"replace requires 2 arguments, got {len(args)}")
            old, new = args
            # Use replace_all to replace all occurrences
            return expr.str.replace_all(old, new, literal=True)

        elif func_name == "substring":
            if len(args) == 1:
                start = int(args[0])
                return expr.str.slice(start)
            elif len(args) == 2:
                start = int(args[0])
                length = int(args[1])
                return expr.str.slice(start, length)
            else:
                raise ValueError(f"substring requires 1 or 2 arguments, got {len(args)}")

        elif func_name == "split":
            if len(args) == 2:
                delimiter = args[0]
                index = int(args[1])
                return expr.str.split(delimiter).list.get(index, null_on_oob=True)
            elif len(args) == 1:
                delimiter = args[0]
                return expr.str.split(delimiter)
            else:
                raise ValueError(f"split requires 1 or 2 arguments, got {len(args)}")

        else:
            raise ValueError(f"Unknown transform function: {func_name}")


def parse_config_value(
    value: str, concept_df: Optional[pl.DataFrame] = None, df_schema: Optional[Dict[str, pl.DataType]] = None
) -> pl.Expr:
    """
    Parse a config value and return a Polars expression.

    This is the main entry point for parsing template strings in config files.

    Args:
        value: Template string from config
        concept_df: DataFrame containing vocabulary mappings
        df_schema: Schema of the input DataFrame

    Returns:
        Polars expression

    Examples:
        >>> parse_config_value("@drug_exposure_start_datetime || @drug_exposure_start_date")
        >>> parse_config_value("$omop:@drug_type_concept_id")
        >>> parse_config_value("IMAGE/{@modality_source_value}|{@anatomic_site_source_value}")
    """
    parser = TemplateParser()
    parsed = parser.parse(value)

    builder = PolarsExpressionBuilder(concept_df=concept_df)
    return builder.build(parsed, df_schema)
