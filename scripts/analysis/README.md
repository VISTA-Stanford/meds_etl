# MEDS Analysis Scripts

Scripts for analyzing MEDS datasets.

## `code_frequency.py`

Generate code frequency analysis from a MEDS dataset.

**Usage:**

```bash
uv run python scripts/analysis/code_frequency.py /path/to/meds/data

# Save full code list to CSV
uv run python scripts/analysis/code_frequency.py /path/to/meds/data -o codes.csv
```

**Output:**

1. **Vocabulary Summary** — Events and unique codes per vocabulary (SNOMED, LOINC, etc.)
2. **Example Codes** — Top 3 most frequent codes per vocabulary
3. **Frequency Distribution** — How many codes appear 1x, 2-10x, 11-100x, etc.

**Example output:**

```
============================================================
VOCABULARY SUMMARY
============================================================
vocabulary  unique_codes  total_events  pct
LOINC       12,345        500,000,000   38.50
SNOMED      45,678        400,000,000   30.80
RxNorm      8,901         200,000,000   15.40
...

============================================================
EXAMPLE CODES (top 3 per vocabulary)
============================================================

LOINC:
  - LOINC/12345-6 (25,000,000)
  - LOINC/78901-2 (18,500,000)
  - LOINC/34567-8 (12,000,000)

SNOMED:
  - SNOMED/123456789 (15,000,000)
  - SNOMED/987654321 (12,500,000)
  - SNOMED/456789012 (10,000,000)
...

============================================================
CODE FREQUENCY DISTRIBUTION
============================================================
frequency  codes   pct_codes  events        pct_events
1          5,234   8.50       5,234         0.00
2-10       12,456  20.24      75,000        0.01
11-100     18,789  30.53      1,200,000     0.09
101-1K     15,234  24.75      8,500,000     0.65
1K-10K     7,891   12.82      45,000,000    3.46
10K-100K   1,890   3.07       89,000,000    6.85
100K-1M    56      0.09       28,000,000    2.15
>1M        12      0.02       1,130,000,000 86.78
```
