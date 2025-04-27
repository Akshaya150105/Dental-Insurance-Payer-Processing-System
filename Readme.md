# Dental Insurance Payer Processing System

## Overview
It addresses the challenges of processing Electronic Remittance Advice (ERA) documents in the dental insurance industry by managing payer information, deduplicating payers, and assigning consistent display names.

## Implementation

### Database Schema
- **PayerGroup**: Stores parent organizations (e.g., "Delta Dental").
- **Payer**: Represents unique insurance companies with a `pretty_name` field (e.g., "Delta Dental of Arizona").
- **PayerDetail**: Captures raw payer data from sources (e.g., name, payer number).
- **Relationships**: 
  - `PayerGroup` 1-to-many `Payer`.
  - `Payer` 1-to-many `PayerDetail`.

### Mapping Algorithm
1. **Loading**: Processes the first non-legend sheets from `payers.xlsx` using `pandas` and `openpyxl`.
2. **Grouping**: Infers payer groups using predefined rules and semantic matching with `rapidfuzz`.
3. **Deduplication**: Matches payers by payer number and name similarity, handling edge cases:
   - Different names, same payer number (same or different payers based on context).
   - Same name, different payer numbers (semantic matching).
4. **Storage**: Bulk inserts into SQLite for efficiency.

### UI Components
- **Map Payers**: View raw payer details and their mappings.
- **Manage Groups**: Merge payer groups manually.
- **Set Pretty Name**: Assign standardized display names to payers.

## Setup
1. **Install Dependencies**:
   ```bash
   pip install flask flask-sqlalchemy pandas openpyxl rapidfuzz
