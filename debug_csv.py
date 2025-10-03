#!/usr/bin/env python3
import csv

# Check what the home office city/state values are for the first few records
with open('examples/data/Active_Iowa_Business_Entities_20251001.csv', 'r') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        if i >= 10:
            break
        print(f"Row {i+1}: {row.get('Legal Name', '')}")
        print(f"  HO City: '{row.get('HO City', '')}' (empty: {not row.get('HO City', '').strip()})")
        print(f"  HO State: '{row.get('HO State', '')}' (empty: {not row.get('HO State', '').strip()})")
        print(f"  HO Location: '{row.get('HO Location', '')}'\n")