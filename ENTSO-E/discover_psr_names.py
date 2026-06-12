"""
One-off helper: queries the ENTSO-E API for a single day and prints every PSR
name found in the A73 (Actual Generation per Generation Unit) response for DK1.

Run from the ENTSO-E/ directory:
    python discover_psr_names.py

Requires the API_KEY environment variable to be set.
"""
import os
import requests
import xml.etree.ElementTree as ET

API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("API_KEY environment variable is not set.")

BASE_URL = "https://web-api.tp.entsoe.eu/api"
DOMAIN_DK1 = "10Y1001A1001A796"
NAMESPACE_URI = "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"
NS = {"ns": NAMESPACE_URI}

# Query one day: 2026-01-14T22:00Z → 2026-01-15T22:00Z
PERIOD_START = "202601142200"
PERIOD_END   = "202601152200"

url = (
    f"{BASE_URL}?documentType=A73&processType=A16"
    f"&in_Domain={DOMAIN_DK1}"
    f"&periodStart={PERIOD_START}&periodEnd={PERIOD_END}"
    f"&securityToken={API_KEY}"
)

print(f"Querying: {PERIOD_START} → {PERIOD_END} (DK1, A73)")
response = requests.get(url, timeout=(60, 120))
response.raise_for_status()

root = ET.fromstring(response.content.decode('utf-8'))

psr_names = set()
for el in root.findall(".//ns:TimeSeries/ns:MktPSRType/ns:PowerSystemResources/ns:name", NS):
    if el.text:
        psr_names.add(el.text)

print(f"\nFound {len(psr_names)} PSR names in DK1 A73 response:\n")
for name in sorted(psr_names):
    marker = " <-- SOLAR?" if "solar" in name.lower() or "viuf" in name.lower() or "håstrup" in name.lower() or "hastrup" in name.lower() else ""
    print(f"  {name}{marker}")
