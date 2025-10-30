# Launch Trigger Uninstall Analysis
## Comprehensive Study of 1-Hour Post-Launch Uninstall Patterns

**Analysis Date:** October 2025
**Analyst:** EDA Team
**Dataset:** WPS Factory-Installed Trial Users (August 2025 Cohort)
**Total Users Analyzed:** 461,897

---

## Executive Summary
XX
### Key Findings
XX

---

## Table of Contents

1. [Methodology](#methodology)
2. [Data Overview](#data-overview)
3. [Part X: De](#part-1)
.....
10. [Part Y:](#part-8)
11. [Recommendations](#recommendations)
12. [Technical Appendix](#technical-appendix)

---

## Methodology

### Analysis Approach

### Data Source

### Key Metrics

### Statistical Methods

---

## Data Overview

### Dataset Characteristics

### Triggers Analyzed

---

## Part 1: Overall Trigger Performance


### Query Complexity Notes

All queries run against `users.kartikey_chauhan.first_seen_devices_aug_25` table with filters:
- `device_source = 'WPS'`
- `product_install_factory = True`
- `product_subscription_type = 'trial'`
- `launched_application_source IS NOT NULL`

Time calculations use `DATEDIFF(minute, start, end)` for precision.

Median calculations use `PERCENTILE_CONT(0.5)` for accurate medians with large datasets.

### Reproducibility

All analysis code available in:
- `/notebooks/temp_code/06-comprehensive_trigger_analysis.py`
- `/notebooks/temp_code/06-volley2_user_journey.py`
- `/notebooks/temp_code/06-volley3_final_summary.py`

Results saved in:
- `/notebooks/temp_code/artifacts/06_overall_triggers.csv`
- `/notebooks/temp_code/artifacts/06_manufacturer_triggers.csv`
- `/notebooks/temp_code/artifacts/06_version_triggers.csv`
- `/notebooks/temp_code/artifacts/06_time_to_launch.csv`
- `/notebooks/temp_code/artifacts/06_launch_frequency.csv`
