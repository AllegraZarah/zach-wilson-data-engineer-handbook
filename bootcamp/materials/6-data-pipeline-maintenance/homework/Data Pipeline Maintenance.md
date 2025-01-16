# Pipeline Management Plan

## Pipelines and Business Areas

1. **Profit**
   - Unit-level profit (for experiments)
   - Aggregate profit (reported to investors)

2. **Growth**
   - Daily growth (for experiments)
   - Aggregate growth (reported to investors)

3. **Engagement**
   - Aggregate engagement (reported to investors)

---

## Ownership

### Primary and Secondary Owners

| Pipeline                             | Primary Owner | Secondary Owner |
|--------------------------------------|---------------|-----------------|
| Unit-level profit                    | Toms          | Maya            |
| Aggregate profit                     | Danny         | Rene            |
| Daily growth                         | Maya          | Toms            |
| Aggregate growth                     | Rene          | Danny           |
| Aggregate engagement                 | Toms          | Danny           |

> **Note:** Ownership assignments were based on the engineers' prior experience and domain expertise. For example, Toms' deep familiarity with experimental design made them the ideal primary owner for the unit-level profit pipeline, while Danny's strong background in financial reporting aligns with aggregate profit responsibilities.

---

## On-Call Schedule

- **Rotation Plan**:
  - Each engineer will be on-call for one week at a time.
  - On-call responsibilities rotate sequentially among the four engineers.

### Holiday Adjustments

- During holidays, the on-call schedule will shift to ensure no one is on-call during their personal time off.
- Engineers will communicate holidays at least two weeks in advance.
- If a holiday coincides with an on-call week, the next engineer in rotation will take over, and the schedule will adjust accordingly.

### Sample On-Call Rotation Schedule

| Week  | On-Call Engineer |
|-------|------------------|
| 1     | Toms             |
| 2     | Maya             |
| 3     | Danny            |
| 4     | Rene             |
| 5     | Toms             |

### Visual Representation

![On-Call Rotation Calendar](on_call_rotation_calendar.png)

---

## Run Books for Investor Metrics

### Key Sections for Each Run Book

1. **Overview**
   - Description of the pipeline and its purpose.

2. **Pipeline Steps**
   - Data sources
   - ETL processes
   - Reporting mechanisms

3. **Monitoring and Alerts**
   - Metrics to monitor
   - Alert thresholds and escalation steps
   - **Service Level Agreements (SLAs):**
     - Critical issues: Response within 15 minutes, resolution within 1 hour.
     - High-priority issues: Response within 30 minutes, resolution within 4 hours.
     - Medium-priority issues: Response within 1 hour, resolution within 8 hours.
     - Low-priority issues: Response within 4 hours, resolution within 24 hours.

4. **Common Issues and Resolutions**
   - **Unit-Level Profit**
     - **Issue:** Data discrepancies from source systems
       - **Example:** Mismatched profit data between transaction logs and financial reports.
       - **Resolution Steps:**
         1. Cross-check transaction logs against financial reports.
         2. Identify mismatched entries and validate data integrity.
         3. Update mapping tables and re-run ETL jobs.
     - **Issue:** Experiment design changes leading to metric misalignment
       - **Example:** New KPIs added without updating pipeline logic.
       - **Resolution Steps:**
         1. Review updated experimental KPIs.
         2. Adjust pipeline logic to reflect new metrics.
         3. Perform end-to-end testing to ensure alignment.

   - **Aggregate Profit**
     - **Issue:** Incomplete or delayed data loads
       - **Example:** Scheduled data imports fail due to API downtime.
       - **Resolution Steps:**
         1. Check API logs for error details.
         2. Retry data loads manually.
         3. Configure fallback sources for future reliability.
     - **Issue:** Calculation errors during aggregation
       - **Example:** Incorrect formulas in aggregation scripts.
       - **Resolution Steps:**
         1. Review and validate aggregation scripts.
         2. Conduct peer code reviews.
         3. Run test scenarios to verify accuracy.

   - **Daily Growth**
     - **Issue:** Missing or delayed daily data updates
       - **Example:** Batch processing jobs exceed their time window.
       - **Resolution Steps:**
         1. Identify bottlenecks in job runtimes.
         2. Optimize ETL scripts for efficiency.
         3. Set up alerts to monitor job completion times.
     - **Issue:** High variance leading to alert fatigue
       - **Example:** Spikes in growth metrics triggering false-positive alerts.
       - **Resolution Steps:**
         1. Analyze historical data for normal variance.
         2. Implement dynamic alert thresholds.
         3. Regularly review and adjust alert settings.

   - **Aggregate Growth**
     - **Issue:** Incorrect aggregation logic
       - **Example:** Misclassified data leading to double-counting.
       - **Resolution Steps:**
         1. Review classification rules and aggregation logic.
         2. Correct data misclassifications.
         3. Validate results with test datasets.
     - **Issue:** Outdated reference data
       - **Example:** Growth benchmarks not updated with the latest market data.
       - **Resolution Steps:**
         1. Schedule regular updates for reference data sources.
         2. Verify benchmark consistency.
         3. Communicate updates to stakeholders.

   - **Aggregate Engagement**
     - **Issue:** Missing user activity logs
       - **Example:** Log files not generated due to server errors.
       - **Resolution Steps:**
         1. Monitor logging services for failures.
         2. Restart failed logging jobs.
         3. Backfill missing logs as needed.
     - **Issue:** Data transformation errors during ETL
       - **Example:** Null values introduced during schema changes.
       - **Resolution Steps:**
         1. Validate ETL transformations with test data.
         2. Add checks to catch null values early.
         3. Reprocess data to fill gaps.
     - **Issue:** Inconsistent user engagement metrics
       - **Example:** Variations in metric definitions across reporting layers.
       - **Resolution Steps:**
         1. Standardize metric definitions.
         2. Update all relevant documentation.
         3. Ensure uniform metrics across reports.

5. **Contact Information**
   - Primary and secondary owner contact details.

---

> **Note:** This document is a living resource. Owners should update it regularly to reflect changes in pipelines, ownership, and processes.
