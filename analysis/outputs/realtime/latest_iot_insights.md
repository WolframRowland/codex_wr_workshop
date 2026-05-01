# Real-Time IoT Taxi Insights

Generated at: 2026-04-30T23:11:49+00:00
Window: 2025-12-02T13:45:00 to 2025-12-02T13:50:00 (5 minutes)
Mode: anomaly-demo

## Summary

- Scanned rows: 1,288,508
- Current-window rows: 1,003
- Previous-window rows: 997
- Negative fare rows: 20
- Negative tip rows: 0
- Negative distance rows: 0
- Invalid timestamp rows: 0
- Average fare: 30.06
- Average distance: 3.15

## Alerts

| Severity | Type | Source | Count | Explanation |
| --- | --- | --- | --- | --- |
| high | NEGATIVE_FARE_CLUSTER | yellow_tripdata_2025-12 VendorID=2 | 20 | Repeated negative fare values may indicate a meter, payment, or upstream device/configuration issue. |

## Source Quality Leaders

| Dataset | Source | Trips | Negative fares | Negative fare rate | Invalid timestamps | Avg fare |
| --- | --- | --- | --- | --- | --- | --- |
| yellow_tripdata_2025-12 | VendorID=2 | 450 | 20 | 0.04 | 0 | 30.65 |
| fhv_tripdata_2025-12 | dispatching_base_num=B00001 | 1 | 0 | 0.00 | 0 | n/a |
| fhv_tripdata_2025-12 | dispatching_base_num=B00111 | 2 | 0 | 0.00 | 0 | n/a |
| fhv_tripdata_2025-12 | dispatching_base_num=B00112 | 1 | 0 | 0.00 | 0 | n/a |
| fhv_tripdata_2025-12 | dispatching_base_num=B00254 | 1 | 0 | 0.00 | 0 | n/a |
| fhv_tripdata_2025-12 | dispatching_base_num=B00256 | 6 | 0 | 0.00 | 0 | n/a |
| fhv_tripdata_2025-12 | dispatching_base_num=B00412 | 28 | 0 | 0.00 | 0 | n/a |
| fhv_tripdata_2025-12 | dispatching_base_num=B00477 | 1 | 0 | 0.00 | 0 | n/a |
| fhv_tripdata_2025-12 | dispatching_base_num=B00559 | 1 | 0 | 0.00 | 0 | n/a |
| fhv_tripdata_2025-12 | dispatching_base_num=B00647 | 2 | 0 | 0.00 | 0 | n/a |

## Volume By Dataset

| Dataset | Trips |
| --- | --- |
| yellow_tripdata_2025-12 | 581 |
| fhv_tripdata_2025-12 | 413 |
| green_tripdata_2025-12 | 9 |

## Top Pickup Zones

| Zone | Trips |
| --- | --- |
| Upper East Side South | 46 |
| Upper East Side North | 38 |
| Midtown Center | 29 |
| Midtown East | 26 |
| Penn Station/Madison Sq West | 26 |
| JFK Airport | 25 |
| Times Sq/Theatre District | 24 |
| Midtown North | 22 |

## Identifier Note

These TLC files do not include a true per-cab IoT device ID. The monitor groups by the most specific available source field, such as VendorID, dispatching_base_num, originating_base_num, or hvfhs_license_num.
