# Ohuseire API Reference

This document collects the public Ohuseire source API details used across the repository.

Base URL:
- `https://www.ohuseire.ee/api`

Default locale used in this repo:
- `en`

## Core Endpoints

### Station Catalog

URL pattern:
- `https://www.ohuseire.ee/api/station/<locale>`

Example:
- `https://www.ohuseire.ee/api/station/en`

Response shape:
- GeoJSON-like object with top-level `type` and `features[]`

Useful fields:
- `id`
- `properties.name`
- `properties.type`
- `properties.airviro_code`
- `properties.indicators`

### Indicator Catalog

URL pattern:
- `https://www.ohuseire.ee/api/indicator/<locale>?type=<INDICATOR|POLLEN>`

Examples:
- `https://www.ohuseire.ee/api/indicator/en?type=INDICATOR`
- `https://www.ohuseire.ee/api/indicator/en?type=POLLEN`

Response shape:
- JSON list

Useful fields:
- `id`
- `name`
- `formula`
- `unit`
- `description`
- `levels[]`

### Monitoring Measurements

URL pattern:
- `https://www.ohuseire.ee/api/monitoring/<locale>?stations=<id>&type=<INDICATOR|POLLEN>&range=<dd.MM.yyyy,dd.MM.yyyy>`

Example:
- `https://www.ohuseire.ee/api/monitoring/en?stations=8&type=INDICATOR&range=10.03.2026,12.03.2026`

Observed optional parameter:
- `indicators=<indicator_id[,indicator_id...]>`

Example:
- `https://www.ohuseire.ee/api/monitoring/en?stations=8&type=INDICATOR&range=10.03.2026,12.03.2026&indicators=66`

Response shape:
- JSON list

Useful fields:
- `measured`
- `value`
- `station`
- `indicator`

## Practical Discovery Order

1. Call `station/<locale>` to discover station ids and the indicator ids listed for each station.
2. Call `indicator/<locale>` for the matching type to translate indicator ids into names, formulas, and units.
3. Call `monitoring/<locale>` for the actual measurements.

## Observed API Behavior

These checks were verified against the live public API on March 28, 2026.

### No Separate Public API Landing Page Found

- `https://www.ohuseire.ee/api`
- `https://www.ohuseire.ee/api/`

Both returned `404` in live checks, and no public OpenAPI, Swagger, or separate API manual page was found. In practice, the metadata endpoints are the live documentation.

### Station 19 Is Not In The Published Station Catalog

The station catalog returned `37` stations, and id `19` was not present.

Direct requests such as:
- `.../monitoring/en?stations=19&type=INDICATOR&range=10.03.2026,12.03.2026`
- `.../monitoring/en?stations=19&type=POLLEN&range=10.03.2026,12.03.2026`

returned `HTTP 500`, not station `19` data. Based on that evidence, station `19` should be treated as unsupported or invalid rather than "present but missing from metadata".

### Manual Indicator Filtering Works, But Has Sharp Edges

The undocumented `indicators=` filter does work.

Verified example:
- `station=8`, `indicator=66`, range `01.01.2020..12.03.2026`
- returned `54,312` rows
- first row: `2020-01-01 00:00:00`
- last row: `2026-03-12 23:00:00`
- runtime was about `15` seconds in the devcontainer

Important caveat:
- if the requested indicator does not belong to the requested station, the API can behave unexpectedly instead of failing cleanly

Examples observed on March 28, 2026:
- `stations=8&indicators=34` returned `864` rows for `12` other stations with indicator `34`
- `stations=999&indicators=66` returned station `8` temperature-at-10m rows
- `stations=19&indicators=66` also returned station `8` rows
- `stations=8&indicators=33` returned `HTTP 500`

Recommendation:
- only use manual `indicators=` overrides after validating the station-indicator combination against `station/<locale>`
- do not assume the API will enforce the station filter correctly for invalid combinations

### Duplicate-Looking Indicator Metadata Exists

The indicator catalog contains duplicate-looking ids with the same name, formula, and unit.

Verified duplicates:
- `[28, 29]` Pressure / `PRESS`
- `[31, 32]` Relative humidity / `HUM`
- `[33, 34]` Temperature / `TEMP`
- `[1, 73]` Sulphur dioxide / `SO2`
- `[3, 74]` Nitrogen dioxide / `NO2`
- `[4, 75]` Carbon monoxide / `CO`
- `[11, 76]` Hydrogen sulphide / `H2S`
- `[21, 77]` Fine particulate matter / `PM10`

Not all duplicate ids appear to be live in current monitoring data.

Examples from March 10-12, 2026:
- `31` returned live humidity rows for station `38`
- `32` returned `HTTP 500`
- `34` returned live temperature rows for multiple stations
- `33` returned `HTTP 500`
- `66` returned live rows only for station `8`

### Station 8 Temperature Really Uses Indicator 66 In Current Metadata

Published station metadata for station `8` lists:
- `[21, 23, 4, 3, 1, 6, 37, 41, 66]`

In the same live tests:
- `stations=8&indicators=66` returned only station `8` rows
- `stations=8&indicators=34` did not return station `8`

So for the current public API, station `8` should still be treated as a `66` station for temperature-like data, even though most other stations use `34`.

## Teaching/ETL Guidance

- Trust the station catalog first.
- Treat `indicators=` as an advanced probe, not a default extraction path.
- Prefer long-form storage keyed by actual `station`, `indicator`, and `measured` values.
- When manual indicator overrides are needed for investigation, validate the returned `station` ids before loading anything into the warehouse.
