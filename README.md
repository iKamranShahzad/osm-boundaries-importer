# OSM Administrative Boundaries Importer

An iterative administrative boundaries importer for ArangoDB that fetches OpenStreetMap data using the Overpass API.

## Features

- **Level-by-level fetching** with geographic containment using `map_to_area`
- **Robust error handling** with retry mechanisms and rate limiting
- **Accurate parent-child relationships** through geographic queries
- **Comprehensive indexing** for optimal query performance
- **Progress tracking** with detailed statistics

## Configuration

Update the configuration in `osm.js`:

```javascript
const Config = {
  database: {
    url: 'http://127.0.0.1:8529',
    name: 'your_database_name',
    username: 'your_username',
    password: 'your_password',
  },
  // ... other config options
};
```

## Usage

### Run for all countries:

```bash
node osm.js
```

### Run for specific countries (ISO2 codes):

```bash
node osm.js US GB CA
```

### Run for specific country IDs:

```bash
node osm.js countryMetadata/5253251
```

## Requirements

- Node.js 14+
- ArangoDB 3.8+
- `arangojs` package
- `node-fetch` package (for older Node.js versions)

## Database Collections

- `adminBoundaries` - Administrative boundary documents
- `adminBoundaryEdges` - Parent-child relationships
- `countryMetadata` - Country reference data (must exist)

## Administrative Levels

The importer handles levels 2-10:

- Level 2: Country
- Level 3: Region
- Level 4: State/Province
- Level 5: Division
- Level 6: District
- Level 7: Sub-district
- Level 8: City/Municipality
- Level 9: Ward/Village
- Level 10: Neighborhood

## License

MIT
