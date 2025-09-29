const { Database, aql } = require('arangojs');

/**
 * Iterative Administrative Boundaries Importer for ArangoDB
 * - Uses level-by-level fetching with geographic containment (like Script 1)
 * - Maintains robust infrastructure and error handling (from Script 2)
 * - Guarantees accurate parent-child relationships through map_to_area
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

const Config = {
  database: {
    url: 'http://127.0.0.1:8529',
    name: 'gigstarp_dev',
    username: 'root',
    password: 'test_db',
  },

  osm: {
    overpassUrl: 'https://overpass-api.de/api/interpreter',
    timeout: 250000, // 250 seconds per query
    maxRetries: 3,
    retryDelay: 5000, // 5 seconds initial
    rateLimitDelay: 2000, // 2 seconds between requests
  },

  processing: {
    batchSize: 1000,
    maxAdminLevel: 10, // Maximum admin level to fetch
    startAdminLevel: 2, // Start from country level
  },

  collections: {
    COUNTRY_METADATA: 'countryMetadata',
    ADMIN_BOUNDARIES: 'adminBoundaries',
    ADMIN_BOUNDARY_EDGES: 'adminBoundaryEdges',
  },
};

const ADMIN_LEVEL_METADATA = {
  2: { name: 'Country', priority: 0 },
  3: { name: 'Region', priority: 1 },
  4: { name: 'State/Province', priority: 2 },
  5: { name: 'Division', priority: 3 },
  6: { name: 'District', priority: 4 },
  7: { name: 'Sub-district', priority: 5 },
  8: { name: 'City/Municipality', priority: 6 },
  9: { name: 'Ward/Village', priority: 7 },
  10: { name: 'Neighborhood', priority: 8 },
  11: { name: 'Block/Locality', priority: 9 },
};

// ============================================================================
// DATABASE SERVICE
// ============================================================================

class DatabaseService {
  constructor(config) {
    this.config = config;
    this.db = null;
    this.collections = {};
  }

  async connect() {
    this.db = new Database({
      url: this.config.database.url,
      databaseName: this.config.database.name,
      auth: {
        username: this.config.database.username,
        password: this.config.database.password,
      },
    });

    await this.initializeCollections();
    return this;
  }

  async initializeCollections() {
    console.log('🔧 Initializing database collections...');

    try {
      // Ensure collections exist
      await this.ensureCollection(Config.collections.ADMIN_BOUNDARIES, {
        type: 2,
      });
      await this.ensureCollection(Config.collections.ADMIN_BOUNDARY_EDGES, {
        type: 3,
      });

      // Create indexes for boundaries collection
      const boundariesCol = this.db.collection(
        Config.collections.ADMIN_BOUNDARIES,
      );
      const indexes = [
        {
          type: 'persistent',
          fields: ['osm_id'],
          unique: true,
          name: 'idx_osm_id',
        },
        { type: 'persistent', fields: ['countryId'], name: 'idx_country' },
        { type: 'persistent', fields: ['admin_level'], name: 'idx_level' },
        {
          type: 'persistent',
          fields: ['custom_level'],
          name: 'idx_custom_level',
        },
        {
          type: 'persistent',
          fields: ['countryId', 'admin_level'],
          name: 'idx_country_level',
        },
        {
          type: 'persistent',
          fields: ['countryId', 'custom_level'],
          name: 'idx_country_custom_level',
        },
        {
          type: 'persistent',
          fields: ['parent_id'],
          name: 'idx_parent',
          sparse: true,
        },
        { type: 'fulltext', fields: ['name'], name: 'idx_name_fulltext' },
      ];

      for (const index of indexes) {
        try {
          await boundariesCol.ensureIndex(index);
        } catch (err) {
          if (!err.message.includes('duplicate')) {
            console.warn(`⚠️  Index creation warning: ${err.message}`);
          }
        }
      }

      // Create indexes for edges collection
      const edgesCol = this.db.collection(
        Config.collections.ADMIN_BOUNDARY_EDGES,
      );
      await edgesCol.ensureIndex({
        type: 'persistent',
        fields: ['_from', '_to'],
        unique: true,
        name: 'idx_edge_unique',
      });

      console.log('✅ Database initialized successfully');
    } catch (error) {
      console.error('❌ Database initialization failed:', error);
      throw error;
    }
  }

  async ensureCollection(name, options = {}) {
    const collection = this.db.collection(name);
    if (!(await collection.exists())) {
      await collection.create(options);
      console.log(`✅ Created collection: ${name}`);
    }
    this.collections[name] = collection;
    return collection;
  }

  async disconnect() {
    if (this.db) {
      await this.db.close();
    }
  }
}

// ============================================================================
// OSM SERVICE - Iterative Fetching
// ============================================================================

class OSMService {
  constructor(config) {
    this.config = config;
    this.fetch = null;
  }

  async initialize() {
    if (typeof globalThis.fetch !== 'undefined') {
      this.fetch = globalThis.fetch;
    } else {
      try {
        this.fetch = require('node-fetch');
      } catch {
        const module = await import('node-fetch');
        this.fetch = module.default;
      }
    }
    return this;
  }

  sleep(ms) {
    return new Promise((res) => setTimeout(res, ms));
  }

  async fetchWithRetry(url, options, retries = this.config.osm.maxRetries) {
    for (let i = 0; i < retries; i++) {
      try {
        const response = await this.fetch(url, {
          ...options,
          timeout: this.config.osm.timeout,
        });
        if (response.ok) return response;
        if (response.status === 429 || response.status >= 500) {
          const delay = this.config.osm.retryDelay * Math.pow(2, i);
          console.log(
            `⏳ Overpass retry in ${delay / 1000}s (attempt ${
              i + 1
            }/${retries})`,
          );
          await this.sleep(delay);
          continue;
        }
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      } catch (err) {
        if (i === retries - 1) throw err;
        const delay = this.config.osm.retryDelay * Math.pow(2, i);
        console.log(
          `⏳ Fetch error, retrying in ${delay / 1000}s: ${err.message}`,
        );
        await this.sleep(delay);
      }
    }
  }

  /**
   * Fetch the country boundary (level 2)
   */
  async fetchCountryBoundary(country) {
    console.log(
      `\n🌍 Fetching country boundary for ${country.name} (${country.iso2})...`,
    );

    const query = `
[out:json][timeout:${Math.floor(this.config.osm.timeout / 1000)}];
relation["admin_level"="2"]["ISO3166-1:alpha2"="${country.iso2}"];
out body;
    `;

    try {
      const response = await this.fetchWithRetry(this.config.osm.overpassUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `data=${encodeURIComponent(query)}`,
      });

      const data = await response.json();
      if (data && data.elements && data.elements.length > 0) {
        console.log(
          `✅ Found country boundary: ${
            data.elements[0].tags?.name || country.name
          }`,
        );
        return data.elements[0];
      }
    } catch (err) {
      console.error(`❌ Failed to fetch country boundary: ${err.message}`);
    }
    return null;
  }

  /**
   * Fetch next administrative level using geographic containment (like Script 1)
   */
  async fetchNextLevel(parentBoundary, targetAdminLevel) {
    const parentOsmId =
      parentBoundary.osm_id || parentBoundary.rel_id || parentBoundary.id;

    console.log(
      `   → Searching for admin_level=${targetAdminLevel} within rel(${parentOsmId})...`,
    );

    const query = `
[out:json][timeout:${Math.floor(this.config.osm.timeout / 1000)}];
rel(${parentOsmId});
map_to_area -> .parentArea;
(
  rel(area.parentArea)["admin_level"="${targetAdminLevel}"]["boundary"="administrative"];
);
out body;
    `;

    try {
      const response = await this.fetchWithRetry(this.config.osm.overpassUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `data=${encodeURIComponent(query)}`,
      });

      const data = await response.json();
      if (data && data.elements) {
        const uniqueElements = this.removeDuplicates(data.elements);
        if (uniqueElements.length > 0) {
          console.log(
            `      ✓ Found ${uniqueElements.length} boundaries at level ${targetAdminLevel}`,
          );
        }
        return uniqueElements;
      }
    } catch (err) {
      console.error(
        `      ✗ Failed to query level ${targetAdminLevel}: ${err.message}`,
      );
    }
    return [];
  }

  removeDuplicates(elements) {
    const unique = {};
    elements.forEach((e) => {
      if (!unique[e.id]) {
        unique[e.id] = e;
      }
    });
    return Object.values(unique);
  }
}

// ============================================================================
// ITERATIVE PROCESSOR - Level-by-level processing
// ============================================================================

class IterativeProcessor {
  constructor(config, db, osm) {
    this.config = config;
    this.db = db;
    this.osm = osm;
    this.processedBoundaries = new Map(); // Track all processed boundaries
  }

  /**
   * Process country iteratively, level by level
   */
  async processCountry(country) {
    console.log(`\n${'='.repeat(80)}`);
    console.log(`📍 Processing: ${country.name} (${country.iso2 || 'N/A'})`);
    console.log(`${'='.repeat(80)}`);

    const stats = {
      name: country.name,
      iso2: country.iso2,
      boundaries: 0,
      relationships: 0,
      levelStats: {},
      errors: [],
      startTime: Date.now(),
    };

    try {
      // Clear processed boundaries for new country
      this.processedBoundaries.clear();

      // Fetch country boundary first
      const countryBoundary = await this.osm.fetchCountryBoundary(country);
      if (!countryBoundary) {
        stats.errors.push('Could not fetch country boundary');
        return stats;
      }

      // Store country boundary
      const countryDoc = await this.storeBoundary(
        countryBoundary,
        country,
        null,
        2,
        0,
      );
      if (countryDoc) {
        this.processedBoundaries.set(String(countryBoundary.id), countryDoc);
        stats.boundaries++;
        stats.levelStats[2] = 1;
      }

      // Start processing from the country level
      const startingLevels = [
        {
          boundary: countryBoundary,
          doc: countryDoc,
          admin_level: 2,
          custom_level: 0,
        },
      ];

      await this.processLevels(startingLevels, country, stats);
    } catch (err) {
      console.error(`❌ Error processing ${country.name}:`, err);
      stats.errors.push(String(err.message || err));
    }

    stats.duration = Date.now() - stats.startTime;
    stats.boundaries = this.processedBoundaries.size;

    // Count relationships
    const relationships = await this.countRelationships(country);
    stats.relationships = relationships;

    return stats;
  }

  /**
   * Process levels iteratively (similar to Script 1's approach)
   */
  async processLevels(currentLevelBoundaries, country, stats) {
    if (!currentLevelBoundaries || currentLevelBoundaries.length === 0) {
      return;
    }

    const firstBoundary = currentLevelBoundaries[0];
    const currentAdminLevel = firstBoundary.admin_level;
    const currentCustomLevel = firstBoundary.custom_level;

    console.log(
      `\n📊 Processing ${currentLevelBoundaries.length} boundaries at admin_level=${currentAdminLevel}, custom_level=${currentCustomLevel}`,
    );

    // Process each boundary at current level
    const nextLevelBoundaries = [];

    for (const levelData of currentLevelBoundaries) {
      if (levelData.admin_level >= this.config.processing.maxAdminLevel) {
        console.log(
          `   ⚠️ Skipping - reached max admin level (${this.config.processing.maxAdminLevel})`,
        );
        continue;
      }

      // Try to fetch next levels (similar to Script 1's logic)
      for (
        let targetAdminLevel = levelData.admin_level + 1;
        targetAdminLevel <= this.config.processing.maxAdminLevel;
        targetAdminLevel++
      ) {
        const childBoundaries = await this.osm.fetchNextLevel(
          levelData.boundary,
          targetAdminLevel,
        );

        if (childBoundaries && childBoundaries.length > 0) {
          // Store these boundaries with proper parent relationship
          const stored = await this.storeChildBoundaries(
            childBoundaries,
            country,
            levelData.doc,
            targetAdminLevel,
            currentCustomLevel + 1,
            stats,
          );

          // Add to next level processing queue
          for (const child of stored) {
            nextLevelBoundaries.push({
              boundary: child.osmData,
              doc: child.doc,
              admin_level: targetAdminLevel,
              custom_level: currentCustomLevel + 1,
            });
          }

          // Found boundaries at this level, stop searching higher levels
          break;
        }
      }

      // Add small delay between boundaries to avoid rate limits
      await this.osm.sleep(this.config.osm.rateLimitDelay);
    }

    // Recursively process next level
    if (nextLevelBoundaries.length > 0) {
      await this.processLevels(nextLevelBoundaries, country, stats);
    }
  }

  /**
   * Store a single boundary
   */
  async storeBoundary(osmElement, country, parentDoc, adminLevel, customLevel) {
    const osmId = String(osmElement.id);

    // Check if already processed
    if (this.processedBoundaries.has(osmId)) {
      return this.processedBoundaries.get(osmId);
    }

    const name =
      osmElement.tags?.['name:en'] ||
      osmElement.tags?.name ||
      osmElement.tags?.official_name ||
      `Unnamed Level ${adminLevel}`;

    const doc = {
      osm_id: osmId,
      osm_type: osmElement.type || 'relation',
      name: name,
      name_en: osmElement.tags?.['name:en'] || null,
      official_name:
        osmElement.tags?.official_name || osmElement.tags?.name || null,
      admin_level: adminLevel,
      custom_level: customLevel,
      level_name:
        ADMIN_LEVEL_METADATA[adminLevel]?.name || `Level ${adminLevel}`,
      iso_code:
        osmElement.tags?.['ISO3166-2'] ||
        osmElement.tags?.['iso3166-2'] ||
        null,
      wikidata: osmElement.tags?.wikidata || null,
      wikipedia: osmElement.tags?.wikipedia || null,
      population: this.parsePopulation(osmElement.tags?.population),
      border_type: osmElement.tags?.border_type || null,
      countryId: country.countryId || country._id,
      parent_id: parentDoc?._id || null,
      tags: this.extractRelevantTags(osmElement.tags),
      created_at: new Date(),
      updated_at: new Date(),
    };

    try {
      const boundariesCol =
        this.db.collections[Config.collections.ADMIN_BOUNDARIES];

      // Check if exists
      const existing = await this.db.db.query(aql`
        FOR doc IN ${boundariesCol}
        FILTER doc.osm_id == ${osmId}
        RETURN doc
      `);
      const existingDoc = await existing.next();

      let savedDoc;
      if (existingDoc) {
        // Update existing
        await this.db.db.query(aql`
          UPDATE ${existingDoc._key}
          WITH ${doc}
          IN ${boundariesCol}
        `);
        savedDoc = { ...existingDoc, ...doc };
      } else {
        // Insert new
        const result = await boundariesCol.save(doc, { returnNew: true });
        savedDoc = result.new || result;
      }

      // Create edge if has parent
      if (parentDoc && savedDoc) {
        await this.createEdge(parentDoc._id, savedDoc._id);
      }

      return savedDoc;
    } catch (err) {
      console.error(`❌ Failed to store boundary ${name}: ${err.message}`);
      return null;
    }
  }

  /**
   * Store multiple child boundaries
   */
  async storeChildBoundaries(
    osmElements,
    country,
    parentDoc,
    adminLevel,
    customLevel,
    stats,
  ) {
    const stored = [];

    console.log(
      `      → Storing ${osmElements.length} boundaries at level ${adminLevel}...`,
    );

    for (const element of osmElements) {
      const doc = await this.storeBoundary(
        element,
        country,
        parentDoc,
        adminLevel,
        customLevel,
      );
      if (doc) {
        this.processedBoundaries.set(String(element.id), doc);
        stored.push({ doc, osmData: element });

        // Update stats
        stats.levelStats[adminLevel] = (stats.levelStats[adminLevel] || 0) + 1;
      }
    }

    console.log(`      ✓ Stored ${stored.length} boundaries successfully`);
    return stored;
  }

  /**
   * Create edge between parent and child
   */
  async createEdge(fromId, toId) {
    try {
      const edgesCol =
        this.db.collections[Config.collections.ADMIN_BOUNDARY_EDGES];

      // Check if edge exists
      const existing = await this.db.db.query(aql`
        FOR e IN ${edgesCol}
        FILTER e._from == ${fromId} AND e._to == ${toId}
        RETURN e
      `);

      const existingEdge = await existing.next();
      if (!existingEdge) {
        await edgesCol.save({
          _from: fromId,
          _to: toId,
          relationship: 'contains',
          created_at: new Date(),
        });
      }
    } catch (err) {
      console.error(
        `Failed to create edge ${fromId} -> ${toId}: ${err.message}`,
      );
    }
  }

  parsePopulation(value) {
    if (!value) return null;
    const parsed = parseInt(String(value).replace(/[^\d]/g, ''), 10);
    return isNaN(parsed) ? null : parsed;
  }

  extractRelevantTags(tags) {
    if (!tags) return null;
    const relevant = {};
    const keepTags = [
      'name',
      'name:en',
      'official_name',
      'type',
      'designation',
      'border_type',
    ];
    for (const tag of keepTags) {
      if (tags[tag]) relevant[tag] = tags[tag];
    }
    return Object.keys(relevant).length > 0 ? relevant : null;
  }

  async countRelationships(country) {
    const cursor = await this.db.db.query(aql`
      FOR b IN ${this.db.collections[Config.collections.ADMIN_BOUNDARIES]}
      FILTER b.countryId == ${country.countryId || country._id}
      FILTER b.parent_id != null
      RETURN 1
    `);
    const results = await cursor.all();
    return results.length;
  }
}

// ============================================================================
// MAIN ORCHESTRATOR
// ============================================================================

class BoundaryImporter {
  constructor() {
    this.db = null;
    this.osm = null;
    this.processor = null;
    this.statistics = {
      startTime: Date.now(),
      countries: [],
      totals: { boundaries: 0, relationships: 0, errors: 0 },
    };
  }

  async initialize() {
    console.log('🚀 Initializing Iterative Administrative Boundaries Importer');
    console.log('='.repeat(80));
    console.log('📝 Using level-by-level fetching with geographic containment');
    console.log('='.repeat(80));

    this.db = await new DatabaseService(Config).connect();
    this.osm = await new OSMService(Config).initialize();
    this.processor = new IterativeProcessor(Config, this.db, this.osm);
    return this;
  }

  async fetchCountries(countryFilter = null) {
    console.log('\n📊 Fetching countries to process...');
    let query;

    if (countryFilter && countryFilter.length > 0) {
      // Support both ISO2 codes and country IDs
      const isCountryId = countryFilter.some((f) => f.includes('/'));
      if (isCountryId) {
        query = aql`
          FOR country IN countryMetadata
          FILTER country._id IN ${countryFilter}
          RETURN { 
            _key: country._key, 
            name: country.name, 
            iso2: country.iso2, 
            countryId: country._id 
          }
        `;
      } else {
        query = aql`
          FOR country IN countryMetadata
          FILTER country.iso2 IN ${countryFilter} OR country.name IN ${countryFilter}
          RETURN { 
            _key: country._key, 
            name: country.name, 
            iso2: country.iso2, 
            countryId: country._id 
          }
        `;
      }
    } else {
      query = aql`
        FOR country IN countryMetadata
        RETURN { 
          _key: country._key, 
          name: country.name, 
          iso2: country.iso2, 
          countryId: country._id 
        }
      `;
    }

    const cursor = await this.db.db.query(query);
    const countries = await cursor.all();
    console.log(`✅ Found ${countries.length} countries to process`);
    return countries;
  }

  async run(countryFilter = null) {
    try {
      await this.initialize();
      const countries = await this.fetchCountries(countryFilter);

      if (countries.length === 0) {
        console.log('⚠️  No countries found to process');
        return;
      }

      for (const country of countries) {
        if (!country.iso2) {
          console.log(`⚠️  Skipping ${country.name} - no ISO2 code`);
          continue;
        }

        const result = await this.processor.processCountry(country);
        this.statistics.countries.push(result);
        this.statistics.totals.boundaries += result.boundaries;
        this.statistics.totals.relationships += result.relationships;
        if (result.errors.length > 0) {
          this.statistics.totals.errors += result.errors.length;
        }

        // Pause between countries to avoid rate limits
        if (countries.indexOf(country) < countries.length - 1) {
          console.log(
            '\n⏳ Waiting before next country (rate limit protection)...',
          );
          await new Promise((r) => setTimeout(r, 3000));
        }
      }

      this.displayFinalReport();
    } catch (err) {
      console.error('💥 Fatal error:', err);
    } finally {
      await this.cleanup();
    }
  }

  displayFinalReport() {
    const duration = Math.round(
      (Date.now() - this.statistics.startTime) / 1000,
    );
    console.log('\n' + '='.repeat(80));
    console.log('✨ IMPORT COMPLETED');
    console.log('='.repeat(80));
    console.log(`   Countries processed: ${this.statistics.countries.length}`);
    console.log(`   Total boundaries: ${this.statistics.totals.boundaries}`);
    console.log(
      `   Total relationships: ${this.statistics.totals.relationships}`,
    );
    console.log(`   Total errors: ${this.statistics.totals.errors}`);
    console.log(`   Duration: ${Math.floor(duration / 60)}m ${duration % 60}s`);

    for (const c of this.statistics.countries) {
      const time = Math.round(c.duration / 1000);
      console.log(`\n   ${c.name} (${c.iso2 || 'N/A'}):`);
      console.log(`     • Boundaries: ${c.boundaries}`);
      console.log(`     • Relationships: ${c.relationships}`);

      if (c.levelStats && Object.keys(c.levelStats).length > 0) {
        console.log(`     • By level:`);
        Object.entries(c.levelStats)
          .sort(([a], [b]) => parseInt(a) - parseInt(b))
          .forEach(([level, count]) => {
            const name = ADMIN_LEVEL_METADATA[level]?.name || `Level ${level}`;
            console.log(`        - ${name} (${level}): ${count}`);
          });
      }

      console.log(`     • Time: ${time}s`);
      if (c.errors && c.errors.length) {
        console.log(`     • Errors: ${c.errors.join('; ')}`);
      }
    }
  }

  async cleanup() {
    console.log('\n🧹 Cleaning up...');
    if (this.db) await this.db.disconnect();
    console.log('✅ Cleanup complete');
  }
}

// ============================================================================
// ENTRY POINT
// ============================================================================

if (require.main === module) {
  const importer = new BoundaryImporter();
  const args = process.argv.slice(2);

  // Can pass country IDs like: "countryMetadata/5253251"
  // Or ISO2 codes like: "US", "GB", "PK"

  importer
    .run(args.length ? args : null)
    .then(() => {
      console.log('\n✅ Import process finished');
      process.exit(0);
    })
    .catch((err) => {
      console.error('\n❌ Import process failed:', err);
      process.exit(1);
    });
}

module.exports = {
  BoundaryImporter,
  DatabaseService,
  OSMService,
  IterativeProcessor,
  Config,
};
