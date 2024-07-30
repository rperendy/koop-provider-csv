/*
  model-test.js

  This file is optional, but is strongly recommended. It tests the `getData` function to ensure its translating
  correctly.
*/

const test = require("tape");
const proxyquire = require("proxyquire");
const fetchMock = require("fetch-mock");
const fs = require("fs");
const path = require("path");
const { Readable } = require("stream");

test("it should throw an error if no path or url is provided", (t) => {
  t.plan(2);

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {},
      },
    },
  };

  const Model = proxyquire("../../src/model", {
    config,
  });
  const model = new Model();

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.ok(err, "has error");
    t.notok(geojson, "geojson does not exist");
  });
});

test("it should throw an error if both path and url are provided", (t) => {
  t.plan(2);

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {
          url: "http://test.com/test.csv",
          path: "./test.csv",
          geometryColumns: {
            longitude: "longitude",
            latitude: "latitude",
          },
        },
      },
    },
  };

  const Model = proxyquire("../../src/model", {
    config,
  });
  const model = new Model();

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.ok(err, "has error");
    t.notok(geojson, "geojson does not exist");
  });
});

test("it should send a request for a URL", (t) => {
  t.plan(3);

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {
          url: "http://my-site.com/points.csv",
          geometryColumns: {
            longitude: "longitude",
            latitude: "latitude",
          },
          metadata: {
            idField: "id",
          },
        },
      },
    },
  };

  const csv = fs.readFileSync(
    path.join(__dirname, "../fixtures/points.csv"),
    "utf-8"
  );

  const readable = new Readable();
  readable.push(csv);
  readable.push(null);

  const fetch = fetchMock
    .sandbox()
    .mock("http://my-site.com/points.csv", readable, { sendAsJson: false });

  const Model = proxyquire("../../src/model", {
    "node-fetch": fetch,
    config,
  });
  const model = new Model();

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.error(err, "no error");
    t.equal(
      geojson.type,
      "FeatureCollection",
      "creates a feature collection object"
    );
    t.ok(geojson.features, "has features");
  });
});

test("it should load the local file for a file path with .csv using the 'url' property", (t) => {
  t.plan(3);

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {
          url: path.join(__dirname, "../fixtures/points.csv"),
          geometryColumns: {
            longitude: "longitude",
            latitude: "latitude",
          },
          metadata: {
            idField: "id",
          },
        },
      },
    },
  };

  const Model = proxyquire("../../src/model", {
    config,
  });
  const model = new Model();

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.error(err, "no error");
    t.equal(
      geojson.type,
      "FeatureCollection",
      "creates a feature collection object"
    );
    t.ok(geojson.features, "has features");
  });
});

test("it should load the local file for a file path with .CSV using the 'url' property", (t) => {
  t.plan(3);

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {
          url: path.join(__dirname, "../fixtures/points.CSV"),
          geometryColumns: {
            longitude: "longitude",
            latitude: "latitude",
          },
          metadata: {
            idField: "id",
          },
        },
      },
    },
  };

  const Model = proxyquire("../../src/model", {
    config,
  });
  const model = new Model();

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.error(err, "no error");
    t.equal(
      geojson.type,
      "FeatureCollection",
      "creates a feature collection object"
    );
    t.ok(geojson.features, "has features");
  });
});

test("it should load a single file with a glob pattern", (t) => {
  t.plan(4);

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {
          path: path.join(__dirname, "../fixtures/points.csv"),
          geometryColumns: {
            longitude: "longitude",
            latitude: "latitude",
          },
          metadata: {
            idField: "id",
          },
        },
      },
    },
  };

  const Model = proxyquire("../../src/model", {
    config,
  });
  const model = new Model();

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.error(err, "no error");
    t.equal(
      geojson.type,
      "FeatureCollection",
      "creates a feature collection object"
    );
    t.equal(geojson.features.length, 2, "only one file is loaded");
    t.ok(geojson.features, "has features");
  });
});

test("it should load multiple files with a glob pattern", (t) => {
  t.plan(4);

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {
          path: path.join(__dirname, "../fixtures/*.csv"),
          geometryColumns: {
            longitude: "longitude",
            latitude: "latitude",
          },
          metadata: {
            idField: "id",
          },
        },
      },
    },
  };

  const Model = proxyquire("../../src/model", {
    config,
  });
  const model = new Model();

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.error(err, "no error");
    t.equal(
      geojson.type,
      "FeatureCollection",
      "creates a feature collection object"
    );
    t.equal(geojson.features.length, 4, "multiple files are loaded");
    t.ok(geojson.features, "has features");
  });
});

test("it should generate custom URLs using the generateUrls function", (t) => {
  t.plan(3);

  const customGenerateUrls = (config) => {
    return config.regions.map(region => `https://example.com/data/${region}`);
  };

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {
          regions: ["Region1", "Region2"],
          geometryColumns: {
            longitude: "longitude",
            latitude: "latitude",
          },
        },
      },
    },
  };

  const csvData = "id,longitude,latitude\n1,-122,37\n2,-121,36";

  // Mock fetch response for generated URLs
  const fetch = fetchMock.sandbox()
    .mock("https://example.com/data/Region1", {
      body: csvData,
      headers: { 'Content-Type': 'text/csv' },
    })
    .mock("https://example.com/data/Region2", {
      body: csvData,
      headers: { 'Content-Type': 'text/csv' },
    });

  const Model = proxyquire("../../src/model", {
    "node-fetch": fetch,
    config,
  });
  const model = new Model();
  model.setGenerateUrls(customGenerateUrls);

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.error(err, "no error");
    t.ok(geojson, "has geojson");
    t.equal(
      geojson.type,
      "FeatureCollection",
      "creates a feature collection object"
    );
  });
});

test("it should validate URLs using isValidUrl function", async (t) => {
  t.plan(2);

  const validUrl = "http://example.com/valid.csv";
  const invalidUrl = "http://example.com/invalid.csv";

  const fetch = fetchMock.sandbox()
    .mock(validUrl, { status: 200, headers: { 'Content-Type': 'text/csv' } }) // Mock valid URL with a 200 response
    .mock(invalidUrl, { status: 404 }); // Mock invalid URL with a 404 response

  const isValid = proxyquire("../../src/model", {
    "node-fetch": fetch,
  }).isValidUrl;

  const validResult = await isValid(validUrl);
  const invalidResult = await isValid(invalidUrl);

  t.ok(validResult, "valid URL is recognized");
  t.notOk(invalidResult, "invalid URL is recognized as such");
});

test("it should apply custom transformation logic in translate", (t) => {
  t.plan(3);

  const config = {
    "koop-provider-csv": {
      name: "csv",
      sources: {
        test: {
          url: "http://example.com/points.csv",
          geometryColumns: {
            longitude: "longitude",
            latitude: "latitude",
          },
        },
      },
    },
  };

  const customTransform = (lat, lon) => [lon + 1, lat + 1]; // Custom transformation logic

  const Model = proxyquire("../../src/model", {
    config,
  });
  const model = new Model();
  model.setTransformCoordinates(customTransform);

  const csvData = "id,longitude,latitude\n1,-122,37\n2,-121,36";
  const readable = new Readable();
  readable.push(csvData);
  readable.push(null);

  const fetch = fetchMock.sandbox().mock("http://example.com/points.csv", readable, { sendAsJson: false });

  model.getData({ params: { id: "test" } }, (err, geojson) => {
    t.error(err, "no error");
    t.ok(geojson, "has geojson");
    t.deepEqual(geojson.features[0].geometry.coordinates, [-121, 38], "coordinates transformed");
  });
});