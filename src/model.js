/*
  model.js

  This file is required. It must export a class with at least one public function called `getData`

  Documentation: http://koopjs.github.io/docs/specs/provider/
*/

const koopConfig = require("config");
const fs = require("fs");
const Papa = require("papaparse");
const fetch = require("node-fetch");
const isUrl = require("is-url-superb");
const { glob } = require("glob");
const translate = require("./utils/translate-csv");

let generateUrls = (config) => [config.url]; // Default pass-through function

function Model(koop) {}

// Public function to return data from the
// Return: GeoJSON FeatureCollection
Model.prototype.getData = async function (req, callback) {
  const config = koopConfig["koop-provider-csv"];
  const sourceId = req.params.id;
  const sourceConfig = config.sources[sourceId];

  const urls = generateUrls(sourceConfig);

  const validUrls = [];
  for (const url of urls){
    if (await isUrl(url)){
      if (await isValidUrl(url)){
        validUrls.push(url);
      }
    }
  };
  const hasValidUrls = validUrls.length > 0;
  const hasPathString =
    typeof sourceConfig.path === "string" && sourceConfig.path !== "";

  if (!hasValidUrls && !hasPathString) {
    console.error(
      new Error(
        'No CSV source specified. Either "url" or "path" must be specified at the source configuration.'
      )
    );
    callback(new Error("Invalid CSV source."));
    return;
  }

  if (hasValidUrls && hasPathString) {
    console.error(
      new Error(
        'Invalid CSV source. Either "url" or "path" must be specified at the source configuration.'
      )
    );
    callback(new Error("Invalid CSV source."));
    return;
  }

  try {
    let csvData;

    if (hasValidUrls) {
      if (validUrls.length === 1){
        csvData = await readFromUrl(validUrls[0]);
      } else {
        csvData = await readFromUrls(validUrls);
      }
    } else {
      csvData = await readFromPath(sourceConfig.path);
    }

    const geojson = translate(csvData, sourceConfig);
    callback(null, geojson);
  } catch (error) {
    console.error(error);
    callback("Unable to read CSV data");
  }
};

async function isValidUrl(url) {
  try {
    const res = await fetch(url, { method: 'HEAD' });
    return res.ok;
  } catch (error) {
    console.error(`URL check failed: ${url}`, error);
    return false;
  }
}

async function readFromUrl(url) {
  let readStream;

  if (isUrl(url)) {
    // this is a network URL
    const res = await fetch(url);
    readStream = res.body;
  } else if (url.toLowerCase().endsWith(".csv") && fs.existsSync(url)) {
    // this is a file path
    readStream = fs.createReadStream(url, "utf8");
  } else {
    throw new Error(`Invalid CSV source url: not a URL or a CSV file path`);
  }

  return paraseData(readStream);
}

async function readFromUrls(urls) {
  const allData = [];
  for (let url of urls){
    const data = await readFromUrl(url);
    allData.push(...data);
  }

  return allData;
}

async function readFromPath(path) {
  const filePaths = await glob(path, {
    nodir: true,
  });

  const sortedPaths = filePaths.sort((a, b) => {
    const birthtimeA = fs.statSync(a).birthtimeMs;
    const birthtimeB = fs.statSync(b).birthtimeMs;
    return birthtimeB - birthtimeA;
  });

  const allData = [];

  for (let path of sortedPaths) {
    const readStream = fs.createReadStream(path);
    const data = await paraseData(readStream);
    allData.push(...data);
  }

  return allData;
}

async function paraseData(readStream) {
  return new Promise((resolve, reject) => {
    Papa.parse(readStream, {
      header: true,
      dynamicTyping: true,
      complete: function (result) {
        if (result.errors.length > 0) {
          callback(reject(new Error(result.errors[0].message)));
        } else {
          resolve(result.data);
        }
      },
      error: reject,
    });
  });
}

module.exports = { Model, setGenerateUrls: (fn) => { generateUrls = fn; } };
