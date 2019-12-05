const MINIMUM_CITATION_ID = 0,
  noPlateFoundCitationNumber = -1,
  noCitationsFoundCitationNumber = -2;

// Exported functions that unit tests need.
module.exports = {
  GetCitationsByPlate: GetCitationsByPlate,
  ProcessCitationsForRequest: ProcessCitationsForRequest,
  // TODO: Move these to a library that jurisdictions can require.
  CitationIDNoPlateFound: noPlateFoundCitationNumber,
  CitationIDNoCitationsFound: noCitationsFoundCitationNumber
};

const test = require('glenbikes-typescript-test');
//const test = require('bikes-typescript-test');
//import {LogType} from 'glenbikes-typescript-test';
var LogType = test.LogType;
const getLogger = test.getLogger;

// modules
var chokidar = require('chokidar'),
    express = require("express"),
    fs = require("fs"),
    licenseHelper = require("../util/licensehelper"),
    log4js = require('log4js'),
    //logging = require('../util/logging.js'),
    path = require("path"),
    server = require('../server'),
    soap = require("soap"),
    strUtils = require("../util/stringutils.js");

var log = getLogger(LogType.app);

var app = express();

const noCitationsFoundMessage = "No citations found for plate #",
  noValidPlate = "No valid license found. Please use XX:YYYYY where XX is two character state/province abbreviation and YYYYY is plate #",
  parkingAndCameraViolationsText = "Total parking and camera violations for #",
  violationsByYearText = "Violations by year for #",
  violationsByStatusText = "Violations by status for #",
  citationQueryText = "License #__LICENSE__ has been queried __COUNT__ times.";

var url =
  "https://web6.seattle.gov/Courts/ECFPortal/JSONServices/ECFControlsService.asmx?wsdl";

function GetCitationsByPlate(plate, state) {
  var vehicleNumPromises = [];

  return new Promise( (resolve, reject) => {
    var citations = [];
    GetVehicleIDs(plate, state).then(async function(vehicles) {
      // Make the calls to GetCitationsByVehicleNum soap method synchronously
      // Or we could get throttled by the server.
      for (var i = 0; i < vehicles.length; i++) {
        var vehicle = vehicles[i];
        citations.push( await GetCitationsByVehicleNum(vehicle.VehicleNumber) );
      }
      
      // citations is an array of an array of citations, one for each vehicle id
      // collapse them into a hash based on 
      var citationsByCitationID = {};
      citations.forEach( (innerArray) => {
        innerArray.forEach( (citation) => {
          citationsByCitationID[citation.Citation] = citation;
        });
      });

      // Now put the unique citations back to an array
      var allCitations = Object.keys(citationsByCitationID).map(function(v) { return citationsByCitationID[v]; });

      resolve(allCitations);
    });
  });
}

function GetVehicleIDs(plate, state) {
  var args = {
    Plate: plate,
    State: state
  };
  
  return new Promise((resolve, reject) => {
    soap.createClient(url, function(err, client) {
      if (err) {
        throw err;
      }
      
      // GetVehicleByPlate returns all vehicles with plates that
      // start with the specified plate. So we have to filter the
      // results.
      client.GetVehicleByPlate(args, function(err, result) {
        var vehicle_records = [];
        var jsonObj = JSON.parse(result.GetVehicleByPlateResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        for (var i = 0; i < jsonResultSet.length; i++) {
          var vehicle = jsonResultSet[i];
          
          if (vehicle.Plate == plate) {
            vehicle_records.push(vehicle);
          }
        }
        resolve(vehicle_records);
      });
    });
  });
}

function GetCitationsByVehicleNum(vehicleID) {
  var args = {
    VehicleNumber: vehicleID
  };
  
  log.debug(`Getting citations for vehicle ID: ${vehicleID}.`);
  
  return new Promise((resolve, reject) => {
    soap.createClient(url, function(err, client) {
      if (err) {
        throw err;
      }
      client.GetCitationsByVehicleNumber(args, function(err, citations) {
        if (err) {
          throw err;
        }
        var jsonObj = JSON.parse(citations.GetCitationsByVehicleNumberResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        resolve(jsonResultSet);
      });
    });
  });
}

function GetCasesByVehicleNum(vehicleID) {
  var args = {
    VehicleNumber: vehicleID
  };
  return new Promise((resolve, reject) => {
    soap.createClient(url, function(err, client) {
      client.GetCasesByVehicleNumber(args, function(err, cases) {
        var jsonObj = JSON.parse(cases.GetCasesByVehicleNumberResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        resolve(jsonResultSet);
      });
    });
  });
}

// Process citations for one request
function ProcessCitationsForRequest( citations ) {
  var general_summary, detailed_list, temporal_summary;
  var categorizedCitations = {};
  var chronologicalCitations = {};
  var violationsByYear = {};
  var violationsByStatus = {};
  
  if (!citations || Object.keys(citations).length == 0) {
    // Should never happen. jurisdictions must return at least a dummy citation
    throw new Error("Jurisdiction modules must return at least one citation, a dummy one if there are none.");
  } else if (citations.length == 1 && citations[0].Citation < MINIMUM_CITATION_ID) {
    switch ( citations[0].Citation ) {
      case noPlateFoundCitationNumber:
        return Promise.resolve([
          noValidPlate
        ]);
        break;
        
      case noCitationsFoundCitationNumber:
        return new Promise( (resolve, reject) => {
          server._getQueryCount(citations[0].license).then( (query_count) => {
            resolve( [
              `${noCitationsFoundMessage}${licenseHelper.formatPlate(citations[0].license)}` +
              "\n\n" +
              citationQueryText.replace('__LICENSE__', licenseHelper.formatPlate(citations[0].license)).replace('__COUNT__', query_count)
            ]);
          });
        });
        break
        
      default:
        throw new Error(`ERROR: Unexpected citation ID: ${citations[0].Citation}.`);
        break;
    }
  } else {
    var license;
    var query_count;
    
    for (var i = 0; i < citations.length; i++) {
      var citation = citations[i];
      var year = "Unknown";
      var violationDate = new Date(Date.now());

      // All citations are from the same license
      if (license == null) {
        license = citation.license;
      }
      
      try {
        violationDate = new Date(Date.parse(citation.ViolationDate));
      } catch (e) {
        // TODO: refactor error handling to a separate file
        throw new Error(e);
      }

      if (!(violationDate in chronologicalCitations)) {
        chronologicalCitations[violationDate] = new Array();
      }

      chronologicalCitations[violationDate].push(citation);

      if (!(citation.Type in categorizedCitations)) {
        categorizedCitations[citation.Type] = 0;
      }
      categorizedCitations[citation.Type]++;

      if (!(citation.Status in violationsByStatus)) {
        violationsByStatus[citation.Status] = 0;
      }
      violationsByStatus[citation.Status]++;

      year = violationDate.getFullYear();

      if (!(year in violationsByYear)) {
        violationsByYear[year] = 0;
      }

      violationsByYear[year]++;
    }
    
    return new Promise( (resolve, reject) => {
      server._getQueryCount(license).then( (query_count) => {
        var general_summary =
          parkingAndCameraViolationsText +
          licenseHelper.formatPlate(license) +
          ": " +
          Object.keys(citations).length;

        Object.keys(categorizedCitations).forEach( key => {
          var line = key + ": " + categorizedCitations[key];

          // Max twitter username is 15 characters, plus the @
          general_summary += "\n";
          general_summary += line;
        });

        general_summary += "\n\n";
        general_summary += citationQueryText
          .replace('__LICENSE__', licenseHelper.formatPlate(license))
          .replace('__COUNT__', query_count);

        var detailed_list = "";

        var sortedChronoCitationKeys = Object.keys(chronologicalCitations).sort(
          function(a, b) {
            return new Date(a).getTime() - new Date(b).getTime();
          }
        );

        var first = true;

        for (var i = 0; i < sortedChronoCitationKeys.length; i++) {
          var key = sortedChronoCitationKeys[i];

          chronologicalCitations[key].forEach( citation => {
            if (first != true) {
              detailed_list += "\n";
            }
            first = false;
            detailed_list += `${citation.ViolationDate}, ${citation.Type}, ${citation.ViolationLocation}, ${citation.Status}`;
          });
        }

        var temporal_summary = violationsByYearText + licenseHelper.formatPlate(license) + ":";
        Object.keys(violationsByYear).forEach( key => {
          temporal_summary += "\n";
          temporal_summary += `${key}: ${violationsByYear[key]}`;
        });

        var type_summary = violationsByStatusText + licenseHelper.formatPlate(license) + ":";
        Object.keys(violationsByStatus).forEach( key => {
          type_summary += "\n";
          type_summary += `${key}: ${violationsByStatus[key]}`;
        });

        // Return them in the order they should be rendered.
        var result = [
          general_summary,
          detailed_list,
          type_summary,
          temporal_summary
        ];

        resolve(result);
      });
    });
  }
}

// Print out subset of citation object properties.
function printCitation( citation ) {
  return (
    `Citation: ${citation.Citation}, Type: ${citation.Type}, Status: ${citation.Status}, Date: ${citation.ViolationDate}, Location: ${citation.ViolationLocation}.`
  )
}

