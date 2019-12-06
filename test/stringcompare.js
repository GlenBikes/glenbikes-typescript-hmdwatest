var assert = require('assert'),
    log4js = require('log4js'),
    sinon = require('sinon');

var utils = require('glenbikes-typescript-test');

var log = utils.getLogger(utils.LogType.test);

describe('Numeric string compare', function() {
  describe('Test strings with a < b', function() {
    it('return < 0', () => {
      assert(utils.CompareNumericStrings('1196720038423617536', '1196727525382049792') < 0);
    });
  });
  
  describe('Test strings with a < b, a shorter', function() {
    it('return < 0', () => {
      assert(utils.CompareNumericStrings('99', '1196727525382049792') < 0);
    });
  });
  
  describe('Test strings with a > b, a shorter', function() {
    it('return > 0', () => {
      assert(utils.CompareNumericStrings('1196727525382049792', '99') > 0);
    });
  });
  
  describe('Test strings with a > b, a shorter', function() {
    it('return > 0', () => {
      assert(utils.CompareNumericStrings('1196727525382049792', '1196720038423617536') > 0);
    });
  });
  
  describe('Test strings with a == b', function() {
    it('return == 0', () => {
      assert(utils.CompareNumericStrings('1196727525382049792', '1196727525382049792') == 0);
    });
  });
  
  describe('Test two empty strings', function() {
    it('return == 0', () => {
      assert(utils.CompareNumericStrings('', '') == 0);
    });
  });
  
  describe('Test empty string < number', function() {
    it('return < 0', () => {
      assert(utils.CompareNumericStrings('', '1') < 0);
    });
  });
  
  describe('Test number > empty string', function() {
    it('return > 0', () => {
      assert(utils.CompareNumericStrings('1', '') > 0);
    });
  });
});

