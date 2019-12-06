// modules
import '../src/string-ext';

import * as uuid from 'uuid';
const uuidv1 = uuid.v1;

/**
 * Wrap this just so we can stub it in tests.
 *
 * Returns:
 *  String GUID of form uuid/v1 (see uuid npm package)
**/
export function GetHowsMyDrivingId(): string {
  return uuidv1();
}

/*
 * Split array of strings to ensure each string is <= maxLen
 *
 * Params:
 *   source_lines: array of strings (each one may be multi-line)
 *   maxLen:       maximum length for each element in source_lines
 * Returns:
 *   array of strings matching source_lines but with any elements longer
 *   than maxLen, broken up into multiple entries, breaking on in order:
 *   - newlines (trailing newlines on broken elements are removed)
 *   - word breaks
 *   - if neither above exist, then just split at maxLen characters
 *
 * Note: elements in source_lines are not joined if < maxLen, only broken
 *       up if > maxLen. The # of returned strings will be >= source_lines.length
**/
export function SplitLongLines(source_lines: Array<string>, maxLen: number): Array<string> {
  var truncated_lines: Array<string> = [];
  
  var index: number = 0;
  source_lines.forEach(source_line => {
    if (source_line.length > maxLen) {
      // break it up into lines to start with
      var chopped_lines: Array<string> = source_line.split("\n");
      var current_line: string = "";
      var first_line: boolean = true;

      chopped_lines.forEach( (line: string) => {
        if (line.length > maxLen) {
          // OK we have a single line that is too long for a tweet
          if (current_line.length > 0) {
            truncated_lines.push(current_line);
            current_line = '';
            first_line = true;
          }
          
          // word break it into multiple items
          var truncate_index: number = maxLen - 1;

          // Go back until we hit a whitespace characater
          while (truncate_index > 0 && !/\s/.test(line[truncate_index])) {
            truncate_index--;
          }

          if (truncate_index == 0) {
            // The line has no whitespace in it, just chop it in two
            truncate_index = maxLen - 1;
          }

          truncated_lines.push(line.substring(0, truncate_index + 1));

          // The rest of the string may still be too long.
          // Call ourselves recursively to split it up.
          var rest_truncated_lines: Array<string> = SplitLongLines(
            [line.substring(truncate_index + 1)],
            maxLen
          );
          truncated_lines = truncated_lines.concat(rest_truncated_lines);
        } else {
          if (current_line.length + line.length + 1 <= maxLen) {
            if (!first_line) {
              current_line += "\n";
            }
            current_line += line;
            first_line = false;
          } else {
            truncated_lines.push(current_line);
            
            // Start again
            current_line = line;
            first_line = true;
          }
        }
      });

      if (current_line.length > 0) {
        truncated_lines.push(current_line);
      }
    } else {
      truncated_lines.push(source_line);
    }
  });

  return truncated_lines;
}

/**
 * Recursively dumps any javascript object.
 *
 * Params:
 *   o:      Object to dump
 *   Indent: # characters to indent. This allows creating an intented 
 *           tree structure of an obect.
 *
 * Returns:
 *   String representing a dump of o and all it's values.
 */
export function DumpObject(o: any, indent: number = 0): string {
  var out: string = "";
  if (typeof indent === "undefined") {
    indent = 0;
  }
  for (var p in o) {
    if (o.hasOwnProperty(p)) {
      var val: any = o[p];
      out += new Array(4 * indent + 1).join(" ") + p + ": ";
      if (typeof val === "object") {
        if (val instanceof Date) {
          out += 'Date "' + val.toISOString() + '"';
        } else {
          out +=
            "{\n" +
            DumpObject(val, indent + 1) +
            new Array(4 * indent + 1).join(" ") +
            "}";
        }
      } else if (typeof val === "function") {
      } else {
        out += '"' + val + '"';
      }
      out += ",\n";
    }
  }
  return out;
}

/**
 * Compares two strings that represent numbers. Comparing as strings
 * will return inconsistent results if the numbers have different number
 * of digits.
 *
 * Params:
 *   a: fist string
 *   b: second string
 * Returns:
 *   0 -   a and b are numberically equal (may not be string equal though)
 *   < 0 - a < b (a is numerically less than b)
 *   > 0 - a > b (a is numerically greater than b)
**/
export function CompareNumericStrings(a: string, b: string): number {
  if (a.length > b.length) {
    b = b.lpad('0', a.length);
  }
  
  if (b.length > a.length) {
    a = a.lpad('0', b.length);
  }
  
  if (a == b) {
    return 0;
  } else if (a < b) {
    return -1;
  } else {
    return 1;
  }
}

