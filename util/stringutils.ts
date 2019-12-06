// modules
const uuidv1 = require("uuid/v1");
import '../src/string-ext';

// Wrap this so we can stub it.
export function getUUID(): string {
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
 *       up if > maxLen
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
 * When investigating a selenium test failure on a remote headless browser that couldn't be reproduced
 * locally, I wanted to add some javascript to the site under test that would dump some state to the
 * page (so it could be captured by Selenium as a screenshot when the test failed). JSON.stringify()
 * didn't work because the object declared a toJSON() method, and JSON.stringify() just calls that
 * method if it's present. This was a Moment object, so toJSON() returned a string but I wanted to see
 * the internal state of the object instead.
 *
 * So, this is a rough and ready function that recursively dumps any old javascript object.
 */
export function printObject(o: any, indent: number): string {
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
            printObject(val, indent + 1) +
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

export function compare_numeric_strings(a: string, b: string): number {
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

