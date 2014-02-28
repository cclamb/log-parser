#!/usr/bin/env python3.3
"""
This is a simple example of how to extract data from log files and analyze data contained therein.
Here, we read data from logs generated via slf4j.  The metrics are saved using a logging context which has
the string '::metrics'.  All data associated with this string contains JSON tuples of the form
[<name:string> : <value:int>].  We read this data, convert the JSON to python data structures, and run
a very simple analysis routine over the data (i.e. we calculate the mean).
"""
import re
import json

__author__ = 'cclamb'


file_names = ['logs/fear-ingestor.log', 'logs/solr-ejector.log']


def matcher(line):
    """
     This will match a line from the log file if it contains metric information.
    """
    if re.search('::metric', line) is not None:
        return True
    else:
        return False


def match_file(file_names, matcher):
    """
    Using a list of file names and a matcher function, this function will extract
    lines from the log file that are of a specific format for later processing.
    """
    buffers = {}
    for name in file_names:
        file = open(name)
        try:
            lines = file.readlines()
            buffer = []
            for line in lines:
                if matcher(line):
                    buffer.append(line)
            buffers[name] = buffer
        finally:
            file.close()
    return buffers


def extract_data(line):
    """
    Here, we split the data containing lines into a header and data section.  Slf4j inserts log messages after
    the string ' - ', so we use that as the delimiter.  Everything to the right of that token is valid JSON.
    """
    lines = line.split(' - ')
    return json.loads(lines[1])


def sort_data(data):
    """
    Converting a group of JSON-style data structures (dictionaries) to lists of numbers sorted by a header.
    """
    sorted_data = {}
    for datum in data:
        for key in datum:
            collection = sorted_data.get(key, [])
            collection.append(int(datum[key]))
            sorted_data[key] = collection
    return sorted_data


def generate_mean(list):
    """
    Generating a mean from a list.
    """
    sum = 0
    for i in list:
        sum += i
    return sum / len(list)


def generate_statistics(data):
    """
    This function processes formatted data, extracting statistics, and then printing the information
    to stdout.
    """
    print(data)
    for key in data:
        print('****\nSummary data for %s:\n----' % key)
        for category in data[key]:
            mean = generate_mean(data[key][category])
            print('\taverage %s: %d' % (category, mean))
        print('\n\n')
    return


def run_main():
    """
    The program main entry point.
    """
    # Matching lines against a matcher function.
    matched_lines = match_file(file_names, matcher)

    # Will contain data sorted by file.
    binned_data = {}

    # Looking through the lines that were inserted into the metrics file via the metrics component.
    for key in matched_lines:

        # Grabbing matched lines by the file or orgination.
        buffer = matched_lines[key]

        # This will contain dictionaries converted from JSON.
        data = []

        # Loop through the collection, appending data converted from JSON entries.
        for line in buffer:
            data.append(extract_data(line))

        # Sort the data by file.
        binned_data[key] = sort_data(data)

    # Output the final results.
    generate_statistics(binned_data)
    return 0


if __name__ == '__main__':
    exit(run_main());