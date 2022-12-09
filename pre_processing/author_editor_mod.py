# Add affilitations to author and country to editor

import csv
import random
import argparse


def author_mod():
    # Define the possible affiliations
    affiliations = ['Imperial College London', 'Princeton University', 'University of Oxford', 'ETH Zurich',
                    'Harvard University', 'Politecnico di Milano', 'Stanford University', 'UCL',
                    'University of Chicago',
                    'National University of Singapore (NUS)', 'EPFL', 'Tsinghua University', 'University of Oxford',
                    'Yale University', 'Massachusetts Institute of Technology', 'Cornell University',
                    'The University of Edinburgh', 'Peking University', 'California Institute of Technology',
                    'University of Pennsylvania', 'University of Cambridge'
                    ]
    # Open input file
    with open(FLAGS.authors_file, 'r') as csvinput:
        # Open output file
        with open('authors_final.csv', 'w') as csvoutput:
            # Define writer and reader
            writer = csv.writer(csvoutput, lineterminator='\n', delimiter=';')
            reader = csv.reader(csvinput)

            # All is a list which will contain all rows
            all = []
            # Read the first line of input file
            row = next(reader)
            # Append the new attribute to the row
            row.append('Affiliation')
            # Append the row to all
            all.append(row)

            # Iterate over the rows
            for row in reader:
                # Take a random element from affiliations list and append it to the current row
                row.append(random.choice(affiliations))
                # Append the row to all rows
                all.append(row)
            # Write all rows at once (can be done since the files are not too big, otherwise RAM issues may occurr
            writer.writerows(all)


def editor_mod():
    # Define the possible nations
    nations = ['United States', "People's Republic of China", 'United Kingdom', 'France',
               'Germany', 'Russia', 'Japan', 'Italy', 'United Arab Emirates', 'Israel', 'South Korea',
               'Canada', 'India', 'Spain', 'Saudi Arabia', 'Australia',
               'Turkey', 'Greece', 'Switzerland', 'Egypt', 'Netherlands', 'Brazil', 'Sweden', 'Belgium', 'Denmark',
               'Mexico', 'Ukraine', 'Austria', 'Norway', 'Qatar'
               ]
    # Open input file
    with open(FLAGS.editors_file, 'r') as csvinput:
        # Open output file
        with open('editors_final.csv', 'w') as csvoutput:
            # Define writer and reader
            writer = csv.writer(csvoutput, lineterminator='\n', delimiter=';')
            reader = csv.reader(csvinput)

            # All is a list which will contain all rows
            all = []
            # Read the first line of input file
            row = next(reader)
            # Append the new attribute to the row
            row.append('Country')
            # Append the row to all
            all.append(row)

            # Iterate over the rows
            for row in reader:
                # Take a random element from affiliations list and append it to the current row
                row.append(random.choice(nations))
                # Append the row to all rows
                all.append(row)
            # Write all rows at once (can be done since the files are not too big, otherwise RAM issues may occurr
            writer.writerows(all)


def main():
    editor_mod()
    author_mod()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--authors_file',
        type=str,
        default='./authors.csv',
        help='Authors.csv file')
    parser.add_argument(
        '--editors_file',
        type=str,
        default='./editors.csv',
        help='Editors.csv file')
    FLAGS, _ = parser.parse_known_args()
    main()
