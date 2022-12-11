# The other fields can be recognised because in the form \n\ni\n\nTITLE OF PARAGRAPH\n\n where i is a number

import json
import os
import re
import argparse


def paragraph_finder(subsection):
    return re.split('\\.\\n', subsection)


def find_subsections(section):
    return re.split('\\n\d\\.\d\\n\\n', section)


def figure_finder(section):
    # Find the figures inside the section
    figures = re.split('Figure \\d:', section)
    # Generate the list figures
    img_list = []
    for img in figures:
        # Generate the dictionary of the current figure
        img_dict = {"Caption": '', "URL": 'figure_' + str(figures.index(img) + 1) + '.com'}
        # For each img take the caption by splitting on .
        caption = img.partition(r'.')[0]
        if len(caption) > 300:
            caption = caption[:300]
        img_dict['Caption'] = caption
        img_list.append(img_dict)
    return img_list


def section_splitter(field, outfile):
    # Create sections dictionary
    sections_dict = {"Sections": []}
    # Generate the list to store the section dictionary
    sections_list = []
    # First delete abstract and authors
    # split = re.split('\\n\\nAbstract', field, 2)
    # Then avoid everything after abstract up to .\n\n
    # txt = re.split('\\.', split[1], 2)
    # print(txt[1])
    # Process the PaperText. Find section by splitting on \n\n number \n\n. Then check if titles and text
    # actually match
    sections = re.split('\\n\\n\d+\\n\\n', field)
    i = 1
    print(len(sections))
    for section in sections[1:]:
        # Split the section to find title and text
        tmp = section.partition('\n\n')
        title = tmp[0]
        if len(tmp[0]) > 100:
            title = title[:100]
        # If title does not start with a letter skip it
        if bool(re.match('[a-zA-Z]', title.strip())):
            # Generate the dictionary for the current section adding Title, Subsections and Figures keys
            section_dict = {'Title': title, 'Subsections': [], 'Figures': []}
            # Find img_list and append it to section_dict
            img_list = figure_finder(tmp[2])
            section_dict['Figures'] = img_list
            # Find subsections
            subsections = find_subsections(tmp[2])
            # Generate the list to store subsection dictionary
            subsections_list = []
            for subsection in subsections:
                # Generate the dictionary for the current subsection
                subsection_dict = {'Title': 'Subsection ' + str(subsections.index(subsection) + 1),
                                   'Paragraphs': []}
                # Find the paragraphs
                paragraphs = paragraph_finder(subsection)
                # Generate the list that will contain the paragraphs
                paragraphs_list = []
                for paragraph in paragraphs:
                    # Generate the dictionary that will contain the text
                    paragraph_dict = {'Title': 'Paragraph ' + str(paragraphs.index(paragraph) + 1), 'Text': paragraph}
                    # Now add this new dictionary to paragraph_list
                    paragraphs_list.append(paragraph_dict)
                # Now that paragraphs_list contains all paragraphs add it to current subsection_dict
                subsection_dict['Paragraphs'].extend(paragraphs_list)
                # Now that subsection dict is complete save it in subsections_list
                subsections_list.append(subsection_dict)
            # Now that subsections_list contains alla paragraphs + all subsections save it in section_dict
            section_dict['Subsections'] = subsections_list
            # Since the dictionary of the single section is complete add it to the sections list
            sections_list.append(section_dict)
    return sections_list


def main():
    # First read the json file and make a dictionary
    # Open the file
    with open(FLAGS.in_file, 'r') as json_file:
        # Populate a dictionary from the json. Practically the dictionaries are actually two: the first contains all
        # articles.json, the second the fields of every article
        data = json.load(json_file)
        # Open the output file
        with open(FLAGS.out_file, 'w') as outfile:
            # File_array is a sequence of dictionaries
            doc_list = []
            # For each article in data generate its dictionary
            for article in data:
                # Iterate over the field of the single article to generate the article
                article_dict = {}
                for field in article:
                    if field == 'PaperText':
                        sections_list = section_splitter(article[field], outfile)
                        article_dict['Sections'] = sections_list
                    elif field == 'Journal':
                        tmp_dict = {}
                        for key in article[field]:
                            if not key == 'volume':
                                tmp_dict[str(key)] = article[field].get(key)
                            else:
                                integer = int(article[field].get(key))
                                tmp_dict[str(key)] = integer
                        article_dict[str(field)] = tmp_dict
                    elif field == "keywords":
                        key_list = []
                        for key in article[field].keys():
                            # save the keywords name in a new array
                            key_list.append(key)
                            article_dict["keywords"] = key_list
                    else:
                        article_dict[str(field)] = article[field]
                # Add references field
                article_dict['References'] = []
                # Now article_dict contains all field and can be put inside doc_list
                doc_list.append(article_dict)
            json.dump(doc_list, outfile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--in_file',
        type=str,
        default='./input_data/datasetv3.json',
        help='Input .json file.')
    parser.add_argument(
        '--out_file',
        type=str,
        default='./output_data/final_dataset.json',
        help='Output .json file.')
    FLAGS, _ = parser.parse_known_args()
    main()
