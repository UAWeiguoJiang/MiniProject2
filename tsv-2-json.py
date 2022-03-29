import json

# function adapted from https://www.geeksforgeeks.org/python-tsv-conversion-to-json/
def tsv2json(input_file,output_file):
    """
        Convert tsv files to json files.

        Arguments:
            input_file: path to the tsv file
            output_file: path to the output json file

        Returns: None
    """
    arr = []    # create a list for storing all dictionaries

    file = open(input_file, 'r', encoding='utf-8')  # open file and retrieve field names
    a = file.readline()
    titles = [t.strip() for t in a.split('\t')]

    for line in file:
        d = {}
        for t, f in zip(titles, line.split('\t')):  # pair up field names and values

            f = f.strip()   # strip extra spaces and newlines
            f = f.rstrip('\\N')

            if f == '':     # if empty string, convert to None / null in json
                d[t] = None
            elif t == 'primaryProfession' or t == 'knownForTitles' or t == 'genres':    # convert these fields to nested arrays
                d[t] = f.split(',')
            elif t == 'characters':     # convert to nested arrays with extra string manipulation
                l = list()
                for i in f.strip('][').split(','):
                    l.append(i.strip('\"\"'))
                d[t] = l
            elif t == 'runtimeMinutes' or t == 'ordering' or t == 'numVotes':   # convert these fields to integers
                d[t] = int(f)
            elif t == 'averageRating':      # convert this field to floats
                d[t] = float(f)
            else:
                d[t] = f

        arr.append(d)   # add dictionary to list
    
    with open(output_file, 'w', encoding='utf-8') as op:    # dump to json file
        op.write(json.dumps(arr, indent=4))

    return
              
        


def main():
    tsv2json('name.basics.tsv', 'name.basics.json')
    tsv2json('title.basics.tsv', 'title.basics.json')
    tsv2json('title.principals.tsv', 'title.principals.json')
    tsv2json('title.ratings.tsv', 'title.ratings.json')

    return


if __name__ == '__main__':
    main()
