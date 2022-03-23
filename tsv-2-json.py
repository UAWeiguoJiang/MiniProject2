import json

def tsv2json(input_file,output_file):
    arr = []
    file = open(input_file, 'r')
    a = file.readline()
    # The first line consist of headings of the record 
    # so we will store it in an array and move to 
    # next line in input_file.
    titles = [t.strip() for t in a.split('\t')]
    for line in file:
        d = {}
        for t, f in zip(titles, line.split('\t')):
            # Convert each row into dictionary with keys as titles
            f = f.strip()
            f = f.strip('\\N')
            # if f == '\n' or '': will make null in json into '' / empty string
            if f == '':
                d[t] = None
            elif t == 'primaryProfession' or t == 'knownForTitles' or t == 'genres':
                d[t] = f.split(',')
            elif t == 'characters':
                l = list()
                for i in f.strip('][').split(','):
                    l.append(i.strip('\"\"'))
                d[t] = l
            elif t == 'runtimeMinutes' or t == 'ordering' or t == 'numVotes':
                d[t] = int(f)
            elif t == 'averageRating':
                d[t] = float(f)
            else:
                d[t] = f

        arr.append(d)
    
    with open(output_file, 'w', encoding='utf-8') as op:
        op.write(json.dumps(arr, indent=4))
              
        


def main():
    tsv2json('name.basics.tsv', 'name.basics.json')
    tsv2json('title.basics.tsv', 'title.basics.json')
    tsv2json('title.principals.tsv', 'title.principals.json')
    tsv2json('title.ratings.tsv', 'title.ratings.json')


if __name__ == '__main__':
    main()