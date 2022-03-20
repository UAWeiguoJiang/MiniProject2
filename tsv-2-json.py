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
            # if f == '\n' or '': will make null in json into ''
            if f == '':
                d[t] = None
            elif t == 'primaryProfession':
                d[t] = f.split(',')
            elif t == 'knownForTitles':
                d[t] = f.split(',')
            elif t == 'genres':
                d[t] = f.split(',')
            elif t == 'characters':
                l = list()
                for i in f.strip('][').split(','):
                    l.append(i.strip('\"\"'))
                d[t] = l
            else:
                d[t] = f
              
        # we will use strip to remove '\n'.
        arr.append(d)
          
        # we will append all the individual dictionaries into list 
        # and dump into file.
    with open(output_file, 'w', encoding='utf-8') as output_file:
        output_file.write(json.dumps(arr, indent=4))


def main():
    tsv2json('name.basics.tsv', 'name.basics.json')
    tsv2json('title.basics.tsv', 'title.basics.json')
    tsv2json('title.principals.tsv', 'title.principals.json')
    tsv2json('title.ratings.tsv', 'title.ratings.json')


if __name__ == '__main__':
    main()
