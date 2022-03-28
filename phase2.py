from pprint import pprint
from pymongo import MongoClient

port_number = int(input('Server port number: '))
client = MongoClient("localhost", port_number)

db = client['291db']

name_basics = db['name_basics']
title_basics = db['title_basics']
title_principals = db['title_principals']
title_ratings = db['title_ratings']

def main_menu():
    while True:
        print('Main menu:')
        print('1: Search for titles')
        print('2: Search for genres')
        print('3: Search for cast/crew members')
        print('4: Add a movie')
        print('5: Add a cast/crew member')
        print('X: exit')
        op = input('You would like to: ')
        if op == '1':
            search_for_movies()
        elif op == '2':
            search_for_genres()
        elif op == '3':
            search_for_members()
        elif op == '4':
            add_a_movie()
        elif op == '5':
            add_a_member()
        elif op.upper() == 'X':
            return
        else:
            print('Invalid option, please try again!')


def search_for_movies():

    """
    get keywords from user
    display all fields in title_basics with matching keywords
    user can select title to see rating, number of votes, the names of cast/crew members
    and their charcters (if any)
    """    

    # create indexes
    title_ratings.create_index([("nconst", pymongo.DESCENDING)])
    title_basics.create_index([("nconst", pymongo.DESCENDING)])
    title_principals.create_index([("nconst", pymongo.DESCENDING)])
    name_basics.create_index([("nconst", pymongo.DESCENDING)])
    
    keywords = ''
    keywordsList = []
    while (keywordsList == []):
        keywords = input("Enter keywords: ").strip()
        keywordsList = keywords.lower().split()        
    
    # turn keywords into specific string for full text search
    for i in range(len(keywordsList)):
        keywordsList[i] = '\"' + keywordsList[i] + '\"'
    
    keywordsForTitle = ' '.join(keywordsList)
    
    # create text index for primaryTitle field, startYear
    title_basics.create_index([('primaryTitle', 'text'), ('startYear', 'text')])
    
    #results from primaryTitle field
    results = title_basics.find({"$text": {"$search": keywordsForTitle}})
    results = list(results)
            
    if results == []:
        print("No available results, returning to Main Menu.")
        # drop all index at the end
        title_basics.drop_index("*") 
        title_ratings.drop_index("*")
        title_principals.drop_index("*")
        name_basics.drop_index("*")        
        return
    else:
        # print each result
        for i in range(len(results)):
            print(str(i)+":", results[i])    
    
    # prompt user to enter a selection
    while True:
        selection = input("Select a movie from above OR any other key to exit: ") 
        
        if selection.isnumeric():
            selection = int(selection)
            if (selection < len(results) and selection >= 0):
                break
            else:
                print("Selection out of bound")
        else:
            print("Back to Main Menu.")
            # drop all index at the end
            title_basics.drop_index("*") 
            title_ratings.drop_index("*")
            title_principals.drop_index("*")
            name_basics.drop_index("*")            
            return
    
    # find the ratings, and numbVote info
    titleDic = results[selection]
    tconst = titleDic["tconst"]
    ratings = list(title_ratings.find({"tconst": tconst}))
    
    # find names of casts/crew by joing using "$lookup"
    match = {"tconst": tconst}
    stages = [
        {"$match": match},
        {"$lookup": 
         {"from": "name_basics",
         "localField": "nconst",
         "foreignField": "nconst",
         "as": "names"}},
        {"$project": {"tconst":1, "characters":1, "names":1}}
    ]
    
    aggregatedResults = list(title_principals.aggregate(stages))
    
#    print(ratings)
#    for result in aggregatedResults:
#        print(result)    
    
    # setup strings for rating and number of votes
    rating = 'RATING: '
    numVotes = 'NUMBER OF VOTES: '
    rating += str(ratings[0]["averageRating"])
    numVotes += str(ratings[0]["numVotes"])
    
    # setup strings for characters and casts/crew members
    pairs = ''
    
    for i in range(len(aggregatedResults)):
        char = aggregatedResults[i]['characters']
        name = aggregatedResults[i]['names']
        if char != None:
            char = '[' + ', '.join(char) + ']'
        else:
            char = 'N/A' + ' (crew)'
        if name != None:
            crew = name[0]['primaryName']
        pairs += chr(9) + crew + ': ' + char + '\n'
    
    print("For selection: ", '\n', titleDic)
    print(rating)
    print(numVotes)
    print('CASTS/CREW MEMBERS: [CHARACTERS]')
    if (pairs == ''):
        print("NO CAST/CREW MEMBERS")
    else:
        print(pairs)
    
    # drop all index at the end
    title_basics.drop_index("*") 
    title_ratings.drop_index("*")
    title_principals.drop_index("*")
    name_basics.drop_index("*")
    return


def search_for_genres():
    '''
    user provides a genre and minimum vote counts
    they will be able see all the title of movie
    under that genre
    '''
    # create indexes
    title_ratings.create_index([("numVotes", pymongo.DESCENDING)])
    title_ratings.create_index([("avgRating", pymongo.DESCENDING)])
    title_ratings.create_index([("tconst", pymongo.DESCENDING)])
    title_basics.create_index([("tconst", pymongo.DESCENDING)])
    title_basics.create_index([("genres", pymongo.DESCENDING)])
    
    # create text index for genres
    title_basics.create_index([("genres", "text")])
    
    # get gere
    genre = ''
    while genre == '':
        genre = input('Enter a genre: ')
    # get votes, make sure it is >= 0 and isdigit
    vote = -1
    while vote < 0:
        num = input('Enter a vote number: ')
        if (num.isdigit()):
            if(int(num) >= 0):
                vote = int(num)
        else:
            print('Please enter numeric values that is greater or equal to 0.')
    
    # text search genres
    # join collection with title_ratings
    # capture data that is greater or equal to numVote
    match = {"$text": {"$search": genre}}
    stages = [
        {"$match": match},
        {"$lookup":
         {"from": "title_ratings",
          "localField": "tconst",
          "foreignField": "tconst",
          "as": "ratings"}},
        {"$sort": {"ratings.averageRating": -1}},
        {"$match": {"ratings.numVotes": {"$gte": vote}}},        
        {"$project": {"tconst":1, "ratings.averageRating":1, "ratings.numVotes":1, "genres":1, "primaryTitle":1, "originalTitle":1}}
    ]
    
    results = list(title_basics.aggregate(stages))
    
    if results == []:
        print("No available results, going back to Main Menu")
        # drop all index at the end
        title_basics.drop_index("*") 
        title_ratings.drop_index("*")
        title_principals.drop_index("*")
        name_basics.drop_index("*")        
        return;
    
    displayString = ''

    for i in range(len(results)):
        primaryTitle = results[i]["primaryTitle"]
        originalTitle = results[i]["originalTitle"]
        pair = ''
        if primaryTitle != None:
            pair += "PRIMARY TITLE: " + '"' + primaryTitle + '"' + '\n'
        else:
            pair += "PRIMARY TITLE: " + 'N/A' + '\n'
        if originalTitle != None:
            pair += chr(9) + "ORIGINAL TITLE: " + '"' + originalTitle + '"'
        else:
            pair += chr(9) + 'ORIGINAL TITLE: ' + "N/A"
        displayString += pair + '\n'
    
    print("--------TITLES--------")
    print(displayString)
    
    # drop all index at the end
    title_basics.drop_index("*") 
    title_ratings.drop_index("*")
    title_principals.drop_index("*")
    name_basics.drop_index("*")
    return


def search_for_members():
    name = input('Cast/crew member name: ')
    c = name_basics.aggregate([
            {
                '$project': {
                    'nconst': 1,
                    'primaryName': {'$toUpper': '$primaryName'},
                    'primaryProfession': 1,
                    'knownForTitles': 1
                }
            },

            {
                '$match': {
                    'primaryName': name.upper()
                }
            },

            {
                '$lookup': {
                    'from': 'title_principals',
                    'localField': 'nconst',
                    'foreignField': 'nconst',
                    'as': 'principals'
                }
            },

            {
                '$unwind': '$principals'
            },

            {
                '$project': {
                    'primaryName': 1,
                    'primaryProfession': 1,
                    'nconst': '$principals.nconst',
                    'tconst': '$principals.tconst',
                    'job': '$principals.job',
                    'characters': '$principals.characters'
                }
            },

            {
                '$lookup': {
                    'from': 'title_basics',
                    'localField': 'tconst',
                    'foreignField': 'tconst',
                    'as': 'basics'
                }
            },

            {
                '$unwind': '$basics'
            },

            {
                '$project': {
                    '_id': 0,
                    'primaryName': 1,
                    'primaryProfession': 1,
                    'nconst': 1,
                    'tconst': 1,
                    'job': 1,
                    'characters': 1,
                    'primaryTitle': '$basics.primaryTitle'
                }
            },

            {
                '$match': {
                    '$or': [
                        {
                            'job': {'$ne': None}
                        },

                        {
                            'characters': {'$ne': None}
                        }
                    ]
                }
            }
        ]
    )

    r = [i for i in c]
    if r == []:
        print('No matching cast/crew member.')
    else:
        cnt = 0
        for i in r:
            pprint(i)
            print('\n')
            cnt += 1
        print(cnt)


def add_a_movie():
    pass


def add_a_member():
    pass


def main():
    main_menu()


if __name__ == '__main__':
    main()
