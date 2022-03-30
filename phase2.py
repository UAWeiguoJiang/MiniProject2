import pymongo
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
    """
        User interface of document store, allows users to perform various task
        or gracefully exit the program.

        Arguments: None

        Returns: None
    """
    while True:
        print('Main menu:')     # provide options
        print('1: Search for titles')
        print('2: Search for genres')
        print('3: Search for cast/crew members')
        print('4: Add a movie')
        print('5: Add a cast/crew member')
        print('X: exit')
        op = input('You would like to: ')
        if op == '1':
            search_for_titles()
        elif op == '2':
            search_for_genres()
        elif op == '3':
            search_for_members()
        elif op == '4':
            add_a_movie()
        elif op == '5':
            add_a_member()
        elif op.upper() == 'X':     # case insensitive
            return
        else:       # error checking
            print('Invalid option, please try again!')
            print('\n')


def search_for_titles():

    """
    get keywords from user
    display all fields in title_basics with matching keywords
    user can select title to see rating, number of votes, the names of cast/crew members
    and their charcters (if any)
    """    

    # drop all index before start
    # title_basics.drop_index("*") 
    # title_ratings.drop_index("*")
    # title_principals.drop_index("*")
    # name_basics.drop_index("*")  
    
    # title_ratings.create_index([("nconst", pymongo.DESCENDING)])
    # title_basics.create_index([("nconst", pymongo.DESCENDING)])
    # title_principals.create_index([("nconst", pymongo.DESCENDING)])
    # name_basics.create_index([("nconst", pymongo.DESCENDING)])
    
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
    
    # results from primaryTitle field
    results = title_basics.find({"$text": {"$search": keywordsForTitle}})
    results = list(results)

    title_basics.drop_index('primaryTitle_text_startYear_text')     # drop text index
            
    resultString = ''    
    
    if results == []:
        print("No available results, returning to Main Menu.")
        # drop all index at the end
        # title_basics.drop_index("*") 
        # title_ratings.drop_index("*")
        # title_principals.drop_index("*")
        # name_basics.drop_index("*")        
        return
    
    else:
        # print each result
        for i in range(len(results)):
            index = 'Result ' + str(i)
            tconstStr = ''
            titleTypeStr = ''
            primarytitleStr = ''
            originaltitleStr = ''
            isAdultStr = ''
            startYearStr = ''
            endYearStr = ''
            runtimeStr = ''
            genresStr = ''
            
            # make sure every field is not null
            if (results[i]['tconst'] == None):
                tconstStr = 'N/A'
            else:
                tconstStr = results[i]['tconst']
            if (results[i]['titleType'] == None):
                titleTypeStr = 'N/A'
            else:
                titleTypeStr = results[i]['titleType']
            if (results[i]['primaryTitle'] == None):
                primarytitleStr = 'N/A'
            else:
                primarytitleStr = results[i]['primaryTitle']
            if (results[i]['originalTitle'] == None):
                originaltitleStr = 'N/A'
            else:
                originaltitleStr = results[i]['originalTitle']
            if (results[i]['isAdult'] == None):
                isAdultStr = 'N/A'
            else:
                isAdultStr = results[i]['isAdult']
            if (results[i]['startYear'] == None):
                startYearStr = 'N/A'
            else:
                startYearStr = results[i]['startYear']
            if (results[i]['endYear'] == None):
                endYearStr = 'N/A'
            else:
                endYearStr = results[i]['endYear']
            if (results[i]['runtimeMinutes'] == None):
                runtimeStr = 'N/A'
            else:
                runtimeStr = str(results[i]['runtimeMinutes'])
            if (results[i]['genres'] == None):
                genresStr = 'N/A'
            else:
                genresStr = ', '.join(results[i]['genres'])
            
            resultString += (index + '\n'
                             'tconst: ' + tconstStr + '\n'
                             'titleType: ' + titleTypeStr + '\n'
                             'primaryTitle: ' + primarytitleStr + '\n'
                             'originalTitle: ' + originaltitleStr + '\n'
                             'isAdult: ' + isAdultStr + '\n'
                             'startYear: ' + startYearStr + '\n'
                             'endYear: ' + endYearStr + '\n'
                             'runtimeMinutes: ' + runtimeStr + '\n'
                             'genres: ' + genresStr + '\n') + '\n'
    print(resultString)
        
    # prompt user to enter a selection
    while True:
        selection = input("Select a movie from the 'result' field OR any other key to exit: ") 
        
        if selection.isnumeric():
            selection = int(selection)
            if (selection < len(results) and selection >= 0):
                break
            else:
                print("Selection out of bound")
        else:
            print("Back to Main Menu.")
            # drop all index at the end
            # title_basics.drop_index("*") 
            # title_ratings.drop_index("*")
            # title_principals.drop_index("*")
            # name_basics.drop_index("*")            
            return
    
    # find the ratings, and numbVote info
    titleDic = results[selection]
    tconst = titleDic["tconst"]
    ratings = list(title_ratings.find({"tconst": tconst}))
    
    # find names of casts/crew by joining using "$lookup"
    match = {"tconst": tconst}
    stages = [
        {"$match": match},
        {"$lookup": 
         {"from": "name_basics",
         "localField": "nconst",
         "foreignField": "nconst",
         "as": "names"}},
        {"$project": {"tconst":1, "characters":1, "names":1, "job":1}}
    ]
    
    aggregatedResults = list(title_principals.aggregate(stages))
    
    aggregatedResultString = ''
    vote = '0'
    rating = '0.0'
    
    if (ratings != []):
        rating = str(ratings[0]['averageRating'])
        votes = str(ratings[0]['numVotes'])
    
    # setup strings for cast/crew member
    if aggregatedResults == []:
        aggregatedResultString = 'No cast/crew member to display.'
    else:
        for i in range(len(aggregatedResults)):
            characterStr = ''
            jobStr = ''
            primaryNameStr = ''
            # make sure fields are not None
            if(aggregatedResults[i]['job'] == None):
                jobStr = 'N/A'
            else:
                jobStr = aggregatedResults[i]['job']
            if(aggregatedResults[i]['characters'] == None):
                characterStr = 'N/A'
            else:
                characterStr = ', '.join(aggregatedResults[i]['characters'])
            if(aggregatedResults[i]['names'] == None):
                primaryNameStr = 'N/A'
            else:
                if (aggregatedResults[i]['names'][0]['primaryName'] == None):
                    primaryNameStr = 'N/A'
                else:
                    primaryNameStr = aggregatedResults[i]['names'][0]['primaryName']
                    
            crewStr = (primaryNameStr + ' ------ ' + 'job: ' + jobStr + ' || ' + 'characters: ' + characterStr + '\n')
            aggregatedResultString += crewStr
        
        
    print('\n' + 'primaryTitle: ' + titleDic['primaryTitle'])
    print('rating: ' + rating + ' || ' + 'votes: ' + vote)
    print(aggregatedResultString)
    
    # drop all index at the end
    # title_basics.drop_index("*") 
    # title_ratings.drop_index("*")
    # title_principals.drop_index("*")
    # name_basics.drop_index("*")

    return


def search_for_genres():
    '''
    user provides a genre and minimum vote counts
    they will be able see all the title of movie
    under that genre
    '''
    # drop all index before start
    # title_basics.drop_index("*") 
    # title_ratings.drop_index("*")
    # title_principals.drop_index("*")
    # name_basics.drop_index("*")    
    
    # title_ratings.create_index([("numVotes", pymongo.DESCENDING)])
    # title_ratings.create_index([("avgRating", pymongo.DESCENDING)])
    # title_ratings.create_index([("tconst", pymongo.DESCENDING)])
    # title_basics.create_index([("tconst", pymongo.DESCENDING)])
    # title_basics.create_index([("genres", pymongo.DESCENDING)])
    
    # create text index for genres
    title_basics.create_index([("genres", "text")])
    
    # get genre
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
        {"$project": {"tconst":1, 
                      "ratings.averageRating":1, 
                      "ratings.numVotes":1, 
                      "genres":1, "primaryTitle":1, "originalTitle":1}}
    ]
    
    results = list(title_basics.aggregate(stages))
    
    if results == []:
        print("No available results, going back to Main Menu")
        # drop all index at the end
        # title_basics.drop_index("*") 
        # title_ratings.drop_index("*")
        # title_principals.drop_index("*")
        # name_basics.drop_index("*")
        title_basics.drop_index('genres_text')      # drop text index        
        return
    
    displayString = '\n'

    for i in range(len(results)):
        primaryTitle = ''
        originalTitle = ''
        rating = ''
        vote = ''
        genres = ', '.join(results[i]['genres'])
        
        if primaryTitle == None:
            primaryTitle = 'N/A'
        else:
            primaryTitle = results[i]["primaryTitle"]
        if originalTitle == None:
            originalTitle = 'N/A'
        else:
            originalTitle = results[i]["originalTitle"]

        if results[i]['ratings'] == []:
            rating = 'N/A'
            vote = 'N/A'
        else:
            rating = results[i]['ratings'][0]['averageRating']
            vote = results[i]['ratings'][0]['numVotes']
            
        string = ('primaryTitle: ' + primaryTitle + '\n'
                  'originalTitle: ' + originalTitle + '\n' +
                  'genres: ' + genres + '\n' +
                  'rating: ' + str(rating) + '\n'
                  'votes: ' + str(vote) + '\n') + '\n'
        displayString += string
    
    print(displayString)
    
    # drop all index at the end
    # title_basics.drop_index("*") 
    # title_ratings.drop_index("*")
    # title_principals.drop_index("*")
    # name_basics.drop_index("*")
    title_basics.drop_index('genres_text')      # drop text index

    return


def search_for_members():
    """
        Given a cast/crew member name and see all professions of the member and for each title
        the member had a job, the primary title, the job and character (if any). Matching of
        the member name is case-insensitive.

        Arguments: None

        Returns: None
    """
    name = input('Cast/crew member name: ')     # ask for cast/crew member name
    name = '^' + name + '$'     # regex exact match

    c = name_basics.aggregate([
            {'$match': {'primaryName': {'$regex': name, '$options': 'i'}}},     # case insensitive match

            {
                '$lookup': {    # join with title_principals to find all movies in which the member participates
                    'from': 'title_principals',
                    'localField': 'nconst',
                    'foreignField': 'nconst',
                    'as': 'principals'
                }
            },

            {
                '$unwind': '$principals'    # unwind matches into separate dictionaries
            },

            {
                '$project': {       # project necessary fields and flatten out dictionary
                    'primaryName': 1,
                    'primaryProfession': 1,
                    'nconst': '$principals.nconst',
                    'tconst': '$principals.tconst',
                    'job': '$principals.job',
                    'characters': '$principals.characters'
                }
            },

            {
                '$lookup': {        # join with title_basics to find movies' primary names
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
                '$project': {       # project necessary fields
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
                '$group': {     # group movies into the corresponding crew/cast
                    '_id': '$nconst',
                    'primaryName': {'$last': '$primaryName'},
                    'primaryProfession': {'$last': '$primaryProfession'},
                    'movies': {'$push': {'$cond': [
                                    {'$or':
                                        [      # check whether both job and characters are None
                                            {'$ne': ['$characters', None]},
                                            {'$ne': ['$job', None]}
                                        ]
                                    },

                                    {
                                        'tconst': '$tconst',    # field projections
                                        'primaryTitle': '$primaryTitle',
                                        'job': '$job',
                                        'characters': '$characters'
                                    },

                                    '$$REMOVE'  # if both are None, we do not include the corresponding movie
                            ]
                        }
                    }
                }
            },

            # {
            #     '$unwind': {    # unwind movies list, preserve empty list
            #         'path': '$movies',
            #         'preserveNullAndEmptyArrays': True
            #     }
            # },

            # {
            #     '$project': {       # project necessary fields
            #         '_id': 0,
            #         'nconst': '$_id',
            #         'primaryName': 1,
            #         'primaryProfession': 1,
            #         'tconst': '$movies.tconst',
            #         'primaryTitle': '$movies.primaryTitle',
            #         'job': '$movies.job',
            #         'characters': '$movies.characters'
            #     }
            # }
        ]
    )

    r = [i for i in c]      # retrieve all results and store it in a list
    print('\n')
    if r == []:     # account for cases where no result is returned
        print('No matching cast/crew member.')
        print('\n') 
    else:       # output formatting
        cnt1 = 0
        for i in r:
            # print('Result {0}'.format(cnt))
            print('PERSON #{0}:'.format(cnt1))
            print('-'*80)
            cnt1 += 1

            print('nconst: {0}'.format(i['_id']))
            print('name: {0}'.format(i['primaryName'].title()))
            if i['primaryProfession'] == None:
                print('primary profession: None')
            else:
                print('primary profession: {0}'.format(', '.join(i['primaryProfession'])))

            print('\n')

            if i['movies'] == []:
                print('No movie in which the member had a job or played a character in.')
            else:
                num = len(i['movies'])
                cnt2 = 0
                for j in i['movies']:
                    print('Movie #{0}'.format(cnt2))
                    print('tconst: {0}'.format(j['tconst']))
                    if j['primaryTitle'] == None:
                        print('primary title: None')
                    else:
                        print('primary title: {0}'.format(j['primaryTitle']))
                    if j['job'] == None:
                        print('job: None')
                    else:
                        print('job: {0}'.format(j['job']))
                    if j['characters'] == None:
                        print('characters: None')
                    else:
                        print('characters: {0}'.format(', '.join(j['characters'])))

                    if cnt2 != num - 1:
                        print('\n')
                    cnt2 += 1
            print('-'*80)
            print('\n')

    return


def add_a_movie():
    while(True):
        uniqueID = input("Please input a unique ID: ")
        #query to make sure unique
        query = title_basics.find({"tconst": uniqueID})
        if(len(list(query)))!=0:
            print("Error: Not uniqueID. Enter in something different please. :)")
        else:
            break 


    title = input("Please input a title: ")
    #don't think any conditions are needed for title

    while(True):
        startYear = input("Please input in a start year: ")
        if(startYear.isnumeric()):
            break
        else:
            print("Error: Year not numeric, consider trying a number.")


 
    while(True):
        runtime = input("Please input in a runtime (In Minutes!): ")
        if(runtime.isnumeric()):
            break
        else:
            print("Error: Runtime not numeric, consider trying a number.")
    
    genres = input("Please enter in a list of genres. (Seperate different genres by comma, no spaces between commas.): ")
    listGenres = genres.split(",") #don't think its actually stored as list?

    #and isAdult and endYear are set to Null (denoted as \N).
    #add row to title_basics
    d = {"tconst": uniqueID, "titleType": "movie", "primaryTitle": title, "originalTitle": title, "isAdult": None, "startYear": startYear, "endYear": None, "runtimeMinutes": runtime, "genres": listGenres}
    x = title_basics.insert_one(d)
    #print(x)

    return


def add_a_member():
    while(True):
        existingCast = input("Please input the existing ID of a cast or crew member: ")
        #query to make sure unique
        query = name_basics.find({"nconst": existingCast})
        if(len(list(query)))==0:
            print("Error: Cast member does not exist. Please try again. :)")
        else:
            break 

    while(True):
        existingTitle = input("Please input the existing titleID of a movie: ")
        #query to make sure unique
        query = title_basics.find({"tconst": existingTitle})
        if(len(list(query)))==0:
            print("Error:  TitleID does not exist. Please try again. :)")
        else:
            break 

    category = input("Please enter in a movie category: ")
    ordering = 0 #make it so that its the max ordering listed for the title plus one

    doc = title_principals.find({"tconst": existingTitle}).sort("ordering", -1).limit(1)
    
    for x in doc:
        ordering = (x["ordering"] + 1)
    #print(ordering)
    if(ordering == 0):
        print("Title not detected in title_principals, ordering shall be 1...")
        ordering = 1
        
    d = {"tconst": existingTitle, "ordering": ordering, "nconst": existingCast, "category": category, "job": None, "characters": None}
    x = title_principals.insert_one(d)
    #print(x)
    #print("If it made it this far without crashing then :)))")

    return


def main():
    main_menu()

    return


if __name__ == '__main__':
    main()
