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
    pass


def search_for_genres():
    pass


def search_for_members():
    pass


def add_a_movie():
    pass


def add_a_member():
    pass


def main():
    main_menu()


if __name__ == '__main__':
    main()
