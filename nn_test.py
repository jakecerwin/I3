import requests
import json
import csv
import pandas as pd

imdb_data = dict()
with open("IMDb_movies.csv") as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)
    for row in reader:
        imdb_data.update({row[0]: (row[19], row[9])})

print('gathering viewed data')

views = dict()
with open("views3.csv") as csv_views:
    reader = csv.reader(csv_views)
    for row in reader:
        views.update({row[0]: row[1]})


print('creating d frame')

def create_new_dframe(src):
    f = open(src, "r")
    fl = f.readlines()
    f.close()


    fieldnames = ['metascore', 'top_100_director', 'in_a_collection', 'Action', 'Adventure', 'Animation',
                      'Comedy', 'Crime', 'Documentary', 'Drama', 'Family', 'Fantasy',
                      'History', 'Horror', 'Music', 'Mystery', 'Romance',
                      'Science Fiction', 'Thriller', 'War', 'Western', 'other genre',
                      'production_cos', 'en', 'es', 'fr', 'it', 'de', 'other_spk_lng', 'profit', 'budget', 'revenue',
                      'popularity', 'runtime', 'age', 'vote_count', 'vote_average','views']

    i = 0
    with open('movie_data.csv', mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for movie in fl:
            i+=1
            if i % 1000 == 0:
                print(i)

            api = dict()
            url = 'http://128.2.204.215:8080/movie/' + movie[:-1]
            try:
                r = requests.get(url)
                query = r.json()
                imdb = query['imdb_id']
            except (json.decoder.JSONDecodeError, KeyError):
                print('json error', movie)
                continue

            if imdb in imdb_data:
                metascore, director = imdb_data[imdb]
            else:
                metascore = 40.0
                director = ''

            if metascore == '':
                metascore = 40.0
            else:
                metascore = float(metascore)
            api['metascore'] = metascore

            # list of top 100 actors according to imdb
            top100 = set([
                            'Stanley Kubrick', 'Ingmar Bergman', 'Alfred Hitchcock',
                            'Akira Kurosawa', 'Orson Welles', 'Federico Fellini',
                            'John Ford', 'Jean-Luc Godard', 'Luis Buñuel',
                            'Martin Scorsese', 'Robert Bresson', 'Charles Chaplin',
                            'Jean Renoir', 'Howard Hawks', 'Steven Spielberg',
                            'Michelangelo Antonioni', 'Andrei Tarkovsky', 'David Lynch',
                            'Yasujirô Ozu', 'Billy Wilder', 'Fritz Lang',
                            'Carl Theodor Dreyer', 'Francis Ford Coppola', 'F.W. Murnau',
                            'Terrence Malick', 'Sergei M. Eisenstein', 'David Lean',
                            'Michael Powell', 'François Truffaut', 'Kenji Mizoguchi',
                            'Woody Allen', 'Robert Altman', 'Vittorio De Sica',
                            'Satyajit Ray', 'Sidney Lumet', 'Roman Polanski',
                            'Roberto Rossellini', 'Luchino Visconti', 'John Cassavetes',
                            'Sergio Leone', ' D.W. Griffith', ' Buster Keaton',
                            'Werner Herzog', 'Krzysztof Kieslowski', 'Abbas Kiarostami',
                            'Béla Tarr', 'Michael Haneke', 'Lars von Trier', 'Joel Coen',
                            'Joel Coen, Ethan Coen', 'Ethan Coen, Joel Coen',
                            'Quentin Tarantino', 'John Huston', 'Frank Capra',
                            'Pedro Almodóvar', 'Kar-Wai Wong', 'David Fincher',
                            'Jean-Pierre Melville', 'Henri-Georges Clouzot',
                            'William Wyler', ' Elia Kazan', 'Christopher Nolan',
                            'Richard Linklater', 'Mike Leigh', 'Yimou Zhang',
                            'Spike Lee', 'Douglas Sirk', 'Alain Resnais',
                            'Jacques Tati', 'Oliver Stone', 'Brian De Palma',
                            'Rainer Werner Fassbinder', 'Wim Wenders', 'Hsiao-Hsien Hou',
                            'David Cronenberg', 'Edward Yang', 'Terry Gilliam',
                            'Pier Paolo Pasolini', 'Bernardo Bertolucci', 'Ridley Scott',
                            'James Cameron', 'Max Ophüls', 'Ernst Lubitsch',
                            'Josef von Sternberg', 'Jacques Demy', 'Preston Sturges',
                            'Jean Cocteau', 'Mike Nichols', 'Milos Forman',
                            'Alfonso Cuarón', 'Alejandro G. Iñárritu', 'Hayao Miyazaki',
                            'Sam Peckinpah', 'Samuel Fuller', 'Chantal Akerman',
                            'Agnès Varda', 'Nicolas Roeg', 'Ken Loach', 'Wes Anderson',
                            'Darren Aronofsky', 'Alejandro Jodorowsky'])


            if director in top100:
                api['top_100_director'] = 1.0
            else:
                api['top_100_director'] = 0.0

            if len(query['belongs_to_collection']) > 0:
                api['in_a_collection'] = 1
            else:
                api['in_a_collection'] = 0

            genres = query['genres']

            # convert genres to one hot encoding
            api.update({'Action': 0})
            api.update({'Adventure': 0})
            api.update({'Animation': 0})
            api.update({'Comedy': 0})
            api.update({'Crime': 0})
            api.update({'Documentary': 0})
            api.update({'Drama': 0})
            api.update({'Family': 0})
            api.update({'Fantasy': 0})
            api.update({'History': 0})
            api.update({'Horror': 0})
            api.update({'Music': 0})
            api.update({'Mystery': 0})
            api.update({'Romance': 0})
            api.update({'Science Fiction': 0})
            api.update({'Thriller': 0})
            api.update({'War': 0})
            api.update({'Western': 0})
            api.update({'other genre': 0})

            for genre in genres:
                name = genre['name']
                if name in api:
                    count = api[name]
                    api.update({name: (count + 1)})
                else:
                    count = api['other genre']
                    api.update({'other genre': count + 1})

            # Clean out overview


            # convert production companies to one hot encoding
            pro_companies = query['production_companies']

            api.update({'production_cos': len(pro_companies)})

            # convert spoken languages into one hot encoding
            spoken_languages = query['spoken_languages']

            api.update({'en': 0})
            api.update({'es': 0})
            api.update({'fr': 0})
            api.update({'it': 0})
            api.update({'de': 0})
            api.update({'other_spk_lng': 0})
            lgs = ['en', 'es', 'fr', 'it', 'de']

            for language in spoken_languages:
                name = language['iso_639_1']
                if name in lgs:
                    count = api[name]
                    api.update({name: count + 1})
                else:
                    count = api['other_spk_lng']
                    api.update({'other_spk_lng': count + 1})

            api.update({'budget': float(query['budget'])})
            api.update({'profit': float(query['revenue']) - float(query['budget'])})
            api.update({'revenue': float(query['revenue'])})
            api.update({'popularity': float(query['popularity'])})
            api.update({'runtime': float(query['runtime'])})

            release_date = query['release_date']

            try:
                year = float(release_date[0:4])
                month = float(release_date[5:7])
                year_adj = year + (month / 12)
                age = 2020.7 - year_adj
                #api.update({'release': year_adj})
                api.update({'age': age})
            except ValueError:
                print('age error', movie)
                continue

            api.update({'vote_count': float(query['vote_count'])})
            api.update({'vote_average': float(query['vote_average'])})

            if query['id'] in views:
                api.update({'views': views[query['id']]})
            else:
                api.update({'views': 0.0})

            writer.writerow(api)
        return None

create_new_dframe('movie_titles.txt')

