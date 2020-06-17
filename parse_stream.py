from kafka import KafkaConsumer
import csv

users = set()

# returns string of title given a ConsumerRecord
def parse_cr(cr):
    binary = cr.value
    string = binary.decode('utf-8')
    # [time, user id, GET request]
    return string.split(',')


# returns string of title given a ConsumerRecord in name+name+year format regardless of rate or data
def get_title(cr):
    get = parse_cr(cr)[2]
    head = get[5:9]
    if head == 'data':
        trunc = get[12:]
        return trunc.split('/')[0]
    else:
        trunc = get[10:]
        return trunc.split('=')[0]

dates = set()
def gather_popularity():
    first = None
    popularity = dict()


    consumer = KafkaConsumer(
        'movielog',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='jcerwin-stream',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )
    duration = 0
    max_duration = 500000000
    for message in consumer:
        if duration > max_duration: break
        else: duration += 1

        if duration % (max_duration / 100) == 0:
            print(duration / (max_duration / 100), "% complete")

        if first is None:
            first = message
        else:
            if message == first:
                print("repeat")
                break

        parsed = parse_cr(message)
        r_block = parsed[2]
        head = r_block[5:9]
        # look for watches only not reviews
        if head == 'data':
            trunc = r_block[12:]
            title = trunc.split('/')[0]

            minutes = r_block.split('/')[4][:-4]
        else:
            continue

        if int(minutes) == 0:
            date = (parsed[0])[5:10]
            if title in popularity:
                count = popularity[title]
                popularity[title] = count + 1

            else:
                popularity[title] = 1

            dates.add(date)


    return popularity

def gather_titles():
    consumer = KafkaConsumer(
        'movielog',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='jcerwin-new',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    f = open("movie_titles.txt", "r")
    fl = f.readlines()
    f.close()
    s = set(fl)
    i = len(s)

    f = open("movie_titles.txt", "a")
    for message in consumer:
        if i > 27000:
            break
        title = get_title(message) + '\n'
        if title in s:
            continue
        else:
            s.add(title)
            f.write(title)
            i = i + 1

    f.close()

#with open('views.csv', 'w') as csv_file:
#    writer = csv.writer(csv_file)
#    for key, value in gather_popularity().items():
#        writer.writerow([key, value])


results = gather_popularity()
num_days = len(dates)

with open('views3.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in results.items():
        writer.writerow([key, value / num_days])


