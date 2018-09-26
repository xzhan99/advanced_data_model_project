import datetime

import pymongo


# [SQ1] Find all users involved in a given question
def find_users_by_question(db, question_id):
    users = db.posts.aggregate([
        {'$lookup': {'from': "users", 'localField': "OwnerUserId", 'foreignField': "Id", 'as': "user_detail"}},
        {'$match': {'$or': [{'Id': question_id}, {'ParentId': question_id}]}},
        {'$project': {'_id': 0, 'Id': "$user_detail.Id", 'CreationDate': "$user_detail.CreationDate",
                      'DisplayName': "$user_detail.DisplayName", 'upVote': "$user_detail.upVote",
                      'DownVote': "user_detail.DownVote"}}
    ])
    for user in list(users):
        print(user)


# [SQ2] find the most viewed question in a given topic
def find_most_viewed_question(db, topic):
    question = db.posts.aggregate([
        {'$match': {'Tags': "neural-networks"}},
        {'$sort': {'ViewCount': -1}},
        {'$limit': 1}
    ])
    question = list(question)[0]
    print(question)


# [AQ1] Given a list of topics (tags), find the question easiest to answer in each topic
def find_easiest_question(db, topic):
    question = db.posts.aggregate([
        {'$match': {'Tags': topic}},
        {'$lookup': {
            'from': "posts",
            'localField': "AnswerId",
            'foreignField': "Id",
            'as': "answers"}},
        {'$unwind': "$answers"},
        {'$project': {'_id': 0, 'Id': "$Id", 'Title': "$Title",
                      'gap': {'$subtract': ["$answers.CreationDate", "$CreationDate"]}}},
        {'$sort': {'gap': 1}},
        {'$limit': 1}
    ])
    question = list(question)[0]
    print(question)


# [AQ2] Given a time period as indicated by starting and ending date, find the top 5 topics in that period
def find_hot_topics_by_period(db, start_time, end_time):
    start = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
    end = datetime.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
    print(start)
    print(end)
    hot_tags = db.posts.aggregate([
        {'$match': {
            'CreationDate': {'$gt': start, '$lt': end}}},
        {'$unwind': "$Tags"},
        {'$group': {'_id': {'Tag': "$Tags", 'OwnerUserId': "$OwnerUserId"}, 'sum': {'$sum': 1}}},
        {'$group': {'_id': "$_id.Tag", 'sumOfPosts': {'$sum': 1}}},
        {'$sort': {'sumOfPosts': -1}},
        {'$limit': 5}
    ])
    for tag in list(hot_tags):
        print(tag)


# [AQ3] Given a topic, find the champion user and all questions the user has answers accepted in that topic


if __name__ == '__main__':
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.assignment1

    # find_users_by_question(db, 1)
    # find_most_viewed_question(db, 'neural-networks')
    # find_easiest_question(db, 'neural-networks')
    find_hot_topics_by_period(db, '2018-08-01T00:00:00', '2018-08-31T00:00:00')

    client.close()
