import datetime
import dateutil.parser as parser

import pymongo

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.assignment_test

# del useless fields in tag and user
tags = db.tags.find()
for t in tags:
    del t['ExcerptPostId']
    del t['WikiPostId']
    db.tags.update({'_id': t['_id']}, t)

users = db.users.find()
for u in users:
    del u['AccountId']
    dt = parser.parse(u['CreationDate']).isoformat().split('.')[0]
    u['CreationDate'] = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
    dt = parser.parse(u['LastAccessDate']).isoformat().split('.')[0]
    u['LastAccessDate'] = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
    db.users.update({'_id': u['_id']}, u)

# reformat Tags, add embedded document OwnerUser
all_post = db.posts.find()
for post in all_post:
    tags = post['Tags']
    tags = [] if tags == '' else tags.replace('\"', '').split(',')
    post['Tags'] = tags
    if 'OwnerUserId' in post and post['OwnerUserId'] != '':
        user_detail = db.users.find_one({'Id': post['OwnerUserId']})
        post['OwnerUser'] = {'Id': user_detail['Id'],
                             'CreationDate': user_detail['CreationDate'],
                             'DisplayName': user_detail['DisplayName'],
                             'UpVotes': user_detail['UpVotes'],
                             'DownVotes': user_detail['DownVotes'],
                             }
    else:
        post['OwnerUser'] = None
    if post['CreationDate'] != '':
        post['CreationDate'] = datetime.datetime.strptime(parser.parse(post['CreationDate']).isoformat().split('.')[0],
                                                          "%Y-%m-%dT%H:%M:%S")
    if post['ClosedDate'] != '':
        post['ClosedDate'] = datetime.datetime.strptime(parser.parse(post['ClosedDate']).isoformat().split('.')[0],
                                                        "%Y-%m-%dT%H:%M:%S")
    db.posts.update({'_id': post['_id']}, post)

# add a list in question to record all the answers
questions = db.posts.aggregate([
    {'$lookup': {
        'from': "posts",
        'localField': "Id",
        'foreignField': "ParentId",
        'as': "answers"}},
])
for q in questions:
    ids = [i['Id'] for i in q['answers']]
    if len(ids) > 0:
        q['AnswerId'] = [i['Id'] for i in q['answers']]
        del q['answers']
        db.posts.update({'_id': q['_id']}, q)

# add Tags, title, IsAccepted for answers
answers = db.posts.aggregate([
    {'$match': {'PostTypeId': 2}},
    {'$lookup': {
        'from': "posts",
        'localField': "ParentId",
        'foreignField': "Id",
        'as': "Parent"}}
])
for a in answers:
    condition = {'_id': a['_id']}
    a['Tags'] = a['Parent'][0]['Tags']
    a['Title'] = a['Parent'][0]['Title']
    if a['Id'] == a['Parent'][0]['AcceptedAnswerId']:
        a['IsAccepted'] = True
    else:
        a['IsAccepted'] = False
    del a['Parent']
    db.posts.update(condition, a)

# add embedded document AcceptedAnswer for questions
accepted_answers = db.posts.find({'PostTypeId': 2, 'IsAccepted': True})
for a in accepted_answers:
    aid = a['Id']
    qid = a['ParentId']
    q = db.posts.find_one({'Id': qid})
    q['AcceptedAnswer'] = {'Id': a['Id'],
                           'CreationDate': a['CreationDate'],
                           'Score': a['Score'],
                           'OwnerUserId': a['OwnerUserId'],
                           'CommentCount': a['CommentCount']
                           }
    del q['AcceptedAnswerId']
    db.posts.update({'Id': qid}, q)

# del useless fields in answers
answers = db.posts.find({'PostTypeId': 2})
for answer in answers:
    del answer['AcceptedAnswerId']
    del answer['ViewCount']
    del answer['AnswerCount']
    del answer['FavoriteCount']
    del answer['ClosedDate']
    del answer['OwnerDisplayName']
    db.posts.update({'_id': answer['_id']}, answer)

# del useless fields in questions
questions = db.posts.find({'PostTypeId': 1})
for q in questions:
    if 'FavoriteCount' in q:
        if q['FavoriteCount'] == '':
            q['FavoriteCount'] = 0
        else:
            q['FavoriteCount'] = int(q['FavoriteCount'])
    if 'ClosedDate' in q:
        if q['ClosedDate'] == '':
            q['ClosedDate'] = None
    if 'ParentId' in q:
        del q['ParentId']
    if 'OwnerDisplayName' in q:
        del q['OwnerDisplayName']
    if 'AcceptedAnswerId' in q and q['AcceptedAnswerId'] != '':
        del q['AcceptedAnswerId']
    elif 'AcceptedAnswerId' in q and q['AcceptedAnswerId'] == '':
        q['AcceptedAnswer'] = None
        del q['AcceptedAnswerId']
    db.posts.update({'_id': q['_id']}, q)

# create indexes
db.posts.create_index([('Id', pymongo.ASCENDING)], unique=True)
db.posts.create_index([('ParentId', pymongo.ASCENDING)])
db.posts.create_index([('Tags', pymongo.ASCENDING)])

client.close()
