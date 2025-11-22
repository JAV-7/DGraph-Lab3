"""
@file: model.py
@author: Francisco Javier Ramos Jimenez.
@date: 23/11/2025.
@description: file defining model behaviour.
"""

#!/usr/bin/env python3

import datetime
import json
import pydgraph
import csv
import os
import traceback

 """
 Parse a Dgraph query JSON string and return the first uid for `key`, or None.
 """
def _first_uid_from_query(json_str: str, key: str):
    try:
        data = json.loads(json_str)
        items = data.get(key, [])
        if not items:
            return None
        return items[0].get('uid')
    except Exception:
        return None

################## Define Schema #############################

"""
Function to set the schema in the Dgraph databse.

The schema is the following:

     |------↓    
     |--- User --- Likes   Saves   Posts --->    Video
            ||---Creates ---> Playlist --- Contains ----↑
            ↓↓                                          |
        |--> Comment --- Replies_to --------------------|
        |            |
        |-Replies_to-|
           

Args:
    client: Dgraph client.
    Returns: Result of the operation.
"""
def set_schema(client: pydgraph.DgraphClient) -> pydgraph.Response: 
    # Define the schema.
    schema = """
    type Comment {
        text
        replies_to_c_c
        replies_to_c_v

    }

    type Playlist {
        title
        visibility
        contains
    }

    type User {
        username
        email
        location
        likes_u_c
        likes_u_v
        comments
        creates
        saves
        posts
    }

    type Video  {
        title
        description
        duration
        date_uploaded
    }

  
    text: string @index(fulltext) .
    title: string @index(term) .
    visibility: string @index(term) .
    username: string @index(term) .
    email: string @index(term) .
    location: geo @index(geo) .
    description: string .
    duration: int @index(int) .
    date_uploaded: datetime @index(hour) .


    comments: [uid] @reverse .
    likes_u_c: [uid] @reverse .
    likes_u_v: [uid] @reverse .
    posts: [uid] @reverse .
    saves: [uid]  .
    creates: [uid] @reverse .
    contains: [uid] .
    replies_to_c_c: [uid] @reverse .
    replies_to_c_v: [uid] @reverse .

    """
    # Apply the schema to the database.
    return client.alter(pydgraph.Operation(schema=schema))

########################## LOAD NODES ####################################

"""
This function loads the content of the file comments.csv into the database.
Args:
    client: DgraphClient
    file_path: string
Return: result dictionary.
"""
def load_comments(client: pydgraph.DgraphClient, file_path: str) -> dict:
    txn = client.txn()
    resp = None
    try:
        comments = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            i = 1
            for row in reader:
                comments.append({
                    'uid': '_:c' + str(i),
                    'dgraph.type': 'Comment',
                    'text': row['text']
                })
                i += 1
            print(f"Loading comments: {comments}")
            resp = txn.mutate(set_obj=comments)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

"""
Function to load playlists nodes.
Args:
    client: DgraphClient
    file_path: string
Return: result dictionary.
"""
def load_playlists(client: pydgraph.DgraphClient, file_path: str) -> dict:
    txn = client.txn()
    resp = None
    try:
        playlists = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            i = 1
            for row in reader:
                playlists.append({
                    'uid': '_:p' + str(i),
                    'dgraph.type': 'Playlist',
                    'title': row['title'],
                    'visibility': row['visibility']
                })
                i += 1
            print(f"Loading playlists: {playlists}")
            resp = txn.mutate(set_obj=playlists)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

"""
Function to load users nodes.
Args:
    client: DgraphClient
    file_path: string
Return: result dictionary.
"""
def load_users(client: pydgraph.DgraphClient, file_path: str) -> dict:
    txn = client.txn()
    resp = None
    try:
        users = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            i = 1
            for row in reader:
                users.append({
                    'uid': '_:u' + str(i),
                    'dgraph.type': 'User',
                    'username': row['username'],
                    'email': row['email'],
                    'location.lat': row['location.lat'],
                    'location.long': row['location.long']
                })
                i += 1
            print(f"Loading users: {users}")
            resp = txn.mutate(set_obj=users)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

"""
Function to load video nodes.
Args:
    client: DgraphClient
    file_path: string
Return: result dictionary.
"""
def load_videos(client: pydgraph.DgraphClient, file_path: str) -> dict:
    txn = client.txn()
    resp = None
    try:
        videos = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            i = 1
            for row in reader:
                videos.append({
                    'uid': '_:v' + str(i),
                    'dgraph.type': 'Video',
                    'title': row['title'],
                    'description': row['description'],
                    'duration': row['duration'],
                    'date_uploaded': row['date_uploaded']
                })
                i += 1
            print(f"Loading videos: {videos}")
            resp = txn.mutate(set_obj=videos)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

############################### LOAD EDGES ###################################3

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. Comment - replies_to -> comment
Args:
    client: DgraphClient
    file_path: string
"""
def load_comment_replies_comment(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            comment_text = row['comment_text']
            replied_to_text = row['replied_to_text']

            query_ct = f"""{{comment(func: eq(text, "{comment_text}")){{uid}}}}"""
            query_rtt = f"""{{comment(func: eq(text, "{replied_to_text}")){{uid}}}}"""

            res_ct = client.txn(read_only=True).query(query_ct)
            ct_uid = _first_uid_from_query(res_ct.json, 'comment')
            if ct_uid is None:
                print(f"Warning: comment not found for text: '{comment_text}', skipping relation")
                continue

            res_rtt = client.txn(read_only=True).query(query_rtt)
            rtt_uid = _first_uid_from_query(res_rtt.json, 'comment')
            if rtt_uid is None:
                print(f"Warning: replied-to comment not found for text: '{replied_to_text}', skipping relation")
                continue

            # Create edge
            txn = client.txn()
            try:
                # `replies_to_c_c` is defined as [uid] in the schema — send as a list of uid objects
                mutation = {
                    "uid": rtt_uid,
                    "replies_to_c_c": [{"uid": ct_uid}]
                }
                print(f"Generating relationship comment replies to comment...")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. Comment - repliest_to -> video
Args:
    client: DgraphClient
    file_path: string
"""
def load_comment_replies_video(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            comment_text = row['comment_text']
            video_title = row['video_title']

            query_c = f'{{ comment(func: anyoftext(text, "{comment_text}")) {{ uid }} }}'
            res_c = client.txn(read_only=True).query(query_c)
            uid_c = _first_uid_from_query(res_c.json, 'comment')
            if uid_c is None:
                print(f"Warning: comment not found for text: '{comment_text}', skipping relation")
                continue

            query_v = f'{{ video(func: eq(title, "{video_title}")) {{ uid }} }}'
            res_v = client.txn(read_only=True).query(query_v)
            uid_v = _first_uid_from_query(res_v.json, 'video')
            if uid_v is None:
                print(f"Warning: video not found for title: '{video_title}', skipping relation")
                continue


            txn = client.txn()
            try:
                mutation = {
                    'uid': uid_c,
                    'replies_to_c_v': [{'uid': uid_v}]
                }
                print(f"Generating relationship comment -> video: '{comment_text}' -> '{video_title}'")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. Playlist - contains -> video
Args:
    client: DgraphClient
    file_path: string
"""
def load_playlist_contains_video(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            playlist_title = row['playlist_title']
            video_title = row['video_title']

            query_p = f'{{ playlist(func: eq(title, "{playlist_title}")) {{ uid }} }}'
            query_v = f'{{ video(func: eq(title, "{video_title}")) {{ uid }} }}'

            res_p = client.txn(read_only=True).query(query_p)
            uid_p = _first_uid_from_query(res_p.json, 'playlist')
            if uid_p is None:
                print(f"Warning: playlist not found for title: '{playlist_title}', skipping relation")
                continue

            res_v = client.txn(read_only=True).query(query_v)
            uid_v = _first_uid_from_query(res_v.json, 'video')
            if uid_v is None:
                print(f"Warning: video not found for title: '{video_title}', skipping relation")
                continue


            txn = client.txn()
            try:
                mutation = {
                    'uid': uid_p,
                    'contains': [{'uid': uid_v}]
                }
                print(f"Generating relationship playlist -> video: '{playlist_title}' -> '{video_title}'")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. User - comments -> comment
Args:
    client: DgraphClient
    file_path: string
"""
def load_user_comments(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            username = row['username']
            comment_text = row['comment_text']

            query_u = f'{{ user(func: eq(username, "{username}")) {{ uid }} }}'
            query_c = f'{{ comment(func: anyoftext(text, "{comment_text}")) {{ uid }} }}'

            res_u = client.txn(read_only=True).query(query_u)
            uid_u = _first_uid_from_query(res_u.json, 'user')
            if uid_u is None:
                print(f"Warning: user not found for username: '{username}', skipping relation")
                continue

            res_c = client.txn(read_only=True).query(query_c)
            uid_c = _first_uid_from_query(res_c.json, 'comment')
            if uid_c is None:
                print(f"Warning: comment not found for text: '{comment_text}', skipping relation")
                continue

            txn = client.txn()
            try:
                mutation = {
                    'uid': uid_u,
                    'comments': [{'uid': uid_c}]
                }
                print(f"Generating relationship user -> comment: '{username}' -> '{comment_text}'")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. User - creates -> playlist
Args:
    client: DgraphClient
    file_path: string
"""
def load_user_creates_playlist(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            username = row['username']
            title = row['title']

            query_u = f'{{ user(func: eq(username, "{username}")) {{ uid }} }}'
            query_p = f'{{ playlist(func: eq(title, "{title}")) {{ uid }} }}'

            res_u = client.txn(read_only=True).query(query_u)
            uid_u = _first_uid_from_query(res_u.json, 'user')
            if uid_u is None:
                print(f"Warning: user not found for username: '{username}', skipping relation")
                continue

            res_p = client.txn(read_only=True).query(query_p)
            uid_p = _first_uid_from_query(res_p.json, 'playlist')
            if uid_p is None:
                print(f"Warning: playlist not found for title: '{title}', skipping relation")
                continue

            txn = client.txn()
            try:
                mutation = {
                    'uid': uid_u,
                    'creates': [{'uid': uid_p}]
                }
                print(f"Generating relationship user -> creates: '{username}' -> '{title}'")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. User - likes -> comment
Args:
    client: DgraphClient
    file_path: string
"""
def load_user_likes_comment(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            username = row['username']
            comment_text = row['comment_text']

            query_u = f'{{ user(func: eq(username, "{username}")) {{ uid }} }}'
            query_c = f'{{ comment(func: anyoftext(text, "{comment_text}")) {{ uid }} }}'

            res_u = client.txn(read_only=True).query(query_u)
            uid_u = _first_uid_from_query(res_u.json, 'user')
            if uid_u is None:
                print(f"Warning: user not found for username: '{username}', skipping relation")
                continue

            res_c = client.txn(read_only=True).query(query_c)
            uid_c = _first_uid_from_query(res_c.json, 'comment')
            if uid_c is None:
                print(f"Warning: comment not found for text: '{comment_text}', skipping relation")
                continue


            txn = client.txn()
            try:
                mutation = {
                    'uid': uid_u,
                    'likes_u_c': [{'uid': uid_c}]
                }
                print(f"Generating relationship user -> likes_comment: '{username}' -> '{comment_text}'")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. user - likes -> video
Args:
    client: DgraphClient
    file_path: string
"""
def load_user_likes_video(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            username = row['username']
            title = row['title']

            query_u = f'{{ user(func: eq(username, "{username}")) {{ uid }} }}'
            query_v = f'{{ video(func: eq(title, "{title}")) {{ uid }} }}'

            res_u = client.txn(read_only=True).query(query_u)
            uid_u = _first_uid_from_query(res_u.json, 'user')
            if uid_u is None:
                print(f"Warning: user not found for username: '{username}', skipping relation")
                continue

            res_v = client.txn(read_only=True).query(query_v)
            uid_v = _first_uid_from_query(res_v.json, 'video')
            if uid_v is None:
                print(f"Warning: video not found for title: '{title}', skipping relation")
                continue


            txn = client.txn()
            try:
                mutation = {
                    'uid': uid_u,
                    'likes_u_v': [{'uid': uid_v}]
                }
                print(f"Generating relationship user -> likes_video: '{username}' -> '{title}'")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. User - posts -> video
Args:
    client: DgraphClient
    file_path: string
"""
def load_user_posts_video(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            username = row['username']
            title = row['title']

            query_u = f'{{ user(func: eq(username, "{username}")) {{ uid }} }}'
            query_v = f'{{ video(func: eq(title, "{title}")) {{ uid }} }}'


            res_u = client.txn(read_only=True).query(query_u)
            res_v = client.txn(read_only=True).query(query_v)
            uid_u = _first_uid_from_query(res_u.json, 'user')
            if uid_u is None:
                print(f"Warning: user not found for username: '{username}', skipping relation")
                continue
            uid_v = _first_uid_from_query(res_v.json, 'video')
            if uid_v is None:
                print(f"Warning: video not found for title: '{title}', skipping relation")
                continue

            txn = client.txn()
            try:
                mutation = {
                    'uid': uid_u,
                    'posts': [{'uid': uid_v}]
                }
                print(f"Generating relationship user -> posts: '{username}' -> '{title}'")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

"""
This function receives the relationship file path, does a query to retrieve the uids of the
matching criteria and stablishes the specified edges. User - saves -> video
Args:
    client: DgraphClient
    file_path: string
"""
def load_user_saves_video(client: pydgraph.DgraphClient, file_path: str) -> None:
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            username = row['username']
            title = row['title']

            query_u = f'{{ user(func: eq(username, "{username}")) {{ uid }} }}'
            query_v = f'{{ video(func: eq(title, "{title}")) {{ uid }} }}'


            res_u = client.txn(read_only=True).query(query_u)
            res_v = client.txn(read_only=True).query(query_v)
            uid_u = _first_uid_from_query(res_u.json, 'user')
            if uid_u is None:
                print(f"Warning: user not found for username: '{username}', skipping relation")
                continue
            uid_v = _first_uid_from_query(res_v.json, 'video')
            if uid_v is None:
                print(f"Warning: video not found for title: '{title}', skipping relation")
                continue

            txn = client.txn()
            try:
                mutation = {
                    'uid': uid_u,
                    'saves': [{'uid': uid_v}]
                }
                print(f"Generating relationship user -> saves: '{username}' -> '{title}'")
                txn.mutate(set_obj=mutation)
                txn.commit()
            finally:
                txn.discard()

############################# CREATE DATA ###########################

"""
This function inserts data into the Dgraph database.
Args:
    client: Dgraph client.
Returns: None.
"""
def create_data(client: pydgraph.DgraphClient) -> None:
    try:
        """
        Following the given structure on GitHub, data should be read and inserted here.
        Compute absolute data folder paths relative to the project root (one level above `src`) so
        the code works whether `main.py` is executed from the project root or from `src`.
        """
        # Project root (one level above this file's directory)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

        NODES_DATA_PATH = os.path.join(project_root, 'data', 'nodes')
        EDGES_DATA_PATH = os.path.join(project_root, 'data', 'edges')

        ######################## Nodes ############################
        # Define all nodes paths
        COMMENTS_PATH = os.path.join(NODES_DATA_PATH, 'comments.csv')
        PLAYLISTS_PATH = os.path.join(NODES_DATA_PATH, 'playlists.csv')
        USERS_PATH = os.path.join(NODES_DATA_PATH, 'users.csv')
        VIDEOS_PATH = os.path.join(NODES_DATA_PATH, 'videos.csv')

        ######################## Edges #############################
        # Define all edges paths
        COMMENT_REPLIES_COMMENT = os.path.join(EDGES_DATA_PATH, 'comment_replies_comment.csv')
        COMMENT_REPLIES_VIDEO = os.path.join(EDGES_DATA_PATH, 'comment_replies_video.csv')
        PLAYLIST_CONTAINS_VIDEO = os.path.join(EDGES_DATA_PATH, 'playlist_contains_video.csv')
        USER_COMMENTS = os.path.join(EDGES_DATA_PATH, 'user_comments.csv')
        USER_CREATES_PLAYLIST = os.path.join(EDGES_DATA_PATH, 'user_creates_playlist.csv')
        USER_LIKES_COMMENT = os.path.join(EDGES_DATA_PATH, 'user_likes_comment.csv')
        USER_LIKES_VIDEO = os.path.join(EDGES_DATA_PATH, 'user_likes_video.csv')
        USER_POSTS_VIDEO = os.path.join(EDGES_DATA_PATH, 'user_posts_video.csv')
        USER_SAVES_VIDEO = os.path.join(EDGES_DATA_PATH, 'user_saves_video.csv')

        ######################## Sanity checks ######################
        # Make sure the files exist before trying to load them — raise informative errors otherwise.
        required_files = [COMMENTS_PATH, PLAYLISTS_PATH, USERS_PATH, VIDEOS_PATH,
                          COMMENT_REPLIES_COMMENT, COMMENT_REPLIES_VIDEO, PLAYLIST_CONTAINS_VIDEO,
                          USER_COMMENTS, USER_CREATES_PLAYLIST, USER_LIKES_COMMENT, USER_LIKES_VIDEO,
                          USER_POSTS_VIDEO, USER_SAVES_VIDEO]

        missing = [f for f in required_files if not os.path.isfile(f)]
        if missing:
            raise FileNotFoundError(f"Missing data files: {missing}")

        ######################## Calling helper functions ###################
        load_comments(client, COMMENTS_PATH)
        load_playlists(client, PLAYLISTS_PATH)
        load_users(client, USERS_PATH)
        load_videos(client, VIDEOS_PATH)
        load_comment_replies_comment(client, COMMENT_REPLIES_COMMENT)
        load_comment_replies_video(client, COMMENT_REPLIES_VIDEO)
        load_playlist_contains_video(client, PLAYLIST_CONTAINS_VIDEO)
        load_user_comments(client, USER_COMMENTS)
        load_user_creates_playlist(client, USER_CREATES_PLAYLIST)
        load_user_likes_comment(client, USER_LIKES_COMMENT)
        load_user_likes_video(client, USER_LIKES_VIDEO)
        load_user_posts_video(client, USER_POSTS_VIDEO)
        load_user_saves_video(client, USER_SAVES_VIDEO)
    except Exception as e:
        print(f"Error loading data: {e}")
        traceback.print_exc()

#################################### QUERIES ############################333333

"""
Query the indexed fulltext `text` field (comments) using anyoftext.
Returns a list of comment objects with uid and text.
"""
def query_by_text(client: pydgraph.DgraphClient, text_search: str) -> None:
    esc = text_search.replace('"', '\\"')
    q = f'{{ comments(func: anyoftext(text, "{esc}")) {{ uid text }} }}'
    txn = client.txn(read_only=True)
    try:
        res = txn.query(q)
        data = json.loads(res.json)
        return data.get('comments', [])
    finally:
        txn.discard()

"""
Query videos filtered by numeric predicate `duration` greater than min_duration.
Returns a list of video objects with uid, title and duration.
"""
def query_by_numeric_duration(client: pydgraph.DgraphClient, min_duration: int = 0) -> None:
    q = f'{{ long_videos(func: gt(duration, {int(min_duration)})) {{ uid title duration }} }}'
    txn = client.txn(read_only=True)
    try:
        res = txn.query(q)
        data = json.loads(res.json)
        return data.get('long_videos', [])
    finally:
        txn.discard()

"""
Graph traversal: return Users and the Videos they posted (two node types).
Returns a list of users where each user contains a `posts` list of videos.
"""
def query_users_with_posts(client: pydgraph.DgraphClient) -> None:
    q = '''{
      users_with_posts(func: type(User)) {
        uid
        username
        posts {
          uid
          title
          duration
        }
      }
    }'''
    txn = client.txn(read_only=True)
    try:
        res = txn.query(q)
        data = json.loads(res.json)
        return data.get('users_with_posts', [])
    finally:
        txn.discard()

"""
Use the reverse edge to find Users who posted a given Video.
Returns the video node with a nested list of users via the ~posts reverse predicate.
"""
def query_video_posters_reverse(client: pydgraph.DgraphClient, video_title: str) -> None:
    esc = video_title.replace('"', '\\"')
    q = f'{{ video_posters(func: eq(title, "{esc}")) {{ uid title ~posts {{ uid username }} }} }}'
    txn = client.txn(read_only=True)
    try:
        res = txn.query(q)
        data = json.loads(res.json)
        return data.get('video_posters', [])
    finally:
        txn.discard()


"""
Return videos sorted by duration. Set desc=False for ascending.
Returns a list of videos with uid, title and duration.
"""
def query_videos_sorted(client: pydgraph.DgraphClient, desc: bool = True) -> None:
    if desc:
        q = '{ videos_sorted(func: type(Video), orderdesc: duration) { uid title duration } }'
    else:
        q = '{ videos_sorted(func: type(Video), orderasc: duration) { uid title duration } }'
    txn = client.txn(read_only=True)
    try:
        res = txn.query(q)
        data = json.loads(res.json)
        return data.get('videos_sorted', [])
    finally:
        txn.discard()

"""
Return the count of Video nodes using count(uid).
"""
def query_video_count(client: pydgraph.DgraphClient) -> int:
    q = '{ video_count(func: type(Video)) { count: count(uid) } }'
    txn = client.txn(read_only=True)
    try:
        res = txn.query(q)
        data = json.loads(res.json)
        items = data.get('video_count', [])
        if items:
            return items[0].get('count', 0)
        return 0
    finally:
        txn.discard()


"""
Return a paginated page of videos sorted by title.
first and offset implement pagination.
"""
def query_videos_paged(client: pydgraph.DgraphClient, first: int = 2, offset: int = 0):
    q = f'{{ paged_videos(func: type(Video), first: {int(first)}, offset: {int(offset)}, orderasc: title) {{ uid title }} }}'
    txn = client.txn(read_only=True)
    try:
        res = txn.query(q)
        data = json.loads(res.json)
        return data.get('paged_videos', [])
    finally:
        txn.discard()




########################################## DELETE FUNCTIONS ##############################################


"""
Delete all Comment nodes that match `comment_text` using fulltext search.
"""
def delete_comment(client: pydgraph.DgraphClient, comment_text: str) -> None:
    query = f'{{ comments(func: anyoftext(text, "{comment_text}")) {{ uid }} }}'

    # find matching UIDs
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        comments = data.get('comments', [])
        uids = [c['uid'] for c in comments if 'uid' in c]
    finally:
        txn.discard()

    if not uids:
        print(f"No comments found matching: '{comment_text}'")
        return 0

    deletes = [{'uid': uid} for uid in uids]
    txn = client.txn()
    try:
        print(f"Deleting {len(deletes)} comment(s) matching: '{comment_text}'")
        txn.mutate(del_obj=deletes)
        txn.commit()
    finally:
        txn.discard()

############################## DROP ALL ###################################

"""
Function to drop all data and schema from the Dgraph database.
Args:
    client: Dgraph client.
Returns: Result of the operation.
Credits to professor Leobardo Ruiz for this function.
"""
def drop_all(client: pydgraph.DgraphClient) -> pydgraph.Response:
    return client.alter(pydgraph.Operation(drop_all=True))