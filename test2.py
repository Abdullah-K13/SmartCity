import base64
from requests import post, get
import json
import time

client_id = 'b11178fe5f354b12a98234fa4bbe95e5'
client_secret = '3c71248dd25949a3b1f844bb2ebb713b'

def get_token():
    auth_strings = client_id + ":" + client_secret
    auth_bytes = auth_strings.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    
    if result.status_code == 200:
        json_result = json.loads(result.content)
        token = json_result["access_token"]
        return token
    else:
        print(f"Failed to get token, status code: {result.status_code}")
        return None

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

def search_top_tracks(token, artist_name):
    url = "https://api.spotify.com/v1/search?"
    headers = get_auth_header(token)
    query = f"q={artist_name}&type=track&limit=10"  # Limit to top 10 tracks
    query_url = url + query

    result = get(query_url, headers=headers)
    
    if result.status_code == 200:
        json_result = json.loads(result.content)
        tracks = json_result['tracks']['items']
        
        if len(tracks) == 0:
            print("No tracks found for this artist...")
            return None
        else:
            print(f"\nTop 10 songs for {artist_name}:\n")
            for idx, track in enumerate(tracks):
                track_name = track['name']
                artist_names = ', '.join([artist['name'] for artist in track['artists']])
                popularity = track['popularity']  # Popularity score (0-100)
                print(f"{idx+1}. {track_name} by {artist_names} - Popularity: {popularity}")
    else:
        print(f"Failed to search for tracks, status code: {result.status_code}")

# Execute the function to get the token
token = get_token()

if token:
    print(f"Access Token: {token}")

    # Continuous fetching of real-time data every X seconds
    artist_name = "AC/DC"  # Specify the artist's name
    while True:
        search_top_tracks(token, artist_name)
        time.sleep(60)  # Fetch every 60 seconds (or adjust as needed)
else:
    print("Failed to retrieve access token")
