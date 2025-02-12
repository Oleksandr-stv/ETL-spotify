# ETL-spotify
Building a pipeline to extract, transform and load data from your own Spotify account (last 10 listened songs)

In order to receive an authorization code from Spotify, you need to insert the following data into the browser address bar: https://accounts.spotify.com/authorize?client_id=YOUR_CLIENT_ID&response_type=code&redirect_uri=YOUR_REDIRECT_URI&scope=YOUR_SCOPE

client_id: Your Client ID obtained after registering your app with Spotify.

response_type: Should be set to code to indicate that an authorization code has been received.

redirect_uri: The URI to which you will be redirected after authorization (must match what is specified in the app settings).

scope: This is the list of permissions that your app wants to obtain.
