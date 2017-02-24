
# Curator Daemon
##### Daemon for background actions for [curator.tekuma.io](https://curator.tekuma.io/)
##### Handles adding data to SQL DB,

**NOTE: Use Node.js LTS version v(6.9.1)**
**NOTE: NPM v3.8.10**

## Using Server
- Log in via SSH to `server1` (10.142.0.2) from [Google Cloud Console](https://console.cloud.google.com/compute/instances?project=artist-tekuma-4a697)

- IF returning: `tmux a -t curator` to go to active process.
- IF restarted: go to `/tekuma/artist-server`
 - `tmux new -s curator`

- Run `sudo node curator_daemon.js` to start the server code

## Note: not implemented yet!
