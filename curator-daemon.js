// JS ES6+
// Copyright 2017 Tekuma Inc.
// All rights reserved.
// created by Stephen L. White
//
//  See README.md for more.

//Libs
const firebase = require('firebase-admin');
const gcloud   = require('google-cloud');

//Keys
const serviceKey = require('./auth/artistKey.json');
const curatorKey = require('./auth/curatorKey.json');


// DEFAULT App : artist-tekuma-4a697 connection : firebase.*
firebase.initializeApp({
    databaseURL : "https://artist-tekuma-4a697.firebaseio.com",
    credential  : firebase.credential.cert(serviceKey)
});
// SECONDARY App : curator-tekuma connection // curator.*
const curator  = firebase.initializeApp({
    databaseURL : "https://curator-tekuma.firebaseio.com/",
    credential  : firebase.credential.cert(curatorKey)
}, "curator");

/*
Restructure:
- Give 'Held' its own branch
- when labeled 'held' move into held and remove from submitted
    - listen for held branch
    - change reviewmanager code
- when artwork is resubmitted, it will re-appear in pending
    - if accepted or declined, move it there and delete from held
    - if moved to held, overwrite held
 */

/**
 * Establishes a listen on the /jobs branch of the DB. Any children added
 * to the node will trigger the handleData callback.
 */
listenForApprovals = () => {
    let path = 'approved/';
    console.log(">>> Firebase Conneced. Listening for approved...");
    curator.database().ref(path).on('child_added', handleSqlInsert);
}

listenForHeld = () => {
    let path = 'held/';
    console.log(">>> Listening for Held...");
    curator.database().ref(path).on('child_added', unlockArtwork);
}

/**
 * Should change the 'submitted' property of the artwork
 * in the artist-tekuma-4a697 firebase database to false,
 * so that the artist can mutate its data.
 * @param  {DataSnapshot} snapshot [description]
 */
unlockArtwork = (snapshot) => {
    let artwork = snapshot.val();
    if (artwork.status == "Held") {
        console.log("> Request to unlock artwork:");
        if (artwork.artist_uid && artwork.artwork_uid) {

            let path = `public/onboarders/${artwork.artist_uid}/artworks/${artwork.artwork_uid}`;
            console.log(path);
            firebase.database().ref(path).transaction((data)=>{
                if (data) {
                    data.submitted = false;
                }
                return data;
            }).catch((err)=>{console.log(err);}).then(()=>{
                console.log(`> Artwork:${artwork.artwork_uid} has been unlocked.`);
            });
        }
    }
}

/**
 * This method should handle extracting all of the artworks data from JSON form,
 * restructuring it, and inserting it into the cloudsql database. On sucesful
 * insertion, the firebase database should be updated to reflect this.
 * @param  {DataSnapshot} snapshot [firebase snapshot of the artwork's JSON]
 */
handleSqlInsert = (snapshot) => {
    console.log("APPROVED ARTWORK: ready for insertion into database");
}

/**
 * Mutates the object at in the approved branch to have property
 * of sql set to true to reflect that it has been inserted into the central
 * database.
 * @param  {[type]} artwork_uid [description]
 */
markAsInserted = (artwork_uid) => {
    let path = `approved/${artwork_uid}`;
    curator.database().ref(path).transaction((data)=>{
        data["sql"] = true;
        return data;
    });
}

connectSQL = () => {
    //TODO
}

// =========== Exec ===========

listenForHeld();
listenForApprovals();
