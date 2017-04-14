// JS ES6+
// Copyright 2017 Tekuma Inc.
// All rights reserved.
// created by Stephen L. White
//
//  See README.md for more, but this file is heavily commented for clarity.

//Libs
const firebase = require('firebase-admin');
const gcloud   = require('google-cloud');
const eachOf   = require('async/eachOf');
const mysql    = require('mysql');
const fs       = require('fs');
const uuid     = require('uuid/v4');

//Keys
const serviceKey = require('./auth/artistKey.json');
const curatorKey = require('./auth/curatorKey.json');
const dbconf     = require('./dbconf.json');

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

// =============== Methods ===================

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
 * so that the artist can mutate its data. Since, after an artist
 * submits an artwork, they are blocked from editing it.
 * @param  {DataSnapshot} snapshot
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
 * `2017-02-05T15:19:43.674Z` => `2017-02-05 15:19:43`
 * JS timestamp to be SQL `DATETIME` style.
 * @param  {String}
 * @return {String}
 */
sqlizeDate = (date) => {
    // date could be in form 1491930244646
    if (typeof date == 'number') {
        date = new Date(date).toISOString();
    }
    date = date.replace("T"," ");
    date = date.replace(".","");
    date = date.substring(0,19);
    return date;
}

/**
 * Establishes connection to the Tekuma Proprietary art database. The database
 * is a google cloud sql instance running MySQL.
 *
 * The production instance name is :  `Tekuma_artworkdb`
 * @return {Obect} The mysql server connection
 */
connectSQL = () => {
    if (dbconf.ssl) {
        dbconf.ssl.cert = fs.readFileSync(__dirname + '/cert/' + dbconf.ssl.cert);
        dbconf.ssl.key  = fs.readFileSync(__dirname + '/cert/' + dbconf.ssl.key);;
        dbconf.ssl.ca   = fs.readFileSync(__dirname + '/cert/' + dbconf.ssl.ca);
    } else {
        console.log("> DB Conf file lacks proper ssl information or is not located in ./cert");
    }
    dbconf.charset = 'utf8'; // just common sense
    dbconf.multipleStatements = true; // transactions require multiple statements.
    let db = mysql.createConnection(dbconf);
    db.connect();
    console.log(">> Connection to MySQL Instance Succesful.");
    return db
}

/**
 *  Labels:
 *       - "#9aa0a9 0.3855" clarifai-color-density
 *       - dominant-color
 *       - clarifai-w3c-color-density
 *       - clarifai-text-tag
 * @param  {JSON} artwork  the artwork object from the /approved branch.
 * @return {Array} an array of objects (labels)
 */
extractLabels = (artwork) => {
    let labels = [];
    if (artwork.colors) {
        for (let i = 0; i < artwork.colors.length; i++) {
            let color = artwork.colors[i];
            let label = {
                source: "Clarif.ai",
                label: `${color.hex} ${color.density}`,
                type: "clarifai-color-density",
                uid: uuid(),
            }
            let w3cLabel = {
                source: "Clarif.ai",
                label: `${color.w3c.hex}`,
                type: "clarifai-w3c-color-density",
                uid: uuid(),
            }
            labels.push(label);
            labels.push(w3cLabel);
        }
    }
    if (artwork.tags) {
        for (let i = 0; i < artwork.tags.length; i++) {
            let tag = {
                source: "Clarif.ai",
                label: artwork.tags[i].text,
                type: "clarifai-text-tag",
                uid: uuid(),
            }
            labels.push(tag);
        }
    }
    return labels;

}

/**
 * This method first translates all the snapshot into valid data, and then forms
 * a TRANSACTION for all of the artwork's data. Additionally, an attempt is made
 * to insert the artist info in a separte query, which could be rejected as a
 * duplicate, or inserted.
 * If you need to check the columns of a table, use the query:
 * `SHOW COLUMNS FROM ${table_name};`
 * @param  {DataSnapshot} snapshot [firebase snapshot of the artwork's JSON]
 */
handleSqlInsert = (snapshot) => {
    let artwork = snapshot.val();
    if (!artwork.sql && snapshot.key != "0") { // if not already inserted / placeholder (0)
        console.log("==> Artowrk: ", snapshot.key, "Is about to be inserted into SQL DB.");
        const db = connectSQL();
        let artwork_query = generateArtworkInsertQuery(artwork,db);
        db.query(artwork_query, (err,res,fld)=>{
            if (err) console.log(err);
            console.log(res);
            insertArtist(artwork,db).then( (success)=>{
                if (success) {
                    console.log("Artwork and Artist Inserted");
                } else {
                    console.log("Artwork Inserted");
                }
            });
        });
    }
}

/**
 * Inserts a row into the artworks table.
 * TABLE: artworks:
 * - uid (char)
 * - title (char)
 * - description (text)
 * - artist_uid (char)
 * - date_of_addition (DATETIME)
 * - date_of_creation (DATETIME)
 * - thumbnail_url (char)
 * - origin (char)
 * - reverse_lookup (char)
 * - META (blob)
 * -----------------
 * @param  {JSON} artwork [object from \approved branch]
 * @param  {MySQL_Connection} db
 * @return {Promise} passed a boolean. True if Succesful, False if error.
 */
generateArtworkInsertQuery = (artwork, db) =>{
    let labels = extractLabels(artwork);
    let theQuery  = "START TRANSACTION; \n";
    let thumbnail = `https://storage.googleapis.com/art-uploads/portal/${artwork.artist_uid}/thumb128/${artwork.artwork_uid}`;
    let date = sqlizeDate(artwork.submitted);

    let art_query  = `INSERT INTO artworks (uid,title,description,artist_uid,date_of_addition,thumbnail_url,origin) VALUES ('${artwork.artwork_uid}', '${artwork.artwork_name}', '${artwork.description}', '${artwork.artist_uid}', '${date}', '${thumbnail}', 'portal'); \n`;
    theQuery = theQuery.concat(art_query);
    for (let label of labels) {
        let quer = `INSERT INTO labels (uid,val,labeltype,origin)  VALUES ('${label.uid}', '${label.label}', '${label.type}', '${label.source}') ; \n`;
        let assoc = `INSERT INTO associations (label_uid, object_uid, object_table) VALUES ('${label.uid}', '${artwork.artwork_uid}', 'artworks'); \n`;
        theQuery = theQuery.concat(quer,assoc);
    }

    return theQuery.concat("COMMIT;");
}

/**
 * Inserts the artist into the table. Is usually expected to throw an error,
 * as it is called on every insert, and could very well be a duplicate.
 *- TABLE: artists.
 *   - uid (char)
     - artist (text)
     - human_name (text)
     - date_of_addition (DATETIME)
     - META (blob)
 *  @return {Promise}
 */
insertArtist = (artwork,db) =>{
    console.log("=> Inserting Aritist",artwork.artist_name,artwork.artist_uid);
    return new Promise((resolve, reject)=>{
        let date = sqlizeDate(new Date().toISOString());
        let artist_query = "INSERT INTO artists (uid, artist, human_name, date_of_addition) VALUES (?, ?, ?, ?);";
        let keys = [artwork.artist_uid, artwork.artist_name, artwork.artist_name, date];
        db.query(artist_query,keys, (err,res,fld)=>{
            if (err) {
                console.log(err)
                resolve(false);
            } else {
                console.log(res);
                resolve(true);
            }
            console.log(res);
        });
    });
}


/**
 * Mutates the object at in the approved branch to have property
 * of sql set to true to reflect that it has been inserted into the central
 * database.
 * @param  {String} artwork_uid
 */
markAsInserted = (artwork_uid) => {
    console.log("Artwork:", artwork_uid, " About to be marked as inserted");
    let path = `approved/${artwork_uid}`;
    curator.database().ref(path).transaction((data)=>{
        data["sql"] = true;
        return data;
    });
}



// =========== Exec ===========

// --- Run these 2 functions to run the daemon ----
listenForHeld();
listenForApprovals();

// let artwork = {
//       "album" : "Vincent",
//       "approved" : 1486308100304,
//       "artist_name" : "Afika Nyati",
//       "artist_uid" : "x4hhJGNPx9g3jH2iikX60tdnn6p1",
//       "artwork_name" : "The Starry Night",
//       "artwork_uid" : "-KcDphAm1fx6U6CYzYtr",
//       "colors" : [ {
//         "density" : 0.34575,
//         "hex" : "#566f88",
//         "w3c" : {
//           "hex" : "#708090",
//           "name" : "SlateGray"
//         }
//       }, {
//         "density" : 0.13825,
//         "hex" : "#28345a",
//         "w3c" : {
//           "hex" : "#483d8b",
//           "name" : "DarkSlateBlue"
//         }
//       }, {
//         "density" : 0.177,
//         "hex" : "#2c3d89",
//         "w3c" : {
//           "hex" : "#483d8b",
//           "name" : "DarkSlateBlue"
//         }
//       }, {
//         "density" : 0.02325,
//         "hex" : "#98994f",
//         "w3c" : {
//           "hex" : "#bdb76b",
//           "name" : "DarkKhaki"
//         }
//       }, {
//         "density" : 0.183,
//         "hex" : "#222523",
//         "w3c" : {
//           "hex" : "#000000",
//           "name" : "Black"
//         }
//       }, {
//         "density" : 0.13275,
//         "hex" : "#8f9a8a",
//         "w3c" : {
//           "hex" : "#a9a9a9",
//           "name" : "DarkGray"
//         }
//       } ],
//       "description" : "This is a collection of famous Van Gogh oil paintings.",
//       "memo" : "Your use of thick brush strokes to evoke movement, combined with your rich color choices result in a really beautiful artwork!",
//       "new_message" : true,
//       "reviewer" : "Afika",
//       "size" : 345814,
//       "status" : "Approved",
//       "submitted" : "2017-02-05T15:19:43.674Z",
//       "tags" : [ {
//         "id" : 1,
//         "text" : "pattern"
//       }, {
//         "id" : 2,
//         "text" : "art"
//       }, {
//         "id" : 3,
//         "text" : "abstract"
//       }, {
//         "id" : 4,
//         "text" : "painting"
//       }, {
//         "id" : 5,
//         "text" : "design"
//       }, {
//         "id" : 6,
//         "text" : "illustration"
//       }, {
//         "id" : 7,
//         "text" : "desktop"
//       }, {
//         "id" : 8,
//         "text" : "texture"
//       }, {
//         "id" : 9,
//         "text" : "nature"
//       }, {
//         "id" : 10,
//         "text" : "water"
//       }, {
//         "id" : 11,
//         "text" : "color"
//       }, {
//         "id" : 12,
//         "text" : "image"
//       }, {
//         "id" : 13,
//         "text" : "artistic"
//       }, {
//         "id" : 14,
//         "text" : "wallpaper"
//       }, {
//         "id" : 15,
//         "text" : "canvas"
//       }, {
//         "id" : 16,
//         "text" : "no person"
//       } ],
//       "upload_date" : "2017-02-05T15:14:08.692Z",
//       "year" : 2017
// };
//

// ---- Use these lines to communicate directly to the DB. -----

// let db = connectSQL();
// let thing = "SHOW COLUMNS FROM artworks"
// db.query(thing, (err,res,fld)=>{
//     console.log("err",err);
//     console.log("res",res);
//     // console.log("fld",fld);
// });
