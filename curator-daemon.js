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
    dbconf.charset = 'utf8';

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
 * This method should handle extracting all of the artworks data from JSON form,
 * restructuring it, and inserting it into the cloudsql database. On sucesful
 * insertion, the firebase database should be updated to reflect this.
 * If you need to check the columns of a table, use the query:
 * `SHOW COLUMNS FROM ${table_name};`
 * @param  {DataSnapshot} snapshot [firebase snapshot of the artwork's JSON]
 */
handleSqlInsert = (snapshot) => {
    let artwork = snapshot.val();
    // console.log(artwork.sql, artwork.artwork_uid);
    if (!artwork.sql && snapshot.key != "0") { // if not already inserted / placeholder (0)
        console.log("==> Artowrk: ", snapshot.key, "Is about to be inserted into SQL DB.");
        const db = connectSQL();

        insertArtwork(artwork,db).then((success)=>{
            if (success) {
                insertArtist(artwork,db).then( (success2)=>{
                    if (success2) {
                        let labels = extractLabels(artwork);
                        insertLables(labels,db).then((success3)=>{
                            if (success3) {
                                insertAssociations(artwork,labels,db).then((success)=>{
                                    if (success) {
                                        console.log("SUCCESSFULLY Inserted entire artwork.");
                                        markAsInserted(artwork.artwork_uid);
                                    } else {
                                        console.log("Failed to insert all parts. See above.");
                                    }
                                    db.end();
                                });
                            } else {
                                console.log("Failed to insert all parts. See above.");
                                db.end();
                            }
                        });
                    } else {
                        console.log("Failed to insert all parts. See above.");
                        db.end();
                    }
                });
            } else {
                console.log("Failed to insert all parts. See above.");
                db.end();
            }
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
insertArtwork = (artwork, db) =>{
    console.log("==> Inserting artwork");
    return new Promise((resolve, reject)=>{
        let thumbnail = `https://storage.googleapis.com/art-uploads/portal/${artwork.artist_uid}/thumb128/${artwork.artwork_uid}`;
        let date = sqlizeDate(artwork.submitted);

        // insert the artwork first
        let insert_artwork = "INSERT INTO artworks (uid,title,description,artist_uid,date_of_addition,thumbnail_url,origin) VALUES (?, ?, ?, ?, ?, ?, ?);";
        let keys = [
            artwork.artwork_uid,
            artwork.artwork_name,
            artwork.description || null,
            artwork.artist_uid,
            date,
            thumbnail,
            "portal"
        ];
        db.query(insert_artwork,keys, (err,res,fields)=>{
            if (err) {
                console.log(err);
                resolve(false);
            } else {
                console.log(res);
                resolve(true);
            }
        });
    });
}

/**
 * Inserts the artist into the table.
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
                resolve(res);
            }
            console.log(res);
        });
    });
}

/**
 * This method runs an async-for-loop over all input labels, and forms an
 * INSERT query for each (FIXME: is there a better way to bulk insert?)
 * Then, after the for-loop finishes, the promise is resolved.
 * @param  {Array} labels [description]
 * @return {Promise}      [description]
 */
insertLables = (labels,db) =>{
    console.log("==> Inserting labels");
    return new Promise((resolve, reject)=>{
        // Use an asnyc-for-loop so we know when the last loop terminates.
        eachOf(labels,(label,key,callback)=>{
            console.log(key);
            let quer = "INSERT INTO labels (uid,val,labeltype,origin)  VALUES (?, ?, ?, ?) ";
            let keys = [label.uid, label.label, label.type, label.source];
            db.query(quer, keys, (err,res,fields)=>{
                if (err) {
                    console.log("--- Error in INSERT ---:", label);
                    console.log(err);
                    callback(err);
                } else {
                    console.log(res);
                    console.log("Label ", key, " inserted Successfully.");
                    callback(); // callback fires after every callback() is called.
                }
            });
        }, (err)=>{
            if (err) {
                console.log(err);
                resolve(false);
            } else {
                console.log("finished all queries.");
                resolve(true);
            }
        });
    });
}

/**
 * TABLE: associations:
 * - label_uid
 * - object_uid  (artwork uid)
 * - object_table (artworks)
 * @param  {JSON} artwork
 * @param  {JSON} labels
 * @param  {MySQL_Connection} db
 * @return {Promise}  boolean
 */
insertAssociations = (artwork,labels,db) => {
    console.log("==> Inserting associations");
    return new Promise((resolve, reject)=>{
        // first, associate the artwork to the artist.
        eachOf(labels, (label,key,callback)=>{
            let ass = "INSERT INTO associations (label_uid, object_uid, object_table) VALUES (?, ?, ?);"
            let keys = [label.uid, artwork.artwork_uid, "artworks"];
            db.query(ass, keys, (err,res,fld)=>{
                if (err) {
                    console.log("--- Error in INSERT ---:", label);
                    console.log(err);
                    callback(err);
                } else {
                    console.log(res);
                    console.log("Association ", key, " inserted Successfully.");
                    callback(); // callback fires after every callback() is called.
                }
            });
        }, (err)=>{
            if (err) {
                console.log(err);
                resolve(false);
            } else {
                console.log("finished all queries.");
                resolve(true);
            }
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
// listenForHeld();
listenForApprovals();


let artwork = {
      "album" : "Vincent",
      "approved" : 1486308100304,
      "artist_name" : "Afika Nyati",
      "artist_uid" : "x4hhJGNPx9g3jH2iikX60tdnn6p1",
      "artwork_name" : "The Starry Night",
      "artwork_uid" : "-KcDphAm1fx6U6CYzYtr",
      "colors" : [ {
        "density" : 0.34575,
        "hex" : "#566f88",
        "w3c" : {
          "hex" : "#708090",
          "name" : "SlateGray"
        }
      }, {
        "density" : 0.13825,
        "hex" : "#28345a",
        "w3c" : {
          "hex" : "#483d8b",
          "name" : "DarkSlateBlue"
        }
      }, {
        "density" : 0.177,
        "hex" : "#2c3d89",
        "w3c" : {
          "hex" : "#483d8b",
          "name" : "DarkSlateBlue"
        }
      }, {
        "density" : 0.02325,
        "hex" : "#98994f",
        "w3c" : {
          "hex" : "#bdb76b",
          "name" : "DarkKhaki"
        }
      }, {
        "density" : 0.183,
        "hex" : "#222523",
        "w3c" : {
          "hex" : "#000000",
          "name" : "Black"
        }
      }, {
        "density" : 0.13275,
        "hex" : "#8f9a8a",
        "w3c" : {
          "hex" : "#a9a9a9",
          "name" : "DarkGray"
        }
      } ],
      "description" : "This is a collection of famous Van Gogh oil paintings.",
      "memo" : "Your use of thick brush strokes to evoke movement, combined with your rich color choices result in a really beautiful artwork!",
      "new_message" : true,
      "reviewer" : "Afika",
      "size" : 345814,
      "status" : "Approved",
      "submitted" : "2017-02-05T15:19:43.674Z",
      "tags" : [ {
        "id" : 1,
        "text" : "pattern"
      }, {
        "id" : 2,
        "text" : "art"
      }, {
        "id" : 3,
        "text" : "abstract"
      }, {
        "id" : 4,
        "text" : "painting"
      }, {
        "id" : 5,
        "text" : "design"
      }, {
        "id" : 6,
        "text" : "illustration"
      }, {
        "id" : 7,
        "text" : "desktop"
      }, {
        "id" : 8,
        "text" : "texture"
      }, {
        "id" : 9,
        "text" : "nature"
      }, {
        "id" : 10,
        "text" : "water"
      }, {
        "id" : 11,
        "text" : "color"
      }, {
        "id" : 12,
        "text" : "image"
      }, {
        "id" : 13,
        "text" : "artistic"
      }, {
        "id" : 14,
        "text" : "wallpaper"
      }, {
        "id" : 15,
        "text" : "canvas"
      }, {
        "id" : 16,
        "text" : "no person"
      } ],
      "upload_date" : "2017-02-05T15:14:08.692Z",
      "year" : 2017
};


// ---- Use these lines to communicate directly to the DB. -----

// let db = connectSQL();
// let query = "SELECT * FROM artworks WHERE uid='-KRBhVSafp6JNq5aiRMH';";
// db.query(query, (err,res,fld)=>{
//     console.log(res);
// });
