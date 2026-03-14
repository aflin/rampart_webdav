rampart.globalize(rampart.utils);
var crypto = require("rampart-crypto");
var Lmdb = require("rampart-lmdb");

var sample_env = {
    "SHELL":     "/bin/bash",
    "COLORTERM": "truecolor",
    "LANG":      "en_US.UTF-8",
    "TERM":      "xterm-256color",
    "TERMCAP":   ""
}

/* ── Session validation (mirrors webdav.js session logic) ── */

var SESSION_COOKIE_NAME = 'dav_session';
var DEFAULT_SESSION_SECONDS = 7200;

function findDataRoot() {
    var p = process.scriptPath;
    while (p.length > 1) {
        p = p.replace(/\/[^\/]+\/?$/, '');
        if (stat(p + '/web_server_conf.js')) break;
    }
    if (p.length < 2) return null;
    return stat(p + '/data') ? p + '/data' : null;
}

var dataRoot = (global.serverConf && global.serverConf.dataRoot)
    ? serverConf.dataRoot : findDataRoot();

var db, userDbi;
if (dataRoot) {
    db = new Lmdb.init(dataRoot + '/webdav_meta', true, {conversion: 'JSON', mapSize: 64});
    userDbi = db.openDb("users", true);
}

function validateSession(req) {
    if (!db || !req.cookies) return null;
    var cookieVal = req.cookies[SESSION_COOKIE_NAME];
    if (!cookieVal) return null;

    var colonIdx = cookieVal.indexOf(':');
    if (colonIdx <= 0) return null;
    var username = cookieVal.substring(0, colonIdx);
    var b64 = cookieVal.substring(colonIdx + 1);

    var userRecord = db.get(userDbi, username);
    if (!userRecord || !userRecord.sessionKey) return null;

    try {
        var encrypted = sprintf("%!B", b64);
        var decrypted = crypto.decrypt({pass: userRecord.sessionKey, data: encrypted, cipher: 'aes-256-cbc'});
        var payload = JSON.parse(bufferToString(decrypted));
        if (payload.u !== username) return null;
        if (payload.e !== 0 && payload.e < Date.now()) return null;
    } catch(e) {
        return null;
    }

    if (!userRecord.terminal) return null;

    return username;
}

/* ── WebSocket handler ── */

var allowRoot=false;
module.exports = function (req)
{
    req.cols=80;
    req.rows=40;

    /* first run, req.body is empty */
    if (!req.count) {

        // Authenticate: validate session cookie and terminal privilege
        var termUser = validateSession(req);
        if (!termUser) {
            req.wsSend("Terminal access denied. Please log in with terminal privileges.\r\n");
            req.wsEnd();
            return;
        }

        // fork a new login, place object in req where we can get at
        // it in subsequent websocket messages from this client
        try {
                var host = req.params.host || 'localhost';
                req.con = forkpty("/usr/bin/ssh", '-o', 'PubkeyAuthentication=no', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '--', host);
        } catch(e) {
            req.wsSend( "Something bad happened to forkpty, seek higher ground quickly." );
            req.wsEnd();
        }

        req.wsOnDisconnect(function(){
            if(req.con.close) //if not already closed
                req.con.close();
        });

        // what to do when we have data waiting to go.
        req.con.on("data", function(){
            //read data and send it (req.con.read returns a buffer, so it is sent as binary).
            req.wsSend(req.con.read());
        });

        req.con.resize(req.cols,req.rows);

        return;
    }

    function docmd(c){
        var jcmd;
        try {
            // convert to string and parse JSON
            jcmd = JSON.parse(sprintf("%s", c));
        } catch(e) { /* typeof jcmd == undefined */ }
        if(typeof jcmd == 'object') {
            // currently only recognizes one command (resize)
            if(typeof jcmd.resize == 'object' ) {
                var r=jcmd.resize
                if (typeof r.cols == 'number' ) req.cols = r.cols;
                if (typeof r.rows == 'number' ) req.rows = r.rows;
                req.con.resize(req.cols,req.rows);
            }
        } else {
            fprintf(stderr, "Warning: terminal.js: failed to process JSON command in '%s'\n", c);
        }
    }


    // second and subsequent run.

    if(req.body.length)
    {
        // if pty proc has exited, write and other pty methods will be undefined
        if(!req.con.write) {
            req.wsSend(stringToBuffer("The shell is not communicating.\r\n"));
            return;
        }
        // if plain text message, direct it to pty
        if(!req.wsIsBin)
            req.con.write(req.body);
        // if binary, treat it as a command
        else
            docmd(req.body);
    }

    return;
}
