/*
 * WebSocket-to-VNC TCP Bridge
 *
 * Accepts WebSocket connections from noVNC and bridges
 * them to a VNC server over TCP. Replaces websockify.
 *
 * Query parameters:
 *   host  — VNC server hostname (default: localhost)
 *   port  — VNC server port (default: 5900)
 */

rampart.globalize(rampart.utils);
var net = require("rampart-net");
var crypto = require("rampart-crypto");
var Lmdb = require("rampart-lmdb");

/* ── Session validation (mirrors webdav.js session logic) ── */

var SESSION_COOKIE_NAME = 'dav_session';

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
    db = new Lmdb.init(dataRoot + '/webdav_meta', true,
        {conversion: 'JSON', mapSize: 64});
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
        var decrypted = crypto.decrypt({
            pass: userRecord.sessionKey,
            data: encrypted, cipher: 'aes-256-cbc'
        });
        var payload = JSON.parse(bufferToString(decrypted));
        if (payload.u !== username) return null;
        if (payload.e !== 0 && payload.e < Date.now())
            return null;
        return { username: username, admin: !!userRecord.admin, vnc: !!userRecord.vnc };
    } catch(e) {
        return null;
    }
}

/* ── WebSocket handler ── */

module.exports = function(req) {

    // First message — set up the TCP connection
    if (!req.count) {

        // Authenticate
        var user = validateSession(req);
        if (!user) {
            req.wsSend(stringToBuffer("ERR:VNC access denied. Please log in."));
            req.wsEnd();
            return;
        }
        if (!user.vnc && !user.admin) {
            req.wsSend(stringToBuffer("ERR:VNC access not enabled for this account."));
            req.wsEnd();
            return;
        }

        var host = req.params.host || 'localhost';
        var port = parseInt(req.params.port) || 5900;

        // TODO: consider restricting non-admin users to
        // localhost/private IPs only

        var sock = new net.Socket();

        sock.on("error", function(err) {
            req.wsEnd();
        });

        sock.on("close", function() {
            req.wsEnd();
        });

        sock.on("data", function(data) {
            try {
                req.wsSend(data);
            } catch(e) {}
        });

        sock.connect(port, host);

        req.sock = sock;

        req.wsOnDisconnect(function() {
            if (req.sock) {
                try { req.sock.destroy(); } catch(e) {}
                req.sock = null;
            }
        });

        return;
    }

    // Subsequent messages — forward to VNC server
    if (req.body && req.body.length && req.sock) {
        try {
            // Ensure we send binary data to the TCP socket
            var data = req.wsIsBin ? req.body : stringToBuffer(req.body);
            req.sock.write(data);
        } catch(e) {
            req.wsEnd();
        }
    }
};
