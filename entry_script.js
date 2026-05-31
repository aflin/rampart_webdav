#!/usr/bin/env rampart
/* ============================================================
 * Rampart File Manager — bundled entry script
 *
 * On first run (no filemanager-config.json next to this binary):
 *   1. Interactive setup: data directory, HTTP/HTTPS choice, port,
 *      drop-to user when started as root.
 *   2. Copy html/, apps/, wsapps/ out of the bundle into the chosen
 *      data directory so the user can customise them.
 *   3. Create <chosen>/data/webdav_root/server_root → <chosen>
 *      symlink, and add <chosen> to webdav's extpaths allow-list.
 *   4. Create the first admin user in the webdav LMDB store.
 *   5. Write filemanager-config.json next to the bundle binary.
 *   6. Start the server.
 *
 * On later runs:
 *   - Load filemanager-config.json, hand the config to rampart-webserver.
 *   - All standard CLI flags (--port, --bindAll, --user, --secure, --letsencrypt,
 *     --sslKeyFile, --sslCertFile, --selfSign, etc.) are processed by
 *     rampart-webserver itself and override the saved defaults.
 *   - If the file is missing keys / malformed, the script refuses to start
 *     and asks the user to fix or delete it (per requirements).
 * ============================================================ */

'use strict';

var u      = rampart.utils;
var crypto = require('rampart-crypto');
var Lmdb   = require('rampart-lmdb');

var stat        = u.stat;
var lstat       = u.lstat;
var mkdir       = u.mkdir;
var readFile    = u.readFile;
var fopen       = u.fopen;
var rmFile      = u.rmFile;
var readdir     = u.readdir;
var symlink     = u.symlink;
var realPath    = u.realPath;
var shell       = u.shell;
var readLine    = u.readLine;
var stdin       = u.stdin;
var stdout      = u.stdout;
var stderr      = u.stderr;
var printf      = u.printf;
var fprintf     = u.fprintf;
var bufferToString = u.bufferToString;

function die(msg) { fprintf(stderr, '%s\n', msg); process.exit(1); }

/* ---------- small interactive prompt helpers ---------- */

function ask(prompt, def) {
    if (def !== undefined && def !== null && def !== '') {
        printf('%s [%s]: ', prompt, def);
    } else {
        printf('%s: ', prompt);
    }
    stdout.fflush();
    var v = readLine(stdin).next();
    if (v === undefined || v === null) { printf('\n'); process.exit(130); }
    v = String(v).replace(/[\r\n]+$/, '').trim();
    if (!v && def !== undefined && def !== null) return String(def);
    return v;
}

function askPassword(prompt) {
    printf('%s: ', prompt);
    stdout.fflush();
    try { shell('stty -echo'); } catch(_) {}
    var v = readLine(stdin).next();
    try { shell('stty echo'); } catch(_) {}
    printf('\n');
    if (v === undefined || v === null) process.exit(130);
    return String(v).replace(/[\r\n]+$/, '');
}

function chooseFromList(prompt, choices, defIdx) {
    printf('%s\n', prompt);
    for (var i = 0; i < choices.length; i++) {
        printf('  %d) %s%s\n', i+1, choices[i], i === defIdx ? '  (default)' : '');
    }
    while (true) {
        var ans = ask('Enter number', defIdx !== undefined ? String(defIdx+1) : '');
        var n = parseInt(ans, 10);
        if (n >= 1 && n <= choices.length) return n - 1;
        printf('Please enter a number between 1 and %d.\n', choices.length);
    }
}

/* ---------- system helpers ---------- */

function whoami() {
    try { return String(shell('whoami').stdout).trim(); } catch(_) { return ''; }
}

function isRoot() { return whoami() === 'root'; }

function userExists(name) {
    if (!name) return false;
    try {
        var res = shell('id -u ' + JSON.stringify(name) + ' 2>/dev/null');
        return /^\d+$/.test(String(res.stdout).trim());
    } catch(_) { return false; }
}

// Plausible "real" users — directories under /home (Linux) or /Users (macOS).
function listHumanUsers() {
    var dir = stat('/home') ? '/home' : (stat('/Users') ? '/Users' : null);
    if (!dir) return [];
    var users = [];
    try {
        var entries = readdir(dir);
        for (var i = 0; i < entries.length; i++) {
            var name = entries[i];
            if (name === '.' || name === '..') continue;
            if (name.charAt(0) === '.') continue;
            if (name === 'Shared' || name === 'lost+found') continue;
            var st = stat(dir + '/' + name);
            if (st && st.isDirectory) users.push(name);
        }
    } catch(_) {}
    users.sort();
    return users;
}

/* ---------- locate the bundle binary so config.json lives beside it ---------- */

function resolveBundlePath() {
    var p = process.argv[0] || '';
    if (p && p.indexOf('/') !== -1) {
        try { return realPath(p) || p; } catch(_) { return p; }
    }
    // Bare name (PATH lookup) → ask the kernel.
    try { var rp = realPath('/proc/self/exe'); if (rp) return rp; } catch(_) {}
    try {
        var which = String(shell('command -v ' + JSON.stringify(p) + ' 2>/dev/null').stdout).trim();
        if (which) { try { return realPath(which) || which; } catch(_) { return which; } }
    } catch(_) {}
    return p;
}

var BUNDLE_PATH = resolveBundlePath();
var BUNDLE_DIR  = (BUNDLE_PATH.replace(/\/[^\/]+$/, '') || '.');
var CONFIG_PATH = BUNDLE_DIR + '/filemanager-conf.js';

/* ---------- payload extraction ----------
 * Works whether or not we're inside a bundle. During development the script
 * can be run directly from disk and we'll just copy from the source tree. */

function isBundled() { return typeof u.payloadGet === 'function'; }

// Extract a subtree from the bundle (or copy from disk during development)
// into `dest`. Caller passes the in-zip path (':zip:/html') and the on-disk
// destination — the destination's basename MUST match the zip subdir name
// (e.g. ':zip:/html' → '<...>/html'), since payloadExtract preserves the
// path stored in the zip entry.
function extractTree(zipPrefix, dest) {
    var subdir = zipPrefix.replace(/^:zip:\/?/, '').replace(/\/$/, '');
    if (!isBundled()) {
        // Dev mode — copy from the source tree
        var src = BUNDLE_DIR + '/' + subdir;
        if (!stat(src)) die('Source tree missing: ' + src);
        mkdir(dest, true);
        var res = shell('cp -a ' + JSON.stringify(src + '/.') + ' ' + JSON.stringify(dest) + '/');
        if (res && res.exitStatus !== 0) die('cp -a failed: ' + (res.stderr || ''));
        return;
    }
    // Bundle mode — payloadExtract(destPath, [filter]) writes entries under
    // destPath, preserving their in-zip path. So if zip has 'html/...' and we
    // pass destPath = parent dir, files land at parent/html/... which is
    // exactly `dest` when dest = parent + '/' + subdir.
    var parent = dest.replace(/\/[^\/]+\/?$/, '') || '/';
    mkdir(parent, true);
    u.payloadExtract(parent, [subdir]);
    if (!stat(dest)) die('Extraction failed: ' + dest + ' was not created');
}

/* ---------- LMDB: create first admin + allow-list the symlink target ---------- */

function setupLmdb(dataDir, adminUser, adminPw) {
    var dbPath = dataDir + '/data/webdav_meta';
    if (!stat(dbPath)) mkdir(dbPath, true);

    // Match webdav.js's open options so the second init in webdav.js reuses
    // the same env. (rampart-lmdb caches per-path handles.)
    var db = new Lmdb.init(dbPath, true, { conversion: 'JSON', mapSize: 256, growOnPut: true });
    var userDbi  = db.openDb('users',    true);
    var groupDbi = db.openDb('groups',   true);
    var extpDbi  = db.openDb('extpaths', true);

    if (!db.get(groupDbi, 'everyone')) {
        db.put(groupDbi, 'everyone', { name: 'everyone', created: new Date().toISOString() });
    }

    var hashed = crypto.passwd(adminPw, null, 'sha512');
    db.put(userDbi, adminUser, {
        hash_line: hashed.line,
        admin:     true,
        created:   new Date().toISOString(),
        groups:    [],
        // First admin gets terminal + remote-desktop access by default.
        terminal:  true,
        remote:    true
    });

    // Add chosen dir to extpaths so the server_root symlink is honoured by
    // webdav.js's checkAllowedPath().
    db.put(extpDbi, 'server_root', { path: dataDir });

    // Make the admin's home directory under webdav_root so first login lands cleanly.
    var homeDir = dataDir + '/data/webdav_root/' + adminUser;
    if (!stat(homeDir)) mkdir(homeDir, true);
}

/* ---------- writability probe for the bundle directory ---------- */

function ensureBundleDirWritable() {
    var probe = BUNDLE_DIR + '/.rfm-write-test-' + process.pid + '-' + Date.now();
    try {
        var fh = fopen(probe, 'w');
        fh.fclose();
        rmFile(probe);
    } catch(e) {
        die('Cannot write config to ' + BUNDLE_DIR + ': ' + (e.message || e) +
            '\nMove the bundle to a directory you own, or run as a user with write access there.');
    }
}

/* ---------- interactive first-run setup ---------- */

function runSetupWizard() {
    var root = isRoot();
    var sudoUser = process.env['SUDO_USER'] || '';

    printf('\n=== Rampart File Manager — first-time setup ===\n\n');
    if (root) {
        printf('Running as root — any port (including 80 / 443) is available.\n');
    } else {
        printf('Running as a regular user.\n');
        printf('Ports below 1024 (e.g. 80 / 443) require root; exit and re-run with sudo if you need them.\n');
    }
    printf('\n');

    ensureBundleDirWritable();

    // 1. Data / logs directory — default to <cwd>/web_server so a fresh
    // invocation drops its state next to where the user ran the bundle from.
    var cwd;
    try { cwd = u.getcwd(); } catch(_) { cwd = process.env.PWD || '.'; }
    var defDir = cwd.replace(/\/+$/, '') + '/web_server';
    var dataDir;
    while (true) {
        dataDir = ask('Data and logs directory', defDir);
        dataDir = dataDir.replace(/\/+$/, '');
        if (!dataDir) { printf('Required.\n'); continue; }
        if (dataDir.charAt(0) !== '/') { printf('Please give an absolute path.\n'); continue; }
        break;
    }

    // 2. HTTP or HTTPS
    var proto = chooseFromList('Protocol:', ['HTTP (plain)', 'HTTPS (TLS)'], 0);
    var useHttps = (proto === 1);

    // 2b. TLS source
    var tls = null;
    if (useHttps) {
        var src = chooseFromList('TLS certificate source:', [
            "Let's Encrypt (auto-renew; initial cert must already be issued)",
            'Self-signed (rampart generates a cert on first start)',
            'Existing key and cert files on disk'
        ], 0);
        if (src === 0) {
            var host;
            while (true) {
                host = ask("Hostname for Let's Encrypt (e.g. files.example.com)");
                if (host) break;
                printf('Hostname required.\n');
            }
            tls = { mode: 'letsencrypt', host: host };
        } else if (src === 1) {
            tls = { mode: 'selfsign' };
        } else {
            var keyFile, certFile;
            while (true) {
                keyFile = ask('Path to TLS key file (PEM)');
                if (keyFile && stat(keyFile)) break;
                printf('File not found: %s\n', keyFile);
            }
            while (true) {
                certFile = ask('Path to TLS certificate file (PEM)');
                if (certFile && stat(certFile)) break;
                printf('File not found: %s\n', certFile);
            }
            tls = { mode: 'files', keyFile: keyFile, certFile: certFile };
        }
    }

    // 3. Port
    var defPort = root ? (useHttps ? 443 : 80)
                       : (useHttps ? 8443 : 8080);
    var port;
    while (true) {
        var pv = ask('Port', String(defPort));
        port = parseInt(pv, 10);
        if (port >= 1 && port <= 65535) break;
        printf('Invalid port.\n');
    }

    // 4. Drop-to user when running as root
    var dropUser = null;
    if (root) {
        var users = listHumanUsers();
        if (sudoUser && users.indexOf(sudoUser) === -1) users.unshift(sudoUser);
        var defIdx = sudoUser ? Math.max(0, users.indexOf(sudoUser)) : 0;
        users.push('(enter another username)');
        var idx = chooseFromList(
            'User to drop privileges to after binding the port:',
            users,
            defIdx
        );
        if (idx === users.length - 1) {
            while (true) {
                dropUser = ask('Username');
                if (dropUser && userExists(dropUser)) break;
                printf('User "%s" not found.\n', dropUser);
            }
        } else {
            dropUser = users[idx];
            if (!userExists(dropUser)) die('User "' + dropUser + '" does not exist.');
        }
    }

    // 5. First admin user for the file manager
    printf('\nNow create the first administrator account for the file manager itself.\n');
    var adminUser, adminPw;
    while (true) {
        adminUser = ask('Admin username', sudoUser || dropUser || '');
        if (/^[a-zA-Z0-9_-]{1,32}$/.test(adminUser)) break;
        printf('Usernames must be 1-32 chars: letters, digits, underscore, hyphen.\n');
    }
    while (true) {
        adminPw = askPassword('Admin password (min 7 chars)');
        if (adminPw.length < 7) { printf('Too short.\n'); continue; }
        var pw2 = askPassword('Confirm password');
        if (adminPw !== pw2) { printf('Passwords do not match.\n'); continue; }
        break;
    }

    // 6. Build directory tree + copy assets out of the bundle
    printf('\nSetting up directories...\n');
    try { mkdir(dataDir, true); } catch(e) { die('Failed to create ' + dataDir + ': ' + (e.message||e)); }
    mkdir(dataDir + '/logs', true);
    mkdir(dataDir + '/data', true);
    mkdir(dataDir + '/data/webdav_root', true);

    printf('Extracting assets:\n');
    printf('  → %s/html\n',   dataDir);   extractTree(':zip:/html',   dataDir + '/html');
    printf('  → %s/apps\n',   dataDir);   extractTree(':zip:/apps',   dataDir + '/apps');
    printf('  → %s/wsapps\n', dataDir);   extractTree(':zip:/wsapps', dataDir + '/wsapps');

    // 7. Symlink server_root → chosen dir
    var srLink = dataDir + '/data/webdav_root/server_root';
    if (lstat(srLink)) { try { rmFile(srLink); } catch(_) {} }
    try { symlink({ src: dataDir, target: srLink }); }
    catch(e) { die('Failed to create symlink ' + srLink + ' → ' + dataDir + ': ' + (e.message||e)); }

    // 8. LMDB setup
    setupLmdb(dataDir, adminUser, adminPw);

    // 9. If running as root and a drop-to user was chosen, chown the data dir
    //    so the lower-privilege user can write into it after privsep.
    if (root && dropUser) {
        try { shell('chown -R ' + JSON.stringify(dropUser) + ' ' + JSON.stringify(dataDir)); }
        catch(e) { fprintf(stderr, 'Warning: chown ' + dataDir + ' to ' + dropUser + ' failed: ' + e.message + '\n'); }
    }

    // 10. Persist config as a JS module
    writeServerConfFile(CONFIG_PATH, {
        dataDir:  dataDir,
        useHttps: useHttps,
        port:     port,
        tls:      tls,
        dropUser: dropUser
    });

    printf('\nSetup complete. Config saved to %s\n', CONFIG_PATH);
    printf('Starting server...\n\n');
    return CONFIG_PATH;
}

/* ---------- generate web_server_conf.js ---------- */

// Body of commented-out defaults appended after the active settings.
// Modeled on rampart's web_server_conf.js template (demo-mode + OnlyOffice
// blocks removed; the only-relevant-here `appendMap` is set above as active).
var SERVER_CONF_DEFAULTS_BLOCK = [
    '    /* ---- Defaults below.  Uncomment any line to override. ---- */',
    '',
    '    /* ipAddr              String. The ipv4 address to bind. */',
    "    //ipAddr:              '127.0.0.1',",
    '',
    '    /* ipv6Addr            String. The ipv6 address to bind. */',
    "    //ipv6Addr:            '[::1]',",
    '',
    '    /* bindAll             Bool.   Set ipAddr and ipv6Addr to 0.0.0.0 and [::] respectively. */',
    '    //bindAll:             false,',
    '',
    '    /* ipPort              Number. Set ipv4 port. */',
    '    //ipPort:              8088,',
    '',
    '    /* ipv6Port            Number. Set ipv6 port. */',
    '    //ipv6Port:            8088,',
    '',
    '    /* port                Number. Set both ipv4 and ipv6 port if > -1. */',
    '    //port:                -1,',
    '',
    "    /* htmlRoot            String. Root directory from which to serve files. Default: serverRoot + '/html'. */",
    "    //htmlRoot:            working_directory + '/html',",
    '',
    "    /* appsRoot            String. Root directory from which to serve apps.  Default: serverRoot + '/apps'. */",
    "    //appsRoot:            working_directory + '/apps',",
    '',
    "    /* wsappsRoot          String. Root directory from which to serve websocket apps.  Default: serverRoot + '/wsapps'. */",
    "    //wsappsRoot:          working_directory + '/wsapps',",
    '',
    "    /* dataRoot            String. Setting for user scripts (LMDB, uploads, etc.). Default: serverRoot + '/data'. */",
    "    //dataRoot:            working_directory + '/data',",
    '',
    "    /* logRoot             String. Log directory. Default: serverRoot + '/logs'. */",
    "    //logRoot:             working_directory + '/logs',",
    '',
    '    /* redirPort           Number. Launch http->https redirect server and set port if < -1. */',
    '    //redirPort:           -1,',
    '',
    '    /* redir               Bool.   Launch http->https redirect server on port 80. */',
    '    //redir:               false,',
    '',
    '    /* redirTemp           Bool. If redirecting, send 302 Moved Temporarily instead of 301. */',
    '    //redirTemp:           false,',
    '',
    "    /* accessLog           String. Log file name or '' for stdout. */",
    "    //accessLog:           working_directory + '/logs/access.log',",
    '',
    "    /* errorLog            String. Error log file name or '' for stderr. */",
    "    //errorLog:            working_directory + '/logs/error.log',",
    '',
    '    /* log                 Bool.   Whether to log requests and errors. */',
    '    //log:                 true,',
    '',
    '    /* rotateLogs          Bool.   Whether to rotate the logs. */',
    '    //rotateLogs:          false,',
    '',
    "    /* rotateStart         String. Time to start log rotations. */",
    "    //rotateStart:         '00:00',",
    '',
    "    /* rotateInterval      Number. Interval between log rotations in seconds, or string 'hourly'/'daily'/'weekly'. */",
    '    //rotateInterval:      86400,',
    '',
    '    /* rotateCount         Number. Number of rotated logs to keep. */',
    '    //rotateCount:         30,',
    '',
    '    /* user                String. If started as root, switch to this user.',
    '                                   Required to start as root if using ports < 1024. */',
    "    //user:                'nobody',",
    '',
    '    /* threads             Number. Limit threads used by the server.  Default (-1) = CPU cores. */',
    '    //threads:             -1,',
    '',
    '    /* secure              Bool.   Whether to use https.  sslKeyFile and sslCertFile must be set. */',
    '    //secure:              false,',
    '',
    '    /* sslKeyFile          String. https TLS key file location. */',
    "    //sslKeyFile:          '',",
    '',
    '    /* sslCertFile         String. https TLS cert file location. */',
    "    //sslCertFile:         '',",
    '',
    '    /* selfSign            Bool.   Generate and use a self-signed cert.',
    '                                   If set, secure must be true and sslKeyFile/sslCertFile/letsencrypt must be unset. */',
    '    //selfSign:            false,',
    '',
    "    /* letsencrypt         String. Domain name for automatic Let's Encrypt setup; sets secure=true and looks",
    "                                   for /etc/letsencrypt/live/<domain>/ to set sslKeyFile + sslCertFile. */",
    "    //letsencrypt:         '',",
    '',
    '    /* developerMode       Bool.   JS errors return 500 with stack trace.  Otherwise 404. */',
    '    //developerMode:       true,',
    '',
    '    /* directoryFunc       Bool.   Provide a directory listing if no index.html is found. */',
    '    //directoryFunc:       false,',
    '',
    '    /* daemon              Bool.   Detach from terminal and run as a daemon. */',
    '    //daemon:              true,',
    '',
    '    /* monitor             Bool.   Launch monitor process to auto-restart server on crash. */',
    '    //monitor:             false,',
    '',
    '    /* scriptTimeout       Number. Max time (sec) for a script module to return. Default 20. */',
    '    //scriptTimeout:       20,',
    '',
    '    /* connectTimeout      Number. Max time (sec) for client to send request. Default 20. */',
    '    //connectTimeout:      20,',
    '',
    '    /* maxBodySize         Number. Max request body in bytes. Default 52428800 (50MB). */',
    '    //maxBodySize:         52428800,',
    '',
    '    /* defaultRangeMBytes  Number. Default chunk size in MB for "Range: bytes=N-" open-ended requests',
    '                                   (often used for seeking into videos). Range: 0.01 to 1000. */',
    '    //defaultRangeMBytes:  8,',
    '',
    '    /* appendProcTitle     Bool.   Append ip:port to process name as seen in ps. */',
    '    //appendProcTitle:     false,',
    '',
    "    /* beginFunc           Bool/Obj/Function. Function to run at the start of each JS function or file load.",
    "                           e.g. beginFunc: {module: working_directory + '/apps/beginfunc.js'} */",
    '    //beginFunc:           false,',
    '',
    '    /* beginFuncOnFile     Bool. Whether to run beginFunc before serving static files. */',
    '    //beginFuncOnFile:     false,',
    '',
    '    /* endFunc             Function to run after each JS callback.  See beginFunc. */',
    '    //endFunc:             false,',
    '',
    '    /* logFunc             Function to replace normal logging when log:true. */',
    '    //logFunc:             false,',
    ''
].join('\n');

function writeServerConfFile(path, opts) {
    var dataDir  = opts.dataDir;
    var useHttps = opts.useHttps;
    var port     = opts.port;
    var tls      = opts.tls || null;
    var dropUser = opts.dropUser || null;

    // Active TLS block — only the one chosen at setup is uncommented.
    // The other modes are emitted as commented examples so the user can flip.
    var activeTls = '';
    if (!useHttps) {
        activeTls = '    /* HTTP only (no TLS). */\n';
    } else if (tls && tls.mode === 'letsencrypt') {
        activeTls = '    secure:      true,\n' +
                    '    letsencrypt: ' + JSON.stringify(tls.host) + ',\n';
    } else if (tls && tls.mode === 'selfsign') {
        activeTls = '    secure:      true,\n' +
                    '    selfSign:    true,\n';
    } else if (tls && tls.mode === 'files') {
        activeTls = '    secure:      true,\n' +
                    '    sslKeyFile:  ' + JSON.stringify(tls.keyFile)  + ',\n' +
                    '    sslCertFile: ' + JSON.stringify(tls.certFile) + ',\n';
    }
    var userLine = dropUser ? '    user:        ' + JSON.stringify(dropUser) + ',\n' : '';

    // Commented alternative TLS examples (helpful when the user wants to switch later).
    var tlsExamples = [
        '',
        '    /* Other TLS modes you can switch to (uncomment one block, comment out the active one above): */',
        '',
        "    /* Let's Encrypt (auto-renew; assumes initial cert already issued):",
        '    secure:      true,',
        "    letsencrypt: 'example.com',",
        '    */',
        '',
        '    /* Self-signed (rampart generates a cert on first start):',
        '    secure:      true,',
        '    selfSign:    true,',
        '    */',
        '',
        '    /* Your own key/cert files:',
        '    secure:      true,',
        "    sslKeyFile:  '/path/to/key.pem',",
        "    sslCertFile: '/path/to/cert.pem',",
        '    */',
        ''
    ].join('\n');

    var content =
        '/* Rampart File Manager — server configuration.\n' +
        ' *\n' +
        ' * Generated by the bundle on first-run setup. Edit freely; subsequent\n' +
        ' * runs will not overwrite this file. To reconfigure from scratch,\n' +
        ' * delete this file and re-run the bundle.\n' +
        ' *\n' +
        ' * This file is require()\'d by the bundled entry script and the exported\n' +
        ' * `serverConf` is passed to rampart-webserver.web_server_conf(). Command-line\n' +
        ' * flags (--port, --bindAll, --user, --secure, --sslKeyFile, --sslCertFile,\n' +
        ' * --selfSign, --letsencrypt, etc.) still override values set here.\n' +
        ' */\n' +
        '\n' +
        'var working_directory = ' + JSON.stringify(dataDir) + ';\n' +
        '\n' +
        'var serverConf = {\n' +
        '    /* ---- File manager wiring (the WebDAV endpoint for the file manager UI) ---- */\n' +
        '    appendMap: {\n' +
        '        "/dav/": {module: working_directory + \'/apps/webdav/webdav.js\'}\n' +
        '    },\n' +
        '    serverRoot: working_directory,\n' +
        '\n' +
        '    /* ---- Settings chosen at first-run setup ---- */\n' +
        '    bindAll:     true,\n' +
        '    port:        ' + port + ',\n' +
        activeTls +
        userLine +
        tlsExamples +
        SERVER_CONF_DEFAULTS_BLOCK +
        '};\n' +
        '\n' +
        'module.exports = serverConf;\n';

    try {
        var fh = fopen(path, 'w');
        fh.fprintf('%s', content);
        fh.fclose();
    } catch(e) {
        die('Failed to write ' + path + ': ' + (e.message||e));
    }
}

/* ---------- load saved config and start the server ---------- */

function loadServerConf() {
    var serverConf;
    try { serverConf = require(CONFIG_PATH); }
    catch(e) {
        die('Failed to load ' + CONFIG_PATH + ': ' + (e.message||e) +
            '\nFix the file or delete it and re-run to start over.');
    }
    if (!serverConf || typeof serverConf !== 'object') {
        die(CONFIG_PATH + ' did not export a serverConf object. Fix it or delete it and re-run.');
    }
    if (!serverConf.serverRoot) {
        die(CONFIG_PATH + ' has no serverRoot. Fix it or delete it and re-run.');
    }
    if (!stat(serverConf.serverRoot)) {
        die('serverRoot ' + serverConf.serverRoot + ' does not exist.\nRestore it or delete ' + CONFIG_PATH + ' and re-run.');
    }
    // Make sure the dav module appendMap is present — that's how the file manager UI talks to the backend.
    if (!serverConf.appendMap || !serverConf.appendMap['/dav/']) {
        die(CONFIG_PATH + ' is missing appendMap["/dav/"] — the file manager backend will not be reachable. Fix the file or delete it and re-run.');
    }
    return serverConf;
}

function startServer() {
    var serverConf = loadServerConf();

    // Search-index lock needs to exist as a global before threads spawn — matches wsc.js.
    global.indexLock = new rampart.lock();
    global.thrlock   = new rampart.lock();

    // Make conf visible to webdav.js (it reads global.serverConf.dataRoot / .logRoot).
    global.serverConf = serverConf;

    // Hand off. rampart-webserver also parses --port / --bindAll / --user /
    // --secure / --letsencrypt / --sslKeyFile / --sslCertFile / --selfSign
    // etc. from process.argv and merges them into the conf, overriding fields here.
    require('rampart-webserver').web_server_conf(serverConf);
}

/* ---------- entrypoint ---------- */

if (!stat(CONFIG_PATH)) {
    runSetupWizard();
}
startServer();
