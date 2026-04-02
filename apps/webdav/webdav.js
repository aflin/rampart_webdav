#!/usr/bin/env rampart
/*
 * WebDAV Server Module for Rampart
 * RFC 4918 compliant, Class 2 (with locking)
 *
 * Mounted at /dav/ via appendMap in web_server_conf.js
 * Serves files from the dav/ directory under the server root.
 */

rampart.globalize(rampart.utils);
var crypto = require("rampart-crypto");
var Lmdb = require("rampart-lmdb");
var gm = require("rampart-gm");
var curl = require("rampart-curl");
var server = require("rampart-server");
var Sql = require("rampart-sql");
var totext = require("rampart-totext");
var net = require("rampart-net");

// Allow search indexing of mounted (rclone/FUSE) directories
// Set global.allowMountedSearch = true in web_server_conf.js to enable
if (global.allowMountedSearch === undefined) global.allowMountedSearch = false;

/* ============================================================
 * Section 1: Configuration & Constants
 * ============================================================ */

function findDataRoot() {
    var st, p = process.scriptPath;

    // find the server root.
    while(p.length>1) {
        p = p.replace(/\/[^\/]+\/?$/, '');
        st = stat(`${p}/web_server_conf.js`);
        if(st)
            break;
    }

    if(p.length < 2) {
        fprintf(stderr, "cannot continue, cannot find the server root directory\n");
        process.exit(1);
    }

    st = stat(`${p}/data`);

    if(!st) {
        fprintf(stderr, "cannot continue, cannot find the server 'data' directory in '${p}'\n");
        process.exit(1);
    }
    return `${p}/data`;
}


var dataRoot;
if((global.serverConf && global.serverConf.dataRoot) || global.DEMO_DATA_ROOT ) {
    dataRoot=global.DEMO_DATA_ROOT ? global.DEMO_DATA_ROOT : serverConf.dataRoot;
} else {
    //run from command line.
    global.indexLock = new rampart.lock();
    global.thrlock = new rampart.lock();
    dataRoot = findDataRoot();
}
// Resolve symlinks so paths match /proc/mounts and other system lookups
try { dataRoot = realPath(dataRoot) || dataRoot; } catch(e) {}

var DAV_ROOT  = dataRoot + '/webdav_root';
if (!stat(DAV_ROOT)) mkdir(DAV_ROOT);
try { DAV_ROOT = realPath(DAV_ROOT) || DAV_ROOT; } catch(e) {}
var DAV_ROOT_DEV = stat(DAV_ROOT).dev;

if(module && module.exports) {
    var DAV_PREFIX = '/dav';
    var LOCK_KEY  = 'webdav_locks';

    var DAV_ROOT_RESOLVED = realPath(DAV_ROOT);
}

function checkAllowedPath(fsPath) {
    var resolved;
    try { resolved = realPath(fsPath); } catch(e) { resolved = null; }
    if (!resolved) {
        // Path doesn't exist yet (PUT/MKCOL) — check the parent directory
        var slash = fsPath.lastIndexOf('/');
        if (slash <= 0) return false;
        try { resolved = realPath(fsPath.substring(0, slash)); } catch(e) { return false; }
        if (!resolved) return false;
    }
    // Always allow paths under DAV_ROOT
    if (resolved === DAV_ROOT_RESOLVED || resolved.indexOf(DAV_ROOT_RESOLVED + '/') === 0) {
        return true;
    }
    // Check external paths from LMDB
    var stored = db.get(extpathsDbi, "", 10000);
    if (stored && typeof stored === 'object') {
        var keys = Object.keys(stored);
        for (var i = 0; i < keys.length; i++) {
            var ep = stored[keys[i]];
            if (ep && ep.path) {
                if (ep.path === '/') return true;
                if (resolved === ep.path || resolved.indexOf(ep.path + '/') === 0) {
                    return true;
                }
            }
        }
    }
    return false;
}

// Compute a relative path from fromDir to toPath (both absolute)
function computeRelativePath(fromDir, toPath) {
    var fromParts = fromDir.replace(/\/+$/, '').split('/');
    var toParts = toPath.replace(/\/+$/, '').split('/');
    var common = 0;
    while (common < fromParts.length && common < toParts.length
           && fromParts[common] === toParts[common]) common++;
    var ups = fromParts.length - common;
    var rel = '';
    for (var i = 0; i < ups; i++) rel += '../';
    rel += toParts.slice(common).join('/');
    return rel || '.';
}

// Chunked upload temp directory
var UPLOAD_TMP = dataRoot + '/webdav_uploads';
if (!stat(UPLOAD_TMP)) mkdir(UPLOAD_TMP);

// LMDB database (users + dead properties)
var DB_PATH = dataRoot + '/webdav_meta';
var db = new Lmdb.init(DB_PATH, true, {conversion: 'JSON', mapSize: 64});
var userDbi = db.openDb("users", true);
var propsDbi = db.openDb("props", true);
var filemetaDbi = db.openDb("filemeta", true);
var extpathsDbi = db.openDb("extpaths", true);
var rcloneDbi = db.openDb("rclone", true);         // Per-user rclone mount configs
var groupDbi = db.openDb("groups", true);          // key=groupname, value={name, created}
var sharesDbi = db.openDb("shares", true);         // key=token, value={path, owner, created, expires, isDir}

// Ensure the built-in "everyone" group exists
if (!db.get(groupDbi, 'everyone')) {
    db.put(groupDbi, 'everyone', { name: 'everyone', created: new Date().toISOString() });
}

// ---- Demo Mode Initialization ----
var DEMO_MODE = !!global.DEMO_MODE;
var DEMO_MAX_FILE_SIZE = global.DEMO_MAX_FILE_SIZE || 50*1024*1024;
var DEMO_MAX_QUOTA = global.DEMO_MAX_QUOTA || 500*1024*1024;
var DEMO_CLEAR_TIME = global.DEMO_CLEAR_TIME || 600;
var DEMO_FILES_DIR = 'demo-files';  // read-only showcase directory name

if (DEMO_MODE) {
    // Ensure demo user exists
    var demoRecord = db.get(userDbi, 'demo');
    if (!demoRecord) {
        var demoHash = crypto.passwd('demo', null, 'sha512');
        demoRecord = {
            hash_line: demoHash.line,
            admin: false,
            created: new Date().toISOString(),
            groups: ['everyone'],
            terminal: false
        };
        db.put(userDbi, 'demo', demoRecord);
    }
    // Ensure demo home directory
    var demoHome = DAV_ROOT + '/demo';
    if (!stat(demoHome)) mkdir(demoHome);
    var demoSubDirs = ['Documents', 'Music', 'Pictures', 'Videos'];
    for (var _di = 0; _di < demoSubDirs.length; _di++) {
        var _dd = demoHome + '/' + demoSubDirs[_di];
        if (!stat(_dd)) mkdir(_dd);
    }
    // Ensure demo-files read-only directory at DAV root level
    var demoFilesPath = DAV_ROOT + '/' + DEMO_FILES_DIR;
    if (!stat(demoFilesPath)) mkdir(demoFilesPath);
}

function _addExternalPath(resolvedPath) {
    db.put(extpathsDbi, resolvedPath, { path: resolvedPath, added: new Date().toISOString() });
}

function _removeExternalPath(resolvedPath) {
    db.del(extpathsDbi, resolvedPath);
}

// Thumbnail directory (shadow tree mirroring WebDAV paths)
var THUMB_DIR = dataRoot + '/webdav_thumbnails';
// Wipe old hash-based thumbnail directory and recreate
if (stat(THUMB_DIR)) {
    // Check for old hash-based layout (single-char subdirs like /a/, /b/)
    var _thumbEntries = readdir(THUMB_DIR);
    if (_thumbEntries && _thumbEntries.length > 0 && _thumbEntries[0].length <= 2) {
        rmdirRecursive(THUMB_DIR);
    }
}
if (!stat(THUMB_DIR)) mkdir(THUMB_DIR);
var THUMB_IMAGE_RE = /\.(png|jpe?g|gif|webp|bmp|tiff?)$/i;
var THUMB_VIDEO_RE = /\.(mp4|webm|ogg|ogv|mkv|avi|mov)$/i;
var FFMPEG_PATH = (function() {
    try {
        var res = shell('which ffmpeg', {timeout: 2000});
        if (res.exitStatus === 0) return (res.stdout || '').trim();
    } catch(e) {}
    // which may fail in restricted PATH; check standard locations
    if (stat('/usr/bin/ffmpeg')) return '/usr/bin/ffmpeg';
    if (stat('/usr/local/bin/ffmpeg')) return '/usr/local/bin/ffmpeg';
    return null;
})();
var FFPROBE_PATH = (function() {
    try {
        var res = shell('which ffprobe', {timeout: 2000});
        if (res.exitStatus === 0) return (res.stdout || '').trim();
    } catch(e) {}
    if (stat('/usr/bin/ffprobe')) return '/usr/bin/ffprobe';
    if (stat('/usr/local/bin/ffprobe')) return '/usr/local/bin/ffprobe';
    return null;
})();
var HAS_FFMPEG = !!FFMPEG_PATH;

// Check if FUSE allow_other is available
var FUSE_ALLOW_OTHER = (function() {
    try {
        var conf = bufferToString(readFile('/etc/fuse.conf'));
        return /^\s*user_allow_other\s*$/m.test(conf);
    } catch(e) { return false; }
})();

// rclone detection and mount infrastructure
var RCLONE_DIR = dataRoot + '/.rclone';
if (!stat(RCLONE_DIR)) mkdir(RCLONE_DIR);

var HAS_RCLONE = (function() {
    try {
        var res = shell('which rclone', {timeout: 2000});
        if (res.exitStatus === 0) return true;
    } catch(e) {}
    // Fallback: check standard locations
    if (stat('/usr/bin/rclone')) return true;
    if (stat('/usr/local/bin/rclone')) return true;
    return false;
})();

var RCLONE_PATH = (function() {
    try {
        var res = shell('which rclone', {timeout: 2000});
        if (res.exitStatus === 0) return res.stdout.trim();
    } catch(e) {}
    if (stat('/usr/bin/rclone')) return '/usr/bin/rclone';
    if (stat('/usr/local/bin/rclone')) return '/usr/local/bin/rclone';
    return '';
})();

var RCLONE_VERSION = '';
if (HAS_RCLONE) {
    try {
        var res = shell((RCLONE_PATH || '/usr/bin/rclone') + ' version 2>/dev/null', {timeout: 5000});
        var m = res.stdout.match(/rclone v([\d.]+)/);
        if (m) RCLONE_VERSION = m[1];
    } catch(e) {}
}

// Provider definitions
var RCLONE_PROVIDERS = {
    drive:    { label: 'Google Drive',  tier: 'oauth' },
    dropbox:  { label: 'Dropbox',       tier: 'oauth' },
    onedrive: { label: 'OneDrive',      tier: 'oauth' },
    s3:       { label: 'Amazon S3',     tier: 's3' },
    b2:       { label: 'Backblaze B2',  tier: 's3' },
    wasabi:   { label: 'Wasabi',        tier: 's3' },
    minio:    { label: 'MinIO',         tier: 's3' },
    _manual:  { label: 'Other (Manual)', tier: 'manual' }
};

function _shellEscape(s) {
    return "'" + s.replace(/'/g, "'\\''") + "'";
}

function getUserRcloneDir(username) {
    var dir = RCLONE_DIR + '/' + username;
    if (!stat(dir)) mkdir(dir);
    return dir;
}

function getUserRcloneConf(username) {
    return getUserRcloneDir(username) + '/rclone.conf';
}

function getUserMountDir(username, mountName, rootMount) {
    if (rootMount) return DAV_ROOT + '/' + mountName;
    return DAV_ROOT + '/' + username + '/' + mountName;
}

// Check if a path appears as a mount point in /proc/mounts
function _isMounted(mountPoint) {
    // Check if mountPoint is a different filesystem than its parent
    // (works cross-platform: Linux, macOS — no /proc or shell needed)
    try {
        var mpStat = stat(mountPoint);
        if (!mpStat || !mpStat.isDirectory) return false;
        var parentDir = mountPoint.replace(/\/[^\/]+\/?$/, '') || '/';
        var parentStat = stat(parentDir);
        if (!parentStat) return false;
        return mpStat.dev !== parentStat.dev;
    } catch(e) {
        return false;
    }
}

// Detect stale FUSE mounts — mounted but transport is dead.
function _isStaleMount(mountPoint) {
    if (!_isMounted(mountPoint)) return false;
    // The mount point has a different dev, so it's a mount.
    // Try to stat the mount point itself — if FUSE transport is dead, stat will throw or return false.
    try {
        // Re-stat the mount point; if the FUSE daemon is dead, this fails
        var staleCheck = stat(mountPoint + '/.');
        return !staleCheck;
    } catch(e) {
        return true;
    }
}

// Detect platform for unmount command
var IS_MACOS = rampart.buildPlatform.indexOf('Darwin') === 0;

function _fuseUnmount(mountPoint, lazy) {
    if (IS_MACOS) {
        return exec("umount", {timeout: 10000}, mountPoint);
    } else if (lazy) {
        return exec("fusermount", {timeout: 10000}, "-uz", mountPoint);
    } else {
        return exec("fusermount", {timeout: 10000}, "-u", mountPoint);
    }
}

// Force-unmount a stale FUSE mount and remove the mount point.
function _recoverStaleMount(mountPoint) {
    fprintf(stderr, "Recovering stale mount: %s\n", mountPoint);
    _fuseUnmount(mountPoint, true);
    // Give kernel a moment to release the mount
    rampart.utils.sleep(0.5);
    try { rmdir(mountPoint); } catch(e) {}
}

function rcloneMountRemote(username, mountName, remoteName, remotePath, extraFlags, envPrefix, rootMount) {
    var mountPoint = getUserMountDir(username, mountName, rootMount);
    var conf = getUserRcloneConf(username);

    if (!stat(mountPoint)) mkdir(mountPoint);

    // Already mounted?
    if (_isMounted(mountPoint)) {
        // Check if the mount is stale (dead FUSE transport)
        if (_isStaleMount(mountPoint)) {
            fprintf(stderr, "Stale mount detected at %s, recovering...\n", mountPoint);
            _recoverStaleMount(mountPoint);
            if (!stat(mountPoint)) mkdir(mountPoint);
        } else {
            return {ok: true, already: true};
        }
    }

    var cmd = (envPrefix || '') + (RCLONE_PATH || 'rclone') + ' mount' +
        ' --config ' + _shellEscape(conf) +
        ' --vfs-cache-mode writes' +
        ' --dir-cache-time 5s' +
        ' --poll-interval 15s' +
        ' --vfs-write-back 5s' +
        (FUSE_ALLOW_OTHER ? ' --allow-other' : '') +
        ' --daemon' +
        ' --daemon-wait 5s' +
        (extraFlags ? ' ' + extraFlags : '') +
        ' ' + _shellEscape(remoteName + ':' + (remotePath || '')) +
        ' ' + _shellEscape(mountPoint);

    try {
        var res = shell(cmd, {timeout: 15000});
        // Don't trust exit code — check if actually mounted (retry a few times)
        for (var _mwait = 0; _mwait < 5; _mwait++) {
            rampart.utils.sleep(1);
            if (_isMounted(mountPoint)) return {ok: true};
        }
        return {ok: false, error: 'Mount failed at ' + mountPoint + ': ' + ((res.stderr || '').trim() || 'not mounted after command completed')};
    } catch(e) {
        // Command may have timed out but mount could still succeed
        for (var _mwait2 = 0; _mwait2 < 5; _mwait2++) {
            rampart.utils.sleep(1);
            if (_isMounted(mountPoint)) return {ok: true};
        }
        return {ok: false, error: 'Mount failed at ' + mountPoint + ': ' + (e.message || 'command failed')};
    }
}

// Build env vars and CLI flags for SFTP mount from decrypted credentials.
// Passwords are passed via env vars (not visible in ps), key path via flags.
// Returns {env: 'VAR=val ...', flags: '--sftp-key-file ...'}.
// Build env vars for SFTP mount from decrypted credentials.
// All sensitive values passed via env vars (not visible in ps, no files on disk).
function buildSftpMountEnv(creds) {
    var env = '';
    if (creds.pass) {
        try {
            var obsRes = shell((RCLONE_PATH || 'rclone') + ' obscure ' + _shellEscape(creds.pass), {timeout: 5000});
            var obsOut = trim(obsRes.stdout || '');
            if (obsOut) {
                env += 'RCLONE_SFTP_PASS=' + _shellEscape(obsOut) + ' ';
            }
        } catch(e) {}
    }
    if (creds.key_pem) {
        // rclone expects key_pem with literal \n instead of actual newlines
        var pemOneLine = creds.key_pem.replace(/\r\n/g, '\n').replace(/\n/g, '\\n');
        env += 'RCLONE_SFTP_KEY_PEM=' + _shellEscape(pemOneLine) + ' ';
        if (creds.key_file_pass) {
            try {
                var obsRes2 = shell((RCLONE_PATH || 'rclone') + ' obscure ' + _shellEscape(creds.key_file_pass), {timeout: 5000});
                var obsOut2 = trim(obsRes2.stdout || '');
                if (obsOut2) {
                    env += 'RCLONE_SFTP_KEY_FILE_PASS=' + _shellEscape(obsOut2) + ' ';
                }
            } catch(e) {}
        }
    }
    return env;
}

function rcloneUnmount(username, mountName, rootMount) {
    var mountPoint = getUserMountDir(username, mountName, rootMount);

    _fuseUnmount(mountPoint, false);
    // Check if actually unmounted
    if (!_isMounted(mountPoint)) {
        try { rmdir(mountPoint); } catch(e) {}
        return {ok: true};
    }
    // Lazy unmount as fallback
    _fuseUnmount(mountPoint, true);
    rampart.utils.sleep(0.5);
    if (!_isMounted(mountPoint)) {
        try { rmdir(mountPoint); } catch(e) {}
        return {ok: true};
    }
    return {ok: false, error: 'Unmount failed'};
}

// Re-mount all active rclone mounts on server startup
if (HAS_RCLONE) {
    (function remountAll() {
        var allMounts = db.get(rcloneDbi, "", 10000);
        if (allMounts && typeof allMounts === 'object') {
            var keys = Object.keys(allMounts);
            for (var i = 0; i < keys.length; i++) {
                var cfg = allMounts[keys[i]];
                if (cfg && cfg.active && !keys[i].match(/^_oauth_|^_rootmount\//)) {
                    var parts = keys[i].split('/');
                    var username = parts[0];
                    var mountName = parts[1];
                    if (username && mountName) {
                        var mp = getUserMountDir(username, mountName, cfg.rootMount);
                        // Register in external paths so WebDAV can serve the mount
                        _addExternalPath(mp);
                        // SFTP mounts need encrypted credentials — skip auto-mount
                        if (cfg.provider === 'sftp') {
                            cfg.active = false;
                            db.put(rcloneDbi, keys[i], cfg);
                        } else {
                            rcloneMountRemote(username, mountName, mountName, (cfg && cfg.remotePath) || '', '', '', cfg.rootMount);
                        }
                    }
                }
            }
        }
    })();
}


/* ============================================================
 * Section 1b: Per-file metadata (ownership, permissions, groups)
 *
 * Key format:  <filename_max256>/<sha256_of_davRelPath>
 * Value:       { path, owner, group, permissions, isDir, created }
 * ============================================================ */

function fileMetaKey(davRelPath) {
    var clean = davRelPath.replace(/\/$/, '');
    var name = clean.split('/').pop() || '';
    if (name.length > 256) name = name.substring(0, 256);
    return name + '/' + crypto.sha256(davRelPath);
}

// Check if a davRelPath is inside a user's home directory.
// User paths are /<username>/... where <username> exists in userDbi.
// Files here are always owned by the user with 600 permissions — no metadata stored.
function isUserPath(davRelPath) {
    var segments = davRelPath.split('/').filter(Boolean);
    if (segments.length < 1) return false;
    return !!db.get(userDbi, segments[0]);
}

function getFileMeta(davRelPath) {
    if (isUserPath(davRelPath)) return null;
    var rec = db.get(filemetaDbi, fileMetaKey(davRelPath));
    return rec || null;
}

function setFileMeta(davRelPath, meta) {
    if (isUserPath(davRelPath)) return;
    db.put(filemetaDbi, fileMetaKey(davRelPath), meta);
}

function deleteFileMeta(davRelPath) {
    if (isUserPath(davRelPath)) return;
    db.del(filemetaDbi, fileMetaKey(davRelPath));
}

function isUserHome(davRelPath) {
    var segments = davRelPath.split('/').filter(Boolean);
    return segments.length === 1 && !!db.get(userDbi, segments[0]);
}

function createFileMeta(davRelPath, owner, isDir) {
    var userPath = isUserPath(davRelPath);
    var meta = {
        path: davRelPath,
        owner: owner,
        group: userPath ? 'nogroup' : 'everyone',
        permissions: userPath ? 600 : (isDir ? 755 : 644),
        isDir: !!isDir,
        created: new Date().toISOString()
    };
    setFileMeta(davRelPath, meta); // no-op for user paths
    return meta;
}

// Infer owner from path: if under /<username>/, owner is that user.
// Otherwise, owner is the first admin found in the user database.
function inferOwner(davRelPath) {
    var segments = davRelPath.split('/').filter(Boolean);
    if (segments.length > 0) {
        var pathUser = segments[0];
        var rec = db.get(userDbi, pathUser);
        if (rec) return pathUser;
    }
    // Not in a user directory — find the first admin
    var all = db.get(userDbi, "", 10000);
    if (all) {
        var keys = Object.keys(all);
        for (var i = 0; i < keys.length; i++) {
            if (all[keys[i]].admin) return keys[i];
        }
        // No admin found, use first user
        if (keys.length > 0) return keys[0];
    }
    return 'unknown';
}

// Get metadata for a path, auto-creating if the file exists but has no entry.
function ensureFileMeta(davRelPath, fsPath) {
    var meta = getFileMeta(davRelPath);
    if (meta) return meta;
    // No metadata — check if the file/dir actually exists on disk
    var st = stat(fsPath);
    if (!st) return null;
    var owner = inferOwner(davRelPath);
    return createFileMeta(davRelPath, owner, st.isDirectory);
}

function moveFileMeta(srcDavRel, destDavRel) {
    var meta = getFileMeta(srcDavRel);
    if (meta) {
        meta.path = destDavRel;
        setFileMeta(destDavRel, meta);
        deleteFileMeta(srcDavRel);
    } else if (!isUserPath(destDavRel)) {
        // Source had no metadata (user path) — create fresh at shared destination
        var st = stat(DAV_ROOT + decodeURIComponent(destDavRel));
        if (st) createFileMeta(destDavRel, inferOwner(destDavRel), st.isDirectory);
    }
}

function moveFileMetaRecursive(srcDavRel, destDavRel, destFsPath) {
    // Move the directory entry itself
    moveFileMeta(srcDavRel, destDavRel);

    // Walk the destination filesystem (already moved) and re-key children
    var entries = readdir(destFsPath);
    if (!entries) return;
    for (var i = 0; i < entries.length; i++) {
        var name = entries[i];
        var childFs = destFsPath + '/' + name;
        var childSrcRel = srcDavRel.replace(/\/?$/, '/') + name;
        var childDestRel = destDavRel.replace(/\/?$/, '/') + name;
        var st = stat(childFs);
        if (st && st.isDirectory) {
            moveFileMetaRecursive(childSrcRel + '/', childDestRel + '/', childFs);
        } else {
            moveFileMeta(childSrcRel, childDestRel);
        }
    }
}

function copyFileMeta(srcDavRel, destDavRel, newOwner) {
    var meta = getFileMeta(srcDavRel);
    if (meta) {
        var copy = JSON.parse(JSON.stringify(meta));
        copy.path = destDavRel;
        copy.owner = newOwner;
        copy.created = new Date().toISOString();
        // Reset to default permissions when copying outside owner's home directory
        if (destDavRel.indexOf('/' + newOwner + '/') !== 0) {
            copy.permissions = copy.isDir ? 755 : 644;
        }
        setFileMeta(destDavRel, copy);
    } else {
        // Source has no metadata; create fresh entry
        var st = stat(DAV_ROOT + decodeURIComponent(destDavRel));
        createFileMeta(destDavRel, newOwner, st ? st.isDirectory : false);
    }
}

function copyFileMetaRecursive(srcDavRel, destDavRel, destFsPath, newOwner) {
    copyFileMeta(srcDavRel, destDavRel, newOwner);

    var entries = readdir(destFsPath);
    if (!entries) return;
    for (var i = 0; i < entries.length; i++) {
        var name = entries[i];
        var childFs = destFsPath + '/' + name;
        var childSrcRel = srcDavRel.replace(/\/?$/, '/') + name;
        var childDestRel = destDavRel.replace(/\/?$/, '/') + name;
        var st = stat(childFs);
        if (st && st.isDirectory) {
            copyFileMetaRecursive(childSrcRel + '/', childDestRel + '/', childFs, newOwner);
        } else {
            copyFileMeta(childSrcRel, childDestRel, newOwner);
        }
    }
}

function deleteFileMetaRecursive(davRelPath, fsPath) {
    // Walk filesystem and remove each entry's metadata (must be called before rmdir)
    var entries = readdir(fsPath);
    if (entries) {
        for (var i = 0; i < entries.length; i++) {
            var name = entries[i];
            var childFs = fsPath + '/' + name;
            var childRel = davRelPath.replace(/\/?$/, '/') + name;
            var st = stat(childFs);
            if (st && st.isDirectory) {
                deleteFileMetaRecursive(childRel + '/', childFs);
            } else {
                deleteFileMeta(childRel);
            }
        }
    }
    deleteFileMeta(davRelPath);
}

/* ============================================================
 * Section 1c: Thumbnails
 *
 * 128x128 JPEG thumbnails stored as a shadow tree mirroring WebDAV paths:
 *   data/webdav_thumbnails/<davRelPath>.jpg
 * Generated server-side via rampart-gm on upload. Served via
 * GET /dav/_thumb/<davRelPath>. Lazy-generated for existing files.
 * ============================================================ */

function thumbPath(davRelPath) {
    var decoded = decodeURIComponent(davRelPath.replace(/\/$/, ''));
    return THUMB_DIR + '/' + decoded + '.jpg';
}

function thumbDir(davRelPath) {
    var decoded = decodeURIComponent(davRelPath.replace(/\/$/, ''));
    return THUMB_DIR + '/' + decoded;
}

function isThumbable(fsPath) {
    if (THUMB_IMAGE_RE.test(fsPath)) return 'image';
    if (HAS_FFMPEG && THUMB_VIDEO_RE.test(fsPath)) return 'video';
    return false;
}

function generateThumbnail(fsPath, davRelPath) {
    var type = isThumbable(fsPath);
    if (!type) return false;
    var dest = thumbPath(davRelPath);
    try {
        ensureDirExists(getParentDir(dest));
        if (type === 'video') {
            return generateVideoThumbnail(fsPath, dest);
        }
        var img = gm.open(fsPath);
        img.mogrify({
            'thumbnail': '256x256',
            'auto-orient': true,
            'quality': '80'
        });
        img.save(dest);
        return true;
    } catch (e) {
        return false;
    }
}

function generateVideoThumbnail(fsPath, dest) {
    try {
        var probe = shell(_shellEscape(FFPROBE_PATH || 'ffprobe') + " -v error -show_entries format=duration -of csv=p=0 " +
            _shellEscape(fsPath), {timeout: 10000});
        var duration = parseFloat(probe.stdout);
        var seekTo = '00:00:01';
        if (!isNaN(duration) && duration > 0) {
            var secs = Math.floor(duration * 0.1);
            var h = Math.floor(secs / 3600);
            var m = Math.floor((secs % 3600) / 60);
            var s = secs % 60;
            seekTo = sprintf('%02d:%02d:%02d', h, m, s);
        }
        var res = shell(_shellEscape(FFMPEG_PATH || 'ffmpeg') + " -y -ss " + seekTo + " -i " +
            _shellEscape(fsPath) +
            " -frames:v 1 -vf 'scale=256:256:force_original_aspect_ratio=decrease' " +
            _shellEscape(dest), {timeout: 120000});
        // Trust the output file over the exit code — ffmpeg sometimes returns
        // non-zero even when the thumbnail was successfully written
        var destSt = stat(dest);
        if (destSt && destSt.size > 0) return true;
        if (res.exitStatus !== 0) {
            _lastThumbError = 'ffmpeg exit ' + res.exitStatus + ': ' + (res.stderr || '').slice(-1000);
        }
        return false;
    } catch (e) {
        fprintf(stderr, 'ffmpeg thumbnail exception for %s: %s\n', fsPath, e.message || e);
        return false;
    }
}

function deleteThumbnail(davRelPath) {
    try { var p = thumbPath(davRelPath); if (stat(p)) rmFile(p); } catch (e) {}
}

function moveThumbnail(srcDavRel, destDavRel) {
    if (!isThumbable(srcDavRel)) return;
    var srcP = thumbPath(srcDavRel);
    if (!stat(srcP)) return;
    var destP = thumbPath(destDavRel);
    try {
        ensureDirExists(getParentDir(destP));
        rename(srcP, destP);
    } catch (e) {}
}

function copyThumbnail(srcDavRel, destDavRel) {
    if (!isThumbable(srcDavRel)) return;
    var srcP = thumbPath(srcDavRel);
    if (!stat(srcP)) return;
    var destP = thumbPath(destDavRel);
    try {
        ensureDirExists(getParentDir(destP));
        copyFile(srcP, destP);
    } catch (e) {}
}

function deleteThumbnailsRecursive(davRelPath) {
    rmdirThumbnails(thumbDir(davRelPath));
    deleteThumbnail(davRelPath);
}

function moveThumbnailsRecursive(srcDavRel, destDavRel) {
    var srcDir = thumbDir(srcDavRel);
    var destDir = thumbDir(destDavRel);
    try {
        if (stat(srcDir)) {
            ensureDirExists(getParentDir(destDir));
            rename(srcDir, destDir);
        }
    } catch (e) {}
    moveThumbnail(srcDavRel, destDavRel);
}

function copyThumbnailsRecursive(srcDavRel, destDavRel) {
    var srcDir = thumbDir(srcDavRel);
    var destDir = thumbDir(destDavRel);
    try {
        if (stat(srcDir)) {
            ensureDirExists(getParentDir(destDir));
            copyRecursive(srcDir, destDir);
        }
    } catch (e) {}
    copyThumbnail(srcDavRel, destDavRel);
}

// Serve a thumbnail — generate on-the-fly if missing
function handleThumb(req, davRelPath, fsPath) {
    if (!isThumbable(fsPath)) {
        return { status: 404, txt: 'No thumbnail available' };
    }
    var st0 = stat(fsPath);
    if (!st0) return { status: 404, txt: 'Not Found' };

    var tp = thumbPath(davRelPath);
    if (!stat(tp)) {
        // Generate on the fly
        if (!generateThumbnail(fsPath, davRelPath)) {
            return { status: 404, txt: 'Thumbnail generation failed' };
        }
    }
    return {
        status: 200,
        headers: { 'Cache-Control': 'max-age=86400' },
        jpg: '@' + tp
    };
}

var SUPPORTED_METHODS = 'OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, PROPFIND, PROPPATCH, LOCK, UNLOCK';



/* ============================================================
 * Section 1d: Full-Text Search Engine
 *
 * Uses rampart-sql (Texis) for full-text indexing and search,
 * and rampart-totext for extracting text from documents.
 *
 * Single table for all indexed documents. Access control is
 * enforced at query time by filtering on path visibility.
 *
 * All indexing operations run in a dedicated background thread
 * via a task queue stored in LMDB.
 * ============================================================ */

var SEARCH_DB_PATH = dataRoot + '/search_db';
var SEARCH_LOG = process.scriptPath + '/logs/index.log';

// File types that can be indexed (totext-supported + plain text subtitles)
var SEARCH_EXTENSIONS = /\.(txt|html?|md|markdown|xml|rtf|tex|latex|csv|json|docx|pptx|xlsx|odt|odp|ods|epub|pdf|doc|srt|vtt)$/i;

// Default indexed subdirectory name under each user's home
var SEARCH_DEFAULT_DIR = 'Documents';

// Which directories are indexed — stored in LMDB
var searchDbi = db.openDb("searchdirs", true);

// Task queue for the indexing thread — stored in LMDB
var indexQueueDbi = db.openDb("indexqueue", true);

// Decode a davRelPath for search DB operations (DB stores decoded paths)
function searchDecodePath(davRelPath) {
    try { return decodeURIComponent(davRelPath); } catch(e) { return davRelPath; }
}

// Get all indexed directory paths
function searchGetIndexedDirs() {
    var all = db.get(searchDbi, "", 10000);
    var dirs = [];
    if (all) {
        var keys = Object.keys(all);
        for (var i = 0; i < keys.length; i++) {
            if (all[keys[i]] && all[keys[i]].enabled) dirs.push(keys[i]);
        }
    }
    return dirs;
}

// Check if a file path is inside any indexed directory
function searchIsIndexed(davRelPath) {
    var dirs = searchGetIndexedDirs();
    var path = searchDecodePath(davRelPath).replace(/\/?$/, '');
    for (var i = 0; i < dirs.length; i++) {
        var dir = dirs[i].replace(/\/?$/, '/');
        if ((path + '/').indexOf(dir) === 0 || path.indexOf(dir) === 0) return true;
    }
    return false;
}


// Check if a directory has an indexed parent
function searchGetIndexedParent(davRelPath) {
    var dirs = searchGetIndexedDirs();
    var path = davRelPath.replace(/\/?$/, '/');
    for (var i = 0; i < dirs.length; i++) {
        var dir = dirs[i].replace(/\/?$/, '/');
        if (path.indexOf(dir) === 0 && path !== dir) return dirs[i];
    }
    return null;
}

// --- Task Queue ---
// Keys are ISO timestamps with a random suffix for uniqueness and sort order
// Values are JSON objects: {op, ...params}

function indexQueueKey() {
    return new Date().toISOString() + '|' + hexify(crypto.rand(4));
}

function indexQueuePush(task) {
    db.put(indexQueueDbi, indexQueueKey(), task);
}

// Public API: queue indexing operations from any server thread

function searchSkipFile(fsPath) {
    var name = fsPath.substring(fsPath.lastIndexOf('/') + 1);
    if (name.indexOf('.~') === 0) return true;  // LibreOffice/OnlyOffice autosave and lock files
    if (name.charAt(0) === '~' && name.charAt(1) === '$') return true;  // MS Office temp files
    return false;
}

function searchIndexFile(fsPath, davRelPath) {
    var decoded = searchDecodePath(davRelPath);
    // Always update path index (unless skipped)
    if (!searchSkipFile(fsPath)) {
        pathIndexFile(davRelPath, false);
    }
    // Document content index — only for supported extensions in indexed dirs
    if (searchSkipFile(fsPath)) return;
    if (!SEARCH_EXTENSIONS.test(fsPath)) return;
    if (!searchIsIndexed(davRelPath)) return;
    indexQueuePush({op: 'index', fsPath: fsPath, davRelPath: decoded});
}

function searchDeleteFile(davRelPath) {
    pathDeleteFile(davRelPath);
    indexQueuePush({op: 'delete', davRelPath: searchDecodePath(davRelPath)});
}

function searchDeleteDir(davRelPath) {
    pathDeleteDir(davRelPath);
    indexQueuePush({op: 'deletedir', davRelPath: searchDecodePath(davRelPath)});
}

function searchMovePath(oldDavRel, newDavRel) {
    var oldPath = searchDecodePath(oldDavRel);
    var newPath = searchDecodePath(newDavRel);

    // Path index: always update
    pathMovePath(oldDavRel, newDavRel);

    // Doc content index: depends on indexed dirs
    var destIndexed = searchIsIndexed(newDavRel);
    var srcIndexed = searchIsIndexed(oldDavRel);

    if (destIndexed && srcIndexed) {
        indexQueuePush({op: 'move', oldPath: oldPath, newPath: newPath});
    } else if (srcIndexed) {
        indexQueuePush({op: 'delete', davRelPath: oldPath});
    } else if (destIndexed) {
        indexQueuePush({op: 'index', fsPath: DAV_ROOT + newPath, davRelPath: newPath});
    }
}

function searchMoveDir(oldDavRel, newDavRel) {
    var oldPath = searchDecodePath(oldDavRel);
    var newPath = searchDecodePath(newDavRel);

    // Path index: always update
    pathMoveDir(oldDavRel, newDavRel);

    // Doc content index: depends on indexed dirs
    var destIndexed = searchIsIndexed(newDavRel);
    var srcIndexed = searchIsIndexed(oldDavRel);

    if (destIndexed && srcIndexed) {
        indexQueuePush({op: 'movedir', oldPath: oldPath, newPath: newPath});
    } else if (srcIndexed) {
        indexQueuePush({op: 'deletedir', davRelPath: oldPath});
    } else if (destIndexed) {
        indexQueuePush({op: 'scan', davRelPath: newPath, fsPath: DAV_ROOT + newPath});
    }
}

function searchEnableDir(davRelPath) {
    db.put(searchDbi, davRelPath, {enabled: true});
    indexQueuePush({op: 'scan', davRelPath: davRelPath, fsPath: DAV_ROOT + davRelPath});
}

function searchDisableDir(davRelPath) {
    db.del(searchDbi, davRelPath);
    indexQueuePush({op: 'deletedir', davRelPath: davRelPath});
}

function searchScanDir(fsBase, davRelBase) {
    indexQueuePush({op: 'scan', davRelPath: davRelBase, fsPath: fsBase});
}

// --- Path index queue functions ---

function pathIndexFile(davRelPath, isDir) {
    indexQueuePush({op: 'path_index', davRelPath: searchDecodePath(davRelPath), isDir: !!isDir});
}

function pathDeleteFile(davRelPath) {
    indexQueuePush({op: 'path_delete', davRelPath: searchDecodePath(davRelPath)});
}

function pathDeleteDir(davRelPath) {
    indexQueuePush({op: 'path_deletedir', davRelPath: searchDecodePath(davRelPath)});
}

function pathMovePath(oldDavRel, newDavRel) {
    indexQueuePush({op: 'path_move', oldPath: searchDecodePath(oldDavRel), newPath: searchDecodePath(newDavRel)});
}

function pathMoveDir(oldDavRel, newDavRel) {
    indexQueuePush({op: 'path_movedir', oldPath: searchDecodePath(oldDavRel), newPath: searchDecodePath(newDavRel)});
}

// --- Seed defaults ---
// Add each user's Documents/ as indexed if not already present
(function searchSeedDefaults() {
    var users = db.get(userDbi, "", 10000);
    if (users) {
        var userNames = Object.keys(users);
        for (var si = 0; si < userNames.length; si++) {
            var uname = userNames[si];
            var docsDav = '/' + uname + '/' + SEARCH_DEFAULT_DIR;
            var docsFs = DAV_ROOT + docsDav;
            if (stat(docsFs) && !db.get(searchDbi, docsDav)) {
                db.put(searchDbi, docsDav, {enabled: true});
            }
        }
    }
    // In demo mode, also index the demo-files directory
    if (DEMO_MODE) {
        var demoFilesDav = '/' + DEMO_FILES_DIR;
        var demoFilesFs = DAV_ROOT + demoFilesDav;
        if (stat(demoFilesFs) && !db.get(searchDbi, demoFilesDav)) {
            db.put(searchDbi, demoFilesDav, {enabled: true});
        }
    }
})();

if(module && module.exports) {
    // --- Launch indexing thread ---
    // Initial scans are NOT queued here. They are triggered inside the
    // indexing thread only when tables are first created (empty DB).
    // After that, incremental updates happen via PUT/DELETE/MOVE/COPY hooks
    // and the nightly scheduled index optimization handles the rest.

    // Start the indexing service thread (once per server lifetime)
    (function startIndexService() {
        indexLock.lock();
        try {
            if (rampart.thread.get('indexServiceStarted')) {
                indexLock.unlock();
                return;
            }

            var thr = new rampart.thread();
            thr.exec(
                // --- Thread function: runs in dedicated thread ---
                function(cfg) {
                    var u = rampart.utils;
                    var _Sql = require("rampart-sql");
                    var _totext = require("rampart-totext");
                    var _Lmdb = require("rampart-lmdb");

                    if(!cfg.logFile)
                        cfg.logFile = serverConf.serverRoot + '/logs/index.log';

                    var logdir = cfg.logFile.replace(/[^/]+$/, '');
                    if(!stat(logdir))
                        mkdir(logdir,true);

                    // Open our own handles
                    var _db = new _Lmdb.init(cfg.dbPath, true, {conversion: 'JSON', mapSize: 64});
                    var _queueDbi = _db.openDb("indexqueue", true);
                    var _searchDbi = _db.openDb("searchdirs", true);

                    var _sql = new _Sql.connection(cfg.searchDbPath, true);

                    // Ensure the docs table exists — initial scan on first creation
                    var docsCreated = false;
                    if (!_sql.one("SELECT * FROM SYSTABLES WHERE NAME = ?", ['docs'])) {
                        _sql.exec(
                            "CREATE TABLE docs (" +
                            "  path varchar(512), " +
                            "  title varchar(255), " +
                            "  content varchar(1000000), " +
                            "  mtime int, " +
                            "  size int" +
                            ")"
                        );
                        _sql.exec(```CREATE FULLTEXT INDEX docs_content_ftx ON docs(content)
                            WITH WORDEXPRESSIONS (
                              '[\alnum\x80-\xFF]{2,99}',
                              '[\alnum\x80-\xFF$<>%@\-_+]{2,99}'
                            )```);
                        _sql.exec("CREATE INDEX docs_path_idx ON docs(path)");
                        try {
                            _sql.scheduleUpdate('docs_content_ftx', '02:00', 'every day', 1000);
                            log("Fulltext index update scheduled: daily at 02:00, threshold 1000");
                        } catch(e) {
                            log("Failed to schedule fulltext index update: " + e.message);
                        }
                        docsCreated = true;
                    }

                    // --- Paths table: filename search ---
                    var pathsCreated = false;
                    if (!_sql.one("SELECT * FROM SYSTABLES WHERE NAME = ?", ['paths'])) {
                        _sql.exec(
                            "CREATE TABLE paths (" +
                            "  path varchar(512), " +
                            "  pathrev varchar(512), " +
                            "  isdir int" +
                            ")"
                        );
                        _sql.exec("CREATE INDEX paths_path_idx ON paths(path)");
                        _sql.exec("CREATE INDEX paths_pathrev_idx ON paths(pathrev)");
                        _sql.exec(```CREATE FULLTEXT INDEX paths_path_ftx ON paths(path)
                            WITH WORDEXPRESSIONS (
                              '[\alnum\x80-\xFF]{2,99}'
                            )```);
                        try {
                            _sql.scheduleUpdate('paths_path_ftx', '02:00', 'every day', 1000);
                        } catch(e) {}
                        pathsCreated = true;
                    }

                    function reversePath(p) {
                        return p.split('').reverse().join('');
                    }

                    var EXTENSIONS = /\.(txt|html?|md|markdown|xml|rtf|tex|latex|csv|json|docx|pptx|xlsx|odt|odp|ods|epub|pdf|doc|srt|vtt)$/i;

                    // Skip autosave/temp files
                    function skipName(name) {
                        if (name.indexOf('.~') === 0) return true;
                        if (name.charAt(0) === '~' && name.charAt(1) === '$') return true;
                        return false;
                    }

                    // Check if a path is a mount point (different device than parent)
                    function isMountPoint(fsPath) {
                        try {
                            var mpStat = u.stat(fsPath);
                            if (!mpStat || !mpStat.isDirectory) return false;
                            var parentDir = fsPath.replace(/\/[^\/]+\/?$/, '') || '/';
                            var parentStat = u.stat(parentDir);
                            if (!parentStat) return false;
                            return mpStat.dev !== parentStat.dev;
                        } catch(e) { return false; }
                    }

                    function log(msg) {
                        u.fprintf(cfg.logFile, true, "%s %s\n", new Date().toISOString(), msg);
                    }

                    // Index a single file
                    function doIndex(fsPath, davRelPath) {
                        if (!EXTENSIONS.test(fsPath)) return;
                        var fsStat = u.stat(fsPath);
                        if (!fsStat || fsStat.isDirectory || fsStat.size === 0) return;

                        // Check if already indexed with same mtime
                        var existing = _sql.one("SELECT mtime FROM docs WHERE path = ?", [davRelPath]);
                        if (existing && existing.mtime === Math.floor(fsStat.mtime / 1000)) return;

                        var text;
                        try {
                            text = _totext.convertFile(fsPath);
                        } catch(e) {
                            log("totext error for " + davRelPath + ": " + e.message);
                            return;
                        }
                        if (!text || !text.length) return;

                        var title = fsPath.substring(fsPath.lastIndexOf('/') + 1);
                        var dotIdx = title.lastIndexOf('.');
                        if (dotIdx > 0) title = title.substring(0, dotIdx);

                        var res = _sql.exec("DELETE FROM docs WHERE path = ?", [davRelPath]);
                        _sql.exec("INSERT INTO docs VALUES (?, ?, ?, ?, ?)",
                            [davRelPath, title, text, Math.floor(fsStat.mtime / 1000), fsStat.size]);
                    }

                    // Recursively scan and index a directory
                    function doScan(fsBase, davRelBase) {
                        var entries;
                        try { entries = u.readdir(fsBase, true); } catch(e) {
                            log("scan readdir error for " + davRelBase + ": " + e.message);
                            return;
                        }
                        for (var i = 0; i < entries.length; i++) {
                            var ename = entries[i];
                            if (ename === '.' || ename === '..') continue;
                            if (ename.indexOf('.~') === 0 || (ename.charAt(0) === '~' && ename.charAt(1) === '$')) continue;
                            var childFs = fsBase + '/' + ename;
                            var childDav = davRelBase.replace(/\/?$/, '/') + ename;
                            var childSt = u.stat(childFs);
                            if (!childSt) continue;
                            if (childSt.isDirectory) {
                                doScan(childFs, childDav);
                            } else {
                                doIndex(childFs, childDav);
                            }
                        }
                    }

                    function doDelete(davRelPath) {
                        var res = _sql.exec("DELETE FROM docs WHERE path = ?", [davRelPath]);
                        if (res.rowCount === 0) log("delete: no match for " + davRelPath);
                    }

                    function doDeleteDir(davRelPath) {
                        var prefix = davRelPath.replace(/\/?$/, '/');
                        _sql.exec("DELETE FROM docs WHERE path MATCHES ?", [prefix + '%']);
                    }

                    function doMove(oldPath, newPath) {
                        var res = _sql.exec("UPDATE docs SET path = ? WHERE path = ?", [newPath, oldPath]);
                        if (res.rowCount === 0) log("move: no match for " + oldPath);
                    }

                    function doMoveDir(oldPath, newPath) {
                        var oldPrefix = oldPath.replace(/\/?$/, '/');
                        var newPrefix = newPath.replace(/\/?$/, '/');
                        var rows = _sql.exec("SELECT path FROM docs WHERE path MATCHES ?", {maxRows: 100000}, [oldPrefix + '%']);
                        if (rows.rows) {
                            for (var i = 0; i < rows.rows.length; i++) {
                                var op = rows.rows[i].path;
                                var np = newPrefix + op.substring(oldPrefix.length);
                                _sql.exec("UPDATE docs SET path = ? WHERE path = ?", [np, op]);
                            }
                        }
                    }

                    // --- Path index operations ---

                    function doPathIndex(davRelPath, isDir) {
                        // Delete old entry if exists
                        _sql.exec("DELETE FROM paths WHERE path = ?", [davRelPath]);
                        _sql.exec("INSERT INTO paths VALUES (?, ?, ?)",
                            [davRelPath, reversePath(davRelPath), isDir ? 1 : 0]);
                    }

                    function doPathDelete(davRelPath) {
                        _sql.exec("DELETE FROM paths WHERE path = ?", [davRelPath]);
                    }

                    function doPathDeleteDir(davRelPath) {
                        var prefix = davRelPath.replace(/\/?$/, '/');
                        _sql.exec("DELETE FROM paths WHERE path = ?", [davRelPath]);
                        _sql.exec("DELETE FROM paths WHERE path MATCHES ?", [prefix + '%']);
                    }

                    function doPathMove(oldPath, newPath) {
                        _sql.exec("DELETE FROM paths WHERE path = ?", [oldPath]);
                        // Check if it's a dir
                        var existing = _sql.one("SELECT isdir FROM paths WHERE path = ?", [oldPath]);
                        var isDir = existing ? existing.isdir : 0;
                        // Just re-insert with new path (since pathrev also changes)
                        _sql.exec("INSERT INTO paths VALUES (?, ?, ?)",
                            [newPath, reversePath(newPath), isDir]);
                    }

                    function doPathMoveDir(oldPath, newPath) {
                        var oldPrefix = oldPath.replace(/\/?$/, '/');
                        var newPrefix = newPath.replace(/\/?$/, '/');
                        // Move the directory entry itself
                        _sql.exec("DELETE FROM paths WHERE path = ?", [oldPath]);
                        _sql.exec("INSERT INTO paths VALUES (?, ?, ?)", [newPath, reversePath(newPath), 1]);
                        // Move all children
                        var rows = _sql.exec("SELECT path, isdir FROM paths WHERE path MATCHES ?", {maxRows: 100000}, [oldPrefix + '%']);
                        if (rows.rows) {
                            for (var i = 0; i < rows.rows.length; i++) {
                                var op = rows.rows[i].path;
                                var np = newPrefix + op.substring(oldPrefix.length);
                                _sql.exec("DELETE FROM paths WHERE path = ?", [op]);
                                _sql.exec("INSERT INTO paths VALUES (?, ?, ?)",
                                    [np, reversePath(np), rows.rows[i].isdir]);
                            }
                        }
                    }

                    function doPathScan(fsBase, davRelBase, _visited) {
                        if (!_visited) _visited = {};
                        // Resolve real path to prevent infinite recursion through symlinks
                        var realDir;
                        try { realDir = u.realPath(fsBase); } catch(e) { realDir = fsBase; }
                        if (_visited[realDir]) return;
                        _visited[realDir] = true;

                        var entries;
                        try { entries = u.readdir(fsBase, true); } catch(e) {
                            log("path_scan readdir error for " + davRelBase + ": " + e.message);
                            return;
                        }
                        for (var i = 0; i < entries.length; i++) {
                            var ename = entries[i];
                            if (ename === '.' || ename === '..') continue;
                            if (skipName(ename)) continue;
                            var childFs = fsBase + '/' + ename;
                            var childDav = davRelBase.replace(/\/?$/, '/') + ename;
                            // Use stat (follows symlinks) to determine type
                            var childSt = u.stat(childFs);
                            if (!childSt) continue;
                            doPathIndex(childDav, childSt.isDirectory);
                            if (childSt.isDirectory) {
                                if (isMountPoint(childFs)) continue;
                                doPathScan(childFs, childDav, _visited);
                            }
                        }
                    }

                    log("Index service started");

                    // Initial scans — only on first-time table creation
                    if (docsCreated) {
                        log("First run: scanning indexed directories for document content");
                        var _searchDirs = _db.get(_searchDbi, "", 10000);
                        if (_searchDirs) {
                            var _sdKeys = Object.keys(_searchDirs);
                            for (var _sdi = 0; _sdi < _sdKeys.length; _sdi++) {
                                if (_searchDirs[_sdKeys[_sdi]] && _searchDirs[_sdKeys[_sdi]].enabled) {
                                    var _sdFs = cfg.davRoot + _sdKeys[_sdi];
                                    if (u.stat(_sdFs)) {
                                        log("Scanning docs: " + _sdKeys[_sdi]);
                                        doScan(_sdFs, _sdKeys[_sdi]);
                                    }
                                }
                            }
                        }
                        try { _sql.exec("ALTER INDEX docs_content_ftx OPTIMIZE HAVING COUNT(NewRows) > 0"); } catch(e) {}
                        log("Document content scan complete");
                    }
                    if (pathsCreated) {
                        log("First run: scanning all paths");
                        doPathScan(cfg.davRoot, '');
                        try { _sql.exec("ALTER INDEX paths_path_ftx OPTIMIZE HAVING COUNT(NewRows) > 0"); } catch(e) {}
                        log("Path scan complete");
                    }

                    // Main loop: process tasks from queue
                    while (true) {
                        var tasks = _db.get(_queueDbi, "", 100);
                        var processed = 0;

                        if (tasks && typeof tasks === 'object') {
                            var keys = Object.keys(tasks).sort(); // ASCII sort = time order
                            for (var ki = 0; ki < keys.length; ki++) {
                                var task = tasks[keys[ki]];
                                try {
                                    switch (task.op) {
                                        case 'index':
                                            doIndex(task.fsPath, task.davRelPath);
                                            break;
                                        case 'delete':
                                            doDelete(task.davRelPath);
                                            break;
                                        case 'deletedir':
                                            doDeleteDir(task.davRelPath);
                                            break;
                                        case 'move':
                                            doMove(task.oldPath, task.newPath);
                                            break;
                                        case 'movedir':
                                            doMoveDir(task.oldPath, task.newPath);
                                            break;
                                        case 'scan':
                                            doScan(task.fsPath, task.davRelPath);
                                            break;
                                        case 'path_index':
                                            doPathIndex(task.davRelPath, task.isDir);
                                            break;
                                        case 'path_delete':
                                            doPathDelete(task.davRelPath);
                                            break;
                                        case 'path_deletedir':
                                            doPathDeleteDir(task.davRelPath);
                                            break;
                                        case 'path_move':
                                            doPathMove(task.oldPath, task.newPath);
                                            break;
                                        case 'path_movedir':
                                            doPathMoveDir(task.oldPath, task.newPath);
                                            break;
                                        case 'path_scan':
                                            doPathScan(task.fsPath, task.davRelPath);
                                            break;
                                        default:
                                            log("unknown task op: " + task.op);
                                    }
                                } catch(e) {
                                    log("task error (" + task.op + " " + (task.davRelPath || task.oldPath || '') + "): " + e.message);
                                }
                                // Delete completed task
                                _db.del(_queueDbi, keys[ki]);
                                processed++;
                            }
                        }

                        // Sleep if no tasks were processed
                        if (processed === 0) {
                            u.sleep(2);
                        }
                    }
                },
                // --- Thread argument ---
                {
                    dbPath: DB_PATH,
                    searchDbPath: SEARCH_DB_PATH,
                    logFile: SEARCH_LOG,
                    davRoot: DAV_ROOT
                },
                // --- Callback: runs in the calling thread's event loop on error ---
                function(value, error) {
                    if (error) {
                        fprintf(stderr, "Index service crashed: %s\n", error.message || error);
                        try {
                            fprintf(SEARCH_LOG, true, "%s Index service crashed: %s\n",
                                new Date().toISOString(), error.message || error);
                        } catch(e) {}
                        rampart.thread.put('indexServiceStarted', false);
                        // Touch webdav.js to trigger module reload on next request
                        try {
                            touch(serverConf.appsRoot + '/webdav/webdav.js');
                        } catch(e) {}
                    }
                }
            );

            rampart.thread.put('indexServiceStarted', true);
            indexLock.unlock();
        } catch(e) {
            indexLock.unlock();
            fprintf(stderr, "Failed to start index service: %s\n", e.message || e);
        }
    })();

    // --- Demo mode: start wipe thread (once per server lifetime) ---
    if (DEMO_MODE && !rampart.thread.get('demo_wipe_running')) {
        var _demoWipeThr = new rampart.thread();
        _demoWipeThr.exec(function(cfg) {
            rampart.thread.put('demo_wipe_running', true);
            var u = rampart.utils;
            var _Sql = require("rampart-sql");
            var _wSql;
            try { _wSql = new _Sql.connection(cfg.searchDbPath); } catch(e) { _wSql = null; }

            while (true) {
                u.sleep(cfg.interval);
                if (!u.stat(cfg.home)) continue;
                var entries = u.readdir(cfg.home, true);
                if (!entries) continue;
                var now = Date.now();
                var deleted = [];
                for (var i = 0; i < entries.length; i++) {
                    if (entries[i] === '.' || entries[i] === '..') continue;
                    var p = cfg.home + '/' + entries[i];
                    var s = u.stat(p);
                    if (!s) continue;
                    if ((now - s.mtime.getTime()) / 1000 > cfg.interval) {
                        deleted.push('/demo/' + entries[i]);
                        u.shell("rm -rf '" + p.replace(/'/g, "'\\''") + "'");
                    }
                }
                for (var di = 0; di < cfg.dirs.length; di++) {
                    var dd = cfg.home + '/' + cfg.dirs[di];
                    if (!u.stat(dd)) try { u.mkdir(dd); } catch(e) {}
                }
                // Clean up search index entries for deleted files
                if (_wSql && deleted.length > 0) {
                    try {
                        for (var ddi = 0; ddi < deleted.length; ddi++) {
                            var delPrefix = deleted[ddi] + '%';
                            _wSql.exec("DELETE FROM docs WHERE path MATCHES ?", [delPrefix]);
                            _wSql.exec("DELETE FROM paths WHERE path MATCHES ?", [delPrefix]);
                            _wSql.exec("DELETE FROM paths WHERE path = ?", [deleted[ddi]]);
                        }
                    } catch(e) {}
                }
                // Reset quota tracking to actual disk usage after wipe
                rampart.thread.put('demo_quota_init', false);

                if (u.stat(cfg.thumbDir))
                    u.shell("rm -rf '" + cfg.thumbDir.replace(/'/g, "'\\''") + "'");
                if (u.stat(cfg.uploadTmp)) {
                    var tmpEntries = u.readdir(cfg.uploadTmp, true);
                    if (tmpEntries) {
                        for (var ti = 0; ti < tmpEntries.length; ti++) {
                            if (tmpEntries[ti] === '.' || tmpEntries[ti] === '..') continue;
                            var tp = cfg.uploadTmp + '/' + tmpEntries[ti];
                            var ts = u.stat(tp);
                            if (ts && (now - ts.mtime.getTime()) / 1000 > cfg.interval) {
                                u.shell("rm -rf '" + tp.replace(/'/g, "'\\''") + "'");
                            }
                        }
                    }
                }
            }
        }, {
            home: DAV_ROOT + '/demo',
            thumbDir: dataRoot + '/webdav_thumbnails/demo',
            uploadTmp: dataRoot + '/webdav_uploads',
            searchDbPath: SEARCH_DB_PATH,
            interval: DEMO_CLEAR_TIME,
            dirs: ['Documents', 'Music', 'Pictures', 'Videos']
        });
    }
}// module.exports

// --- Search query (runs in server threads, reads from Texis DB) ---

// Open a read connection for queries (separate from the indexing thread's connection)
// Don't create — the indexing thread handles DB creation
var searchSql = stat(SEARCH_DB_PATH) ? new Sql.connection(SEARCH_DB_PATH) : null;

function searchQuery(query, username, isAdmin, maxRows, skipRows, subPath) {
    if (!query || !query.trim()) return {results: [], total: 0};
    maxRows = maxRows || 20;
    skipRows = skipRows || 0;
    var pathFilter = subPath ? subPath.replace(/\/?$/, '/') : null;

    // Connect lazily if the DB has been created by the indexing thread
    if (!searchSql && stat(SEARCH_DB_PATH)) {
        try { searchSql = new Sql.connection(SEARCH_DB_PATH); } catch(e) {}
    }
    if (!searchSql) return {results: [], total: 0};

    // Check that the docs table exists
    if (!searchSql.one("SELECT * FROM SYSTABLES WHERE NAME = ?", ['docs'])) {
        return {results: [], total: 0};
    }

    // If searching a specific directory, verify access once upfront
    if (pathFilter && !userCanSeePath(username, isAdmin, pathFilter)) {
        return {results: [], total: 0};
    }

    searchSql.set({
        suffixproc: true,
        minwordlen: 5,
        likepRows: 1000
    });

    try {
        // Build query: add path filter in SQL if searching a specific directory
        var sql, params;
        if (pathFilter) {
            sql = "SELECT path, title, " +
                "  stringformat('%mbH', ?q, abstract(content, 0, 'querymultiple', ?q)) AS snippet " +
                "FROM docs WHERE content LIKEP ?q AND path MATCHES ?dir";
            params = {q: query, dir: pathFilter + '%'};
        } else {
            sql = "SELECT path, title, " +
                "  stringformat('%mbH', ?q, abstract(content, 0, 'querymultiple', ?q)) AS snippet " +
                "FROM docs WHERE content LIKEP ?q";
            params = {q: query};
        }

        var res = searchSql.exec(sql,
            {maxRows: 1000, skipRows: skipRows, includeCounts: true},
            params
        );

        var results = [];
        if (res.rows) {
            // If searching a specific directory, we already checked access once above
            // For unfiltered search, check each result's visibility
            var needsAccessCheck = !pathFilter;
            for (var ri = 0; ri < res.rows.length; ri++) {
                var row = res.rows[ri];
                if (needsAccessCheck && !userCanSeePath(username, isAdmin, row.path)) continue;
                results.push({
                    path: row.path,
                    title: row.title,
                    snippet: row.snippet || '',
                    href: DAV_PREFIX + row.path
                });
            }
        }

        // If we filtered out rows (access check), use filtered count
        // If we hit the 1000 row limit, report -1 meaning "many"
        var total;
        if (results.length < res.rows.length) {
            // Some rows filtered out — use the filtered count
            total = results.length;
        } else if (res.rows.length >= 1000) {
            total = -1; // "many"
        } else {
            total = results.length;
        }

        return {
            results: results.slice(0, maxRows),
            total: total
        };
    } catch(e) {
        return {results: [], total: 0};
    }
}

// Get list of indexed directories a user can search
function searchGetUserDirs(username, isAdmin) {
    var allDirs = searchGetIndexedDirs();
    var dirs = [];
    for (var i = 0; i < allDirs.length; i++) {
        if (userCanSeePath(username, isAdmin, allDirs[i])) dirs.push(allDirs[i]);
    }
    return dirs;
}

// --- Filename search ---

function filenameSearch(query, username, isAdmin, maxRows, skipRows, subPath) {
    if (!query || !query.trim()) return {results: [], total: 0};
    maxRows = maxRows || 20;
    skipRows = skipRows || 0;
    var pathFilter = subPath ? subPath.replace(/\/?$/, '/') : null;

    if (!searchSql && stat(SEARCH_DB_PATH)) {
        try { searchSql = new Sql.connection(SEARCH_DB_PATH); } catch(e) {}
    }
    if (!searchSql) return {results: [], total: 0};
    if (!searchSql.one("SELECT * FROM SYSTABLES WHERE NAME = ?", ['paths'])) {
        return {results: [], total: 0};
    }

    // Check directory access upfront if path-filtered
    if (pathFilter && !userCanSeePath(username, isAdmin, pathFilter)) {
        return {results: [], total: 0};
    }

    query = query.trim();
    var sql, params;

    if (query.indexOf('*.') === 0) {
        // Extension search: *.jpg -> reverse search on pathrev
        var ext = query.substring(2); // "jpg"
        var revExt = ext.split('').reverse().join('') + '.'; // "gpj."
        if (pathFilter) {
            var revFilter = pathFilter.split('').reverse().join('');
            sql = "SELECT path, isdir FROM paths WHERE pathrev MATCHES ?rev AND path MATCHES ?dir";
            params = {rev: revExt + '%', dir: pathFilter + '%'};
        } else {
            sql = "SELECT path, isdir FROM paths WHERE pathrev MATCHES ?rev";
            params = {rev: revExt + '%'};
        }
        var res = searchSql.exec(sql, {maxRows: 1000, skipRows: skipRows}, params);
        var results = [];
        if (res.rows) {
            for (var i = 0; i < res.rows.length; i++) {
                var row = res.rows[i];
                if (!pathFilter && !userCanSeePath(username, isAdmin, row.path)) continue;
                results.push({path: row.path, isDir: !!row.isdir, href: DAV_PREFIX + row.path});
            }
        }
        var ftotal = results.length < res.rows.length ? results.length : (res.rows.length >= 1000 ? -1 : results.length);
        return {results: results.slice(0, maxRows), total: ftotal};

    } else if (query.indexOf('/') === 0) {
        // Exact path prefix search: /path/to/file* -> matches on path
        var pathQuery = query.replace(/\*$/, '') + '%';
        sql = "SELECT path, isdir FROM paths WHERE path MATCHES ?p";
        params = {p: pathQuery};
        var res = searchSql.exec(sql, {maxRows: 1000, skipRows: skipRows}, params);
        var results = [];
        if (res.rows) {
            for (var i = 0; i < res.rows.length; i++) {
                var row = res.rows[i];
                if (!userCanSeePath(username, isAdmin, row.path)) continue;
                results.push({path: row.path, isDir: !!row.isdir, href: DAV_PREFIX + row.path});
            }
        }
        var ftotal = results.length < res.rows.length ? results.length : (res.rows.length >= 1000 ? -1 : results.length);
        return {results: results.slice(0, maxRows), total: ftotal};

    } else {
        // Fulltext search on path: filename or partial path
        searchSql.set({suffixproc: false, likepRows: 1000});
        if (pathFilter) {
            sql = "SELECT path, isdir FROM paths WHERE path LIKEP ?q AND path MATCHES ?dir";
            params = {q: query, dir: pathFilter + '%'};
        } else {
            sql = "SELECT path, isdir FROM paths WHERE path LIKEP ?q";
            params = {q: query};
        }
        var res = searchSql.exec(sql, {maxRows: 1000, skipRows: skipRows, includeCounts: true}, params);
        var results = [];
        if (res.rows) {
            for (var i = 0; i < res.rows.length; i++) {
                var row = res.rows[i];
                if (!pathFilter && !userCanSeePath(username, isAdmin, row.path)) continue;
                results.push({path: row.path, isDir: !!row.isdir, href: DAV_PREFIX + row.path});
            }
        }
        var ftotal = results.length < res.rows.length ? results.length : (res.rows.length >= 1000 ? -1 : results.length);
        return {results: results.slice(0, maxRows), total: ftotal};
    }
}

/* ============================================================
 * Section 2: Utility Functions
 * ============================================================ */

// Check if a user can see a path based on top-level directory visibility.
// Users can see their own home directory and non-user directories (shared, etc.).
// Admins can see everything.
function userCanSeePath(username, isAdmin, davRelPath) {
    if (isAdmin) return true;
    var parts = davRelPath.replace(/^\/+/, '').split('/');
    if (parts.length < 1) return false;
    var topDir = parts[0];
    if (topDir === username) return true;
    // Non-user directory (not a username) is visible to all
    if (!db.get(userDbi, topDir)) return true;
    return false;
}

// Filter an array of objects with a 'path' key, returning only those
// the user has permission to see. Works for search results, locate results, etc.
function filterVisiblePaths(items, username, isAdmin) {
    if (isAdmin) return items;
    var result = [];
    for (var i = 0; i < items.length; i++) {
        if (userCanSeePath(username, isAdmin, items[i].path)) result.push(items[i]);
    }
    return result;
}

function generateUUID() {
    var bytes = crypto.rand(16);
    var hex = hexify(bytes).toLowerCase();
    return hex.substr(0, 8) + '-' + hex.substr(8, 4) + '-4' +
           hex.substr(13, 3) + '-a' + hex.substr(17, 3) + '-' +
           hex.substr(20, 12);
}

function generateETag(st) {
    return '"' + st.ino + '-' + st.size + '-' + st.mtime.getTime() + '"';
}

function formatRFC1123(d) {
    if (typeof d === 'string') d = new Date(d);
    return dateFmt("%a, %d %b %Y %H:%M:%S GMT", d);
}

function formatISO8601(d) {
    if (typeof d === 'string') d = new Date(d);
    return dateFmt("%Y-%m-%dT%H:%M:%SZ", d);
}

// Parse a human-readable duration or absolute date into an expiration result.
// Returns {seconds, expires} on success, or {error} on failure.
// Inspired by freq_to_sec() in rampart-sqlUpdate.js.
function parseDuration(s) {
    if (typeof s === 'number') return {seconds: s, expires: s > 0 ? new Date(Date.now() + s * 1000).toISOString() : null};
    if (typeof s !== 'string') return {error: 'Invalid input'};
    s = s.trim();
    if (!s) return {error: 'Empty input'};
    var sl = s.toLowerCase();

    // Named shortcuts
    if (sl === 'forever' || sl === 'never') return {seconds: 0, expires: null};
    switch (sl) {
        case 'daily':  return {seconds: 86400,  expires: new Date(Date.now() + 86400000).toISOString()};
        case 'hourly': return {seconds: 3600,   expires: new Date(Date.now() + 3600000).toISOString()};
        case 'weekly': return {seconds: 604800, expires: new Date(Date.now() + 604800000).toISOString()};
    }

    // Strip filler words and convert ordinals to cardinals for stringToNumber
    var ordinals = ['every','each','first','second','third','fourth','fifth','sixth',
                    'seventh','eighth','nineth','ninth','tenth','eleventh','twelfth'];
    var cardinals = ['','','one','two','three','four','five','six',
                     'seven','eight','nine','ten','eleven','twelve'];
    var f = sl;
    // Remove filler words
    f = f.replace(/\b(in|next|the|a|an|from now)\b/g, ' ').replace(/\s+/g, ' ').trim();
    for (var oi = 0; oi < ordinals.length; oi++) {
        f = f.split(ordinals[oi]).join(cardinals[oi]);
    }
    f = f.replace(/teenth/g, 'teen').replace(/ieth/g, 'y');

    // Try duration: number + time unit
    var res = stringToNumber(f, true);
    if (res && !res.min && !res.max) {
        if (!res.rem) res = {rem: f, value: 1};
        var rem = res.rem.trim();
        var perint = 0;
        if (rem.indexOf('second') !== -1 || rem.indexOf('sec') !== -1)      perint = 1;
        else if (rem.indexOf('minute') !== -1 || rem.indexOf('min') !== -1)  perint = 60;
        else if (rem.indexOf('hour') !== -1 || rem.indexOf('hr') !== -1)     perint = 3600;
        else if (rem.indexOf('day') !== -1)                                   perint = 86400;
        else if (rem.indexOf('week') !== -1)                                  perint = 604800;
        else if (rem.indexOf('month') !== -1 || rem.indexOf('mo') !== -1)    perint = 2592000;
        else if (rem.indexOf('year') !== -1 || rem.indexOf('yr') !== -1)     perint = 31536000;

        if (perint > 0) {
            var secs = Math.round(res.value * perint);
            return {seconds: secs, expires: new Date(Date.now() + secs * 1000).toISOString()};
        }
    }

    // Try absolute date via autoScanDate
    var asd = autoScanDate(s);
    if (asd && asd.date) {
        var target = asd.date.getTime();
        var now = Date.now();
        if (target <= now) return {error: 'Date is in the past'};
        var diffSec = Math.round((target - now) / 1000);
        return {seconds: diffSec, expires: asd.date.toISOString()};
    }

    return {error: 'Could not parse duration or date'};
}

function _formatSize(bytes) {
    if (bytes === 0) return '0 B';
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = Math.floor(Math.log(bytes) / Math.log(1024));
    if (i >= units.length) i = units.length - 1;
    return (bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1) + ' ' + units[i];
}

// Check if a path has any active (non-expired) share links.
// Caches the set of shared paths for 5 seconds to avoid repeated DB scans.
var _sharedPathsCache = null;
var _sharedPathsCacheTime = 0;
function getSharedPaths() {
    var now = Date.now();
    if (_sharedPathsCache && now - _sharedPathsCacheTime < 5000) return _sharedPathsCache;
    var allShares = db.get(sharesDbi, "", 10000);
    var paths = {};
    if (allShares) {
        var keys = Object.keys(allShares);
        for (var i = 0; i < keys.length; i++) {
            var rec = allShares[keys[i]];
            if (rec.expires && new Date(rec.expires) < new Date()) continue;
            paths[rec.path] = true;
        }
    }
    _sharedPathsCache = paths;
    _sharedPathsCacheTime = now;
    return paths;
}

// Invalidate shared paths cache (call after create/delete share)
function invalidateSharedPathsCache() {
    _sharedPathsCache = null;
}

// Generate a random share token
function generateShareToken() {
    var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var token = '';
    for (var i = 0; i < 32; i++) {
        token += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return token;
}

// Extract DAV-relative path from full request path (strip /dav prefix)
function getDavRelPath(reqPath) {
    if (reqPath.indexOf(DAV_PREFIX) === 0) {
        return reqPath.substring(DAV_PREFIX.length) || '/';
    }
    return reqPath || '/';
}

// Build filesystem path from DAV-relative path with traversal protection
function buildFsPath(davRelPath) {
    var decoded = decodeURIComponent(davRelPath);
    if (decoded.indexOf('..') !== -1) return null;
    var fsPath = DAV_ROOT + decoded;
    // Strip trailing slash for filesystem operations (except root)
    if (fsPath.length > DAV_ROOT.length + 1 && fsPath.charAt(fsPath.length - 1) === '/') {
        fsPath = fsPath.substring(0, fsPath.length - 1);
    }
    return fsPath;
}

// Encode a path for use in XML href elements
function encodeHref(path) {
    return path.split('/').map(function(seg) {
        return encodeURIComponent(seg);
    }).join('/');
}

function xmlEscape(s) {
    if (!s) return '';
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
                     .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Get MIME type for a file.
// 1. server.getMime(ext) — Rampart's built-in extension map
// 2. File metadata cache (stored per-file in LMDB)
// 3. `file --mime-type` detection, result stored in file metadata
// 4. Fallback: application/octet-stream (also stored so we don't retry)
function getMimeType(fsPath, davRelPath) {
    var dot = fsPath.lastIndexOf('.');
    var ext = dot !== -1 ? fsPath.substring(dot + 1).toLowerCase() : '';

    // 1. Check Rampart's built-in map
    if (ext) {
        var builtin = server.getMime(ext);
        if (builtin) return builtin;
    }

    // 2. Check per-file LMDB cache
    if (davRelPath) {
        var meta = getFileMeta(davRelPath);
        if (meta && meta.mimeType) return meta.mimeType;
    }

    // 3. Detect via `file --mime-type` — skip for remote/FUSE mounts (too slow)
    var fsStat = stat(fsPath);
    if (fsStat && fsStat.dev !== DAV_ROOT_DEV) {
        return 'application/octet-stream';
    }

    var detected = 'application/octet-stream';
    if (fsStat) {
        try {
            var res = shell("file --mime-type -b " + _shellEscape(fsPath), {timeout: 3000});
            var out = trim(res.stdout);
            if (out && out.indexOf('/') !== -1) {
                detected = out;
            }
        } catch(e) { /* use fallback */ }
    }

    // Store in file metadata
    if (davRelPath) {
        var meta2 = ensureFileMeta(davRelPath, fsPath);
        if (meta2) {
            meta2.mimeType = detected;
            setFileMeta(davRelPath, meta2);
        }
    }

    return detected;
}

// Get header value (case-insensitive lookup)
function getHeader(headers, name) {
    if (!headers) return undefined;
    var lower = name.toLowerCase();
    for (var key in headers) {
        if (headers.hasOwnProperty(key) && key.toLowerCase() === lower) {
            return headers[key];
        }
    }
    return undefined;
}

/* ============================================================
 * Section 2.5: Session Cookies & Authentication
 *
 * Cookie-based sessions using per-user encryption keys.
 * Cookie value = base64(encrypt({username, expires})) prefixed with "username:"
 * so we can look up the right key without decrypting first.
 * ============================================================ */

var SESSION_COOKIE_NAME = 'dav_session';
var DEFAULT_SESSION_SECONDS = 7200;  // 2 hours
var SESSION_REFRESH_SECONDS = 300;   // refresh cookie every 5 minutes

function ensureUserSessionKey(userRecord, username) {
    if (!userRecord.sessionKey) {
        userRecord.sessionKey = hexify(crypto.rand(32));
        userRecord.sessionTimeout = DEFAULT_SESSION_SECONDS;
        db.put(userDbi, username, userRecord);
    }
    if (typeof userRecord.sessionTimeout !== 'number') {
        userRecord.sessionTimeout = DEFAULT_SESSION_SECONDS;
        db.put(userDbi, username, userRecord);
    }
    return userRecord;
}

function cookieSecureFlag(req) {
    return req && req.path && req.path.scheme === 'https';
}

function createSessionCookie(username, userRecord, req) {
    ensureUserSessionKey(userRecord, username);
    var timeout = typeof userRecord.sessionTimeout === 'number' ? userRecord.sessionTimeout : DEFAULT_SESSION_SECONDS;
    // 0 = never expire
    var expires = (timeout === 0) ? 0 : Date.now() + (timeout * 1000);
    var payload = JSON.stringify({u: username, e: expires});
    var encrypted = crypto.encrypt({pass: userRecord.sessionKey, data: payload, cipher: 'aes-256-cbc'});
    var b64 = sprintf("%B", encrypted);
    var cookieVal = username + ':' + b64;
    // For never-expire, set max-age to 10 years
    var maxAge = (timeout === 0) ? 315360000 : timeout;
    var flags = cookieSecureFlag(req) ? 'HttpOnly; Secure; SameSite=Strict' : 'HttpOnly; SameSite=Strict';
    return sprintf("%s=%U; Max-Age=%d; Path=/; ", SESSION_COOKIE_NAME, cookieVal, maxAge) + flags;
}

function validateSessionCookie(cookieVal) {
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
        // expires === 0 means never expire
        if (payload.e !== 0 && payload.e < Date.now()) return null;
        var timeout = typeof userRecord.sessionTimeout === 'number' ? userRecord.sessionTimeout : DEFAULT_SESSION_SECONDS;
        var needsRefresh = (timeout !== 0) && (payload.e - Date.now()) < (timeout * 1000 - SESSION_REFRESH_SECONDS * 1000);
        return {
            username: username,
            admin: !!userRecord.admin,
            groups: userRecord.groups || [],
            _record: userRecord,
            _needsRefresh: needsRefresh
        };
    } catch(e) {
        return null;
    }
}

function clearSessionCookie(req) {
    var flags = cookieSecureFlag(req) ? 'HttpOnly; Secure; SameSite=Strict' : 'HttpOnly; SameSite=Strict';
    return sprintf("%s=; Max-Age=0; Path=/; ", SESSION_COOKIE_NAME) + flags;
}

// Return 401 headers: include WWW-Authenticate only for non-browser clients
// (if the browser sees it, it caches Basic Auth credentials that survive cookie revocation)
function make401Headers(req) {
    if (req.cookies && req.cookies[SESSION_COOKIE_NAME]) {
        // Browser session — bare 401, no Basic challenge
        return { 'Set-Cookie': clearSessionCookie(req) };
    }
    // Suppress Basic challenge for browser fetch/XHR requests to avoid
    // the native auth popup — the file manager handles 401 in JS.
    var sfm = getHeader(req.headers, 'Sec-Fetch-Mode');
    if (sfm === 'cors' || sfm === 'same-origin' || sfm === 'navigate') {
        return {};
    }
    return { 'WWW-Authenticate': 'Basic realm="WebDAV"' };
}

function authenticate(req) {
    // Try session cookie first
    if (req.cookies && req.cookies[SESSION_COOKIE_NAME]) {
        var sessionUser = validateSessionCookie(req.cookies[SESSION_COOKIE_NAME]);
        if (sessionUser) return sessionUser;
    }

    // Fall back to Basic Auth (for WebDAV clients, curl, etc.)
    var authHeader = getHeader(req.headers, 'Authorization');
    if (!authHeader) return null;

    var parts = authHeader.split(' ');
    if (parts.length !== 2 || parts[0] !== 'Basic') return null;

    var decoded;
    try {
        decoded = sprintf("%!B", parts[1]);
    } catch(e) {
        return null;
    }

    var colon = decoded.indexOf(':');
    if (colon === -1) return null;

    var username = decoded.substring(0, colon);
    var password = decoded.substring(colon + 1);
    if (!username || !password) return null;

    var userRecord = db.get(userDbi, username);
    if (!userRecord) return null;

    if (!crypto.passwdCheck(userRecord.hash_line, password)) return null;

    return {
        username: username,
        admin: !!userRecord.admin,
        groups: userRecord.groups || [],
        _record: userRecord,
        _basicAuth: true  // flag: set cookie on response
    };
}

function authorize(user, davRelPath, method) {
    var path = davRelPath;
    if (path.length > 1 && path.charAt(path.length - 1) === '/') {
        path = path.substring(0, path.length - 1);
    }

    // Root: only PROPFIND and OPTIONS allowed for regular users
    if (path === '/' || path === '') {
        if (method === 'PROPFIND' || method === 'OPTIONS') return true;
        return user.admin;
    }

    // Extract first path segment: /<owner>/...
    var segments = path.split('/');
    var pathOwner = segments[1];

    // Admins can access everything
    if (user.admin) return true;

    // Users can access their own home directory
    if (pathOwner === user.username) return true;

    // Non-user top-level directories (shared, shared2, etc.) are accessible to all
    if (!db.get(userDbi, pathOwner)) return true;

    return false;
}

// Check Unix-style permission bits from file metadata.
// Returns true if the user has the required access.
// need: 'r' for read, 'w' for write, 'x' for execute/traverse
function checkPermission(meta, user, need) {
    if (!meta) return true; // no metadata = allow (legacy files)
    if (user.admin) return true;

    // Determine which octal digit applies: owner / group / other
    var mode = meta.permissions || 0;
    var bits;
    if (meta.owner === user.username) {
        bits = Math.floor(mode / 100) % 10; // owner triad
    } else if (meta.group === 'everyone' ||
               (meta.group !== 'nogroup' && user.groups && user.groups.indexOf(meta.group) !== -1)) {
        bits = Math.floor(mode / 10) % 10;  // group triad
    } else {
        bits = mode % 10;                    // other triad
    }

    switch (need) {
        case 'r': return (bits & 4) !== 0;
        case 'w': return (bits & 2) !== 0;
        case 'x': return (bits & 1) !== 0;
        default:  return false;
    }
}

// High-level access check that combines path authorization with file permissions.
// Returns null if access is allowed, or a status code (403) if denied.
var WRITE_METHODS = ['PUT', 'DELETE', 'MKCOL', 'MOVE', 'PROPPATCH', 'LOCK'];
function checkAccess(user, davRelPath, fsPath, method) {
    if (user.admin) return null; // admins bypass
    // User paths: authorize() already verified ownership; no per-file permissions
    if (isUserPath(davRelPath)) return null;

    var isWrite = WRITE_METHODS.indexOf(method) !== -1;
    var fileStat = stat(fsPath);

    if (isWrite) {
        if (fileStat) {
            var meta = ensureFileMeta(davRelPath, fsPath);
            if (!checkPermission(meta, user, 'w'))
                return {status: 403, msg: 'You do not have write permission for this file'};
        } else {
            var parentDav = davRelPath.replace(/\/[^\/]*\/?$/, '') || '/';
            if (parentDav !== '/' && parentDav.charAt(parentDav.length - 1) !== '/') {
                parentDav += '/';
            }
            var parentFs = buildFsPath(parentDav);
            if (parentFs) {
                var parentMeta = ensureFileMeta(parentDav, parentFs);
                if (!checkPermission(parentMeta, user, 'w'))
                    return {status: 403, msg: 'You do not have write permission for the destination folder'};
            }
        }
    }
    // Read permission on source: GET, HEAD, COPY, MOVE all require read access
    if (method === 'GET' || method === 'HEAD' || method === 'COPY' || method === 'MOVE') {
        if (fileStat) {
            var readMeta = ensureFileMeta(davRelPath, fsPath);
            if (!checkPermission(readMeta, user, 'r'))
                return {status: 403, msg: 'You do not have read permission for this file'};
        }
    }
    // PROPFIND, OPTIONS, UNLOCK — allow (listing filtered separately)
    return null;
}

function ensureUserHome(username) {
    var homePath = DAV_ROOT + '/' + username;
    if (!stat(homePath)) {
        try {
            mkdir(homePath);
        } catch(e) {
            // Another thread may have created it between stat and mkdir
            if (!stat(homePath)) throw e;
        }
    }
    // Ensure default subdirectories
    var defaultDirs = ['Documents', 'Music', 'Pictures', 'Videos'];
    for (var di = 0; di < defaultDirs.length; di++) {
        var dirPath = homePath + '/' + defaultDirs[di];
        if (!stat(dirPath)) {
            try { mkdir(dirPath); } catch(e) {
                if (!stat(dirPath)) throw e;
            }
        }
    }
}

/* ============================================================
 * Section 3: Filesystem Helpers
 * ============================================================ */

function readdirAll(path) {
    return readdir(path, true).filter(function(n) { return n !== '.' && n !== '..'; });
}

function rmdirRecursive(path) {
    var entries = readdirAll(path);
    for (var i = 0; i < entries.length; i++) {
        var full = path + '/' + entries[i];
        var s = stat(full);
        if (s && s.isDirectory) {
            rmdirRecursive(full);
        } else if (s) {
            rmFile(full);
        }
    }
    rmdir(path);
}

// Fast recursive delete, restricted to paths inside the thumbnail directory.
// Uses rm -rf for performance. Symlink-friendly (no realPath resolution).
// Verifies the path is strictly inside THUMB_DIR with no .. traversal.
function rmdirThumbnails(path) {
    // Must start with THUMB_DIR/
    if (path.indexOf(THUMB_DIR + '/') !== 0) return;
    // The portion after THUMB_DIR must not contain .. traversal
    if (path.substring(THUMB_DIR.length).indexOf('..') !== -1) return;
    shell('rm -rf ' + _shellEscape(path));
}

function copyRecursive(src, dst) {
    var srcStat = stat(src);
    if (!srcStat) return false;
    if (srcStat.isDirectory) {
        try { mkdir(dst); } catch(e) {}
        var entries = readdirAll(src);
        for (var i = 0; i < entries.length; i++) {
            copyRecursive(src + '/' + entries[i], dst + '/' + entries[i]);
        }
    } else {
        copyFile(src, dst);
    }
    return true;
}

function ensureDirExists(path) {
    if (stat(path)) return;
    var parts = path.split('/');
    var current = '';
    for (var i = 0; i < parts.length; i++) {
        if (i === 0 && parts[i] === '') {
            current = '/';
            continue;
        }
        current += (current === '/' ? '' : '/') + parts[i];
        if (!stat(current)) {
            try { mkdir(current); } catch(e) {}
        }
    }
}

// Auto-rename a file to avoid overwriting: file.ext → file-1.ext → file-2.ext
function autoRenameFile(dir, name) {
    if (!stat(dir + '/' + name)) return name;
    var dot = name.lastIndexOf('.');
    var base = dot > 0 ? name.substring(0, dot) : name;
    var ext = dot > 0 ? name.substring(dot) : '';
    for (var n = 1; n < 1000; n++) {
        var candidate = base + '-' + n + ext;
        if (!stat(dir + '/' + candidate)) return candidate;
    }
    return base + '-' + Date.now() + ext;
}

function getParentDir(path) {
    var idx = path.lastIndexOf('/');
    if (idx <= 0) return '/';
    return path.substring(0, idx);
}

/* ============================================================
 * Section 4: XML Parser (regex-based, for WebDAV request bodies)
 * ============================================================ */

function parseWebDAVXml(xmlStr) {
    if (!xmlStr || !xmlStr.trim()) return { namespaces: {}, root: null };

    // Strip XML declaration
    xmlStr = xmlStr.replace(/<\?xml[^?]*\?>\s*/, '');

    // Collect namespace prefix mappings
    var nsMap = {};
    var nsRegex = /xmlns:([A-Za-z][A-Za-z0-9]*)="([^"]*)"/g;
    var m;
    while ((m = nsRegex.exec(xmlStr)) !== null) {
        nsMap[m[1]] = m[2];
    }
    var defNs = xmlStr.match(/xmlns="([^"]*)"/);
    if (defNs) nsMap[''] = defNs[1];

    // Strip all namespace prefixes and xmlns attributes
    var clean = xmlStr
        .replace(/\s*xmlns(?::[A-Za-z][A-Za-z0-9]*)?="[^"]*"/g, '')
        .replace(/<\/?([A-Za-z][A-Za-z0-9]*):(?=[A-Za-z])/g, function(match) {
            return match.indexOf('/') === 1 ? '</' : '<';
        });

    function parseElement(str, pos) {
        var elements = [];
        while (pos < str.length) {
            var tagStart = str.indexOf('<', pos);
            if (tagStart === -1) break;
            if (str.charAt(tagStart + 1) === '/') break;

            var tagMatch = str.substring(tagStart).match(/^<([A-Za-z][A-Za-z0-9_:-]*)((?:\s+[^>]*?)?)(\/?)\s*>/);
            if (!tagMatch) { pos = tagStart + 1; continue; }

            var tagName = tagMatch[1].toLowerCase();
            var selfClosing = tagMatch[3] === '/';
            var afterTag = tagStart + tagMatch[0].length;

            var node = { tag: tagName, children: [], text: '' };

            if (selfClosing) {
                elements.push(node);
                pos = afterTag;
            } else {
                var closeTag = '</' + tagMatch[1] + '>';
                var closeIdx = str.indexOf(closeTag, afterTag);
                // Also try lowercase
                if (closeIdx === -1) closeIdx = str.indexOf('</' + tagName + '>', afterTag);
                if (closeIdx === -1) {
                    elements.push(node);
                    pos = afterTag;
                    continue;
                }

                var content = str.substring(afterTag, closeIdx);
                if (content.indexOf('<') !== -1) {
                    var result = parseElement(str, afterTag);
                    node.children = result.elements;
                } else {
                    node.text = content.trim();
                }

                elements.push(node);
                pos = closeIdx + closeTag.length;
                if (pos <= closeIdx) pos = str.indexOf('>', closeIdx) + 1;
            }
        }
        return { elements: elements, pos: pos };
    }

    var result = parseElement(clean, 0);
    return { namespaces: nsMap, root: result.elements[0] || null };
}

// Find a named tag in parsed XML tree
function findXmlTag(node, name) {
    if (!node) return null;
    if (node.tag === name) return node;
    for (var i = 0; i < node.children.length; i++) {
        var found = findXmlTag(node.children[i], name);
        if (found) return found;
    }
    return null;
}

function parsePropfindBody(xmlStr) {
    if (!xmlStr || !xmlStr.trim()) {
        return { type: 'allprop' }; // empty body = allprop per RFC 4918
    }
    var parsed = parseWebDAVXml(xmlStr);
    if (!parsed.root) return { type: 'allprop' };

    var root = parsed.root;
    for (var i = 0; i < root.children.length; i++) {
        var child = root.children[i];
        if (child.tag === 'allprop') return { type: 'allprop' };
        if (child.tag === 'propname') return { type: 'propname' };
        if (child.tag === 'prop') {
            var names = [];
            for (var j = 0; j < child.children.length; j++) {
                names.push(child.children[j].tag);
            }
            return { type: 'prop', names: names };
        }
    }
    return { type: 'allprop' };
}

function parseLockBody(xmlStr) {
    var result = { scope: 'exclusive', type: 'write', owner: '' };
    if (!xmlStr || !xmlStr.trim()) return result;
    var parsed = parseWebDAVXml(xmlStr);
    if (parsed.root) {
        var scope = findXmlTag(parsed.root, 'lockscope');
        if (scope && scope.children.length) result.scope = scope.children[0].tag;
        var owner = findXmlTag(parsed.root, 'owner');
        if (owner) {
            var href = findXmlTag(owner, 'href');
            result.owner = href ? href.text : (owner.text || '');
        }
    }
    return result;
}

function parseProppatchBody(xmlStr) {
    var ops = { set: [], remove: [] };
    if (!xmlStr || !xmlStr.trim()) return ops;
    var parsed = parseWebDAVXml(xmlStr);
    if (!parsed.root) return ops;

    for (var i = 0; i < parsed.root.children.length; i++) {
        var child = parsed.root.children[i];
        if (child.tag === 'set') {
            var prop = findXmlTag(child, 'prop');
            if (prop) {
                for (var j = 0; j < prop.children.length; j++) {
                    ops.set.push({ name: prop.children[j].tag, value: prop.children[j].text });
                }
            }
        } else if (child.tag === 'remove') {
            var rprop = findXmlTag(child, 'prop');
            if (rprop) {
                for (var k = 0; k < rprop.children.length; k++) {
                    ops.remove.push({ name: rprop.children[k].tag });
                }
            }
        }
    }
    return ops;
}

/* ============================================================
 * Section 5: XML Response Generators
 * ============================================================ */

function xmlMultiStatus(responses) {
    var xml = '<?xml version="1.0" encoding="utf-8"?>\n';
    xml += '<D:multistatus xmlns:D="DAV:" xmlns:R="urn:rampart:dav">\n';
    for (var i = 0; i < responses.length; i++) {
        xml += responses[i];
    }
    xml += '</D:multistatus>';
    return xml;
}

function xmlResponse(href, propstats) {
    var xml = '<D:response>\n';
    xml += '<D:href>' + xmlEscape(href) + '</D:href>\n';
    for (var i = 0; i < propstats.length; i++) {
        xml += propstats[i];
    }
    xml += '</D:response>\n';
    return xml;
}

function xmlPropstat(props, status) {
    var xml = '<D:propstat>\n<D:prop>\n';
    xml += props;
    xml += '</D:prop>\n';
    xml += '<D:status>HTTP/1.1 ' + status + '</D:status>\n';
    xml += '</D:propstat>\n';
    return xml;
}

function buildActiveLockXml(lockInfo) {
    return '<D:activelock>\n' +
        '<D:locktype><D:write/></D:locktype>\n' +
        '<D:lockscope><D:' + lockInfo.scope + '/></D:lockscope>\n' +
        '<D:depth>' + (lockInfo.depth || '0') + '</D:depth>\n' +
        '<D:owner>' + (lockInfo.owner ? '<D:href>' + xmlEscape(lockInfo.owner) + '</D:href>' : '') + '</D:owner>\n' +
        '<D:timeout>Second-' + lockInfo.timeout + '</D:timeout>\n' +
        '<D:locktoken><D:href>' + lockInfo.token + '</D:href></D:locktoken>\n' +
        '</D:activelock>\n';
}

function buildLockResponseXml(lockInfo) {
    return '<?xml version="1.0" encoding="utf-8"?>\n' +
        '<D:prop xmlns:D="DAV:">\n' +
        '<D:lockdiscovery>\n' +
        buildActiveLockXml(lockInfo) +
        '</D:lockdiscovery>\n' +
        '</D:prop>';
}

// Standard live DAV property names
var LIVE_PROPS = ['displayname', 'resourcetype', 'getcontentlength', 'getcontenttype',
    'getlastmodified', 'creationdate', 'getetag', 'supportedlock', 'lockdiscovery'];

function buildResourceResponse(href, fsPath, st, propReq) {
    var foundProps = '';
    var notFoundProps = '';

    var allProps = (propReq.type === 'allprop');
    var propnameOnly = (propReq.type === 'propname');
    var requestedNames = propReq.names || [];

    function wantProp(name) {
        return allProps || propnameOnly || requestedNames.indexOf(name) !== -1;
    }

    if (propnameOnly) {
        // Return just the property names with empty elements
        for (var p = 0; p < LIVE_PROPS.length; p++) {
            foundProps += '<D:' + LIVE_PROPS[p] + '/>\n';
        }
        var deadProps = loadDeadProps(getDavRelPath(href));
        for (var dk in deadProps) {
            if (deadProps.hasOwnProperty(dk)) {
                foundProps += '<' + dk + '/>\n';
            }
        }
        var pls = lstat(fsPath);
        if (pls && pls.isSymbolicLink) {
            foundProps += '<R:symlink/>\n';
        }
        foundProps += '<R:owner/>\n<R:permissions/>\n<R:group/>\n';
    } else {
        if (wantProp('displayname')) {
            // Strip trailing slash for collections before extracting name
            var hrefForName = href;
            if (hrefForName.length > 1 && hrefForName.charAt(hrefForName.length - 1) === '/') {
                hrefForName = hrefForName.substring(0, hrefForName.length - 1);
            }
            var name = decodeURIComponent(hrefForName.substring(hrefForName.lastIndexOf('/') + 1)) || '';
            foundProps += '<D:displayname>' + xmlEscape(name) + '</D:displayname>\n';
        }
        if (wantProp('resourcetype')) {
            foundProps += '<D:resourcetype>' + (st.isDirectory ? '<D:collection/>' : '') + '</D:resourcetype>\n';
        }
        if (wantProp('getcontentlength')) {
            foundProps += '<D:getcontentlength>' + (st.isDirectory ? 0 : st.size) + '</D:getcontentlength>\n';
        }
        if (wantProp('getcontenttype')) {
            foundProps += '<D:getcontenttype>' + (st.isDirectory ? 'httpd/unix-directory' : getMimeType(fsPath, getDavRelPath(href))) + '</D:getcontenttype>\n';
        }
        if (wantProp('getlastmodified')) {
            foundProps += '<D:getlastmodified>' + formatRFC1123(st.mtime) + '</D:getlastmodified>\n';
        }
        if (wantProp('creationdate')) {
            foundProps += '<D:creationdate>' + formatISO8601(st.ctime) + '</D:creationdate>\n';
        }
        if (wantProp('getetag')) {
            foundProps += '<D:getetag>' + generateETag(st) + '</D:getetag>\n';
        }
        if (wantProp('supportedlock')) {
            foundProps += '<D:supportedlock>' +
                '<D:lockentry><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockentry>' +
                '<D:lockentry><D:lockscope><D:shared/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockentry>' +
                '</D:supportedlock>\n';
        }
        if (wantProp('lockdiscovery')) {
            var lockInfo = getLock(href);
            foundProps += '<D:lockdiscovery>';
            if (lockInfo) {
                foundProps += buildActiveLockXml(lockInfo);
            }
            foundProps += '</D:lockdiscovery>\n';
        }

        // Dead properties
        if (allProps) {
            var dead = loadDeadProps(getDavRelPath(href));
            for (var key in dead) {
                if (dead.hasOwnProperty(key)) {
                    foundProps += '<' + key + '>' + xmlEscape(dead[key]) + '</' + key + '>\n';
                }
            }
        } else if (requestedNames.length > 0) {
            // Check for requested dead props
            var deadCheck = loadDeadProps(getDavRelPath(href));
            for (var n = 0; n < requestedNames.length; n++) {
                var rn = requestedNames[n];
                if (LIVE_PROPS.indexOf(rn) === -1) {
                    if (deadCheck.hasOwnProperty(rn)) {
                        foundProps += '<' + rn + '>' + xmlEscape(deadCheck[rn]) + '</' + rn + '>\n';
                    }
                    // Silently skip unknown properties rather than returning
                    // them in a 404 propstat without proper namespace context
                }
            }
        }

        // Symlink detection — emit custom property for our frontend
        var ls = lstat(fsPath);
        if (ls && ls.isSymbolicLink) {
            var linkDavPath = '';
            var linkBroken = false;
            var resolvedTarget;
            try { resolvedTarget = realPath(fsPath); } catch(e) { resolvedTarget = null; }
            if (resolvedTarget) {
                var davRoot = realPath(DAV_ROOT);
                if (resolvedTarget.indexOf(davRoot + '/') === 0) {
                    linkDavPath = resolvedTarget.substring(davRoot.length);
                } else if (resolvedTarget === davRoot) {
                    linkDavPath = '/';
                } else {
                    linkDavPath = resolvedTarget;
                }
            } else {
                linkBroken = true;
            }
            foundProps += '<R:symlink><R:target>' + xmlEscape(linkDavPath) + '</R:target>' +
                (linkBroken ? '<R:broken>true</R:broken>' : '') +
                '</R:symlink>\n';
        }

        // Owner and permissions from file metadata
        if (wantProp('owner') || wantProp('permissions') || wantProp('group')) {
            var davRel = getDavRelPath(href);
            var fileMeta = ensureFileMeta(davRel, fsPath);
            if (wantProp('owner')) {
                foundProps += '<R:owner>' + xmlEscape(fileMeta ? fileMeta.owner : 'unknown') + '</R:owner>\n';
            }
            if (wantProp('permissions')) {
                foundProps += '<R:permissions>' + (fileMeta ? fileMeta.permissions : 0) + '</R:permissions>\n';
            }
            if (wantProp('group')) {
                foundProps += '<R:group>' + xmlEscape(fileMeta ? fileMeta.group : 'nogroup') + '</R:group>\n';
            }
        }

        // Filesystem-level readable/writable (from OS stat, useful for mounted volumes)
        if (wantProp('fsreadable') || wantProp('fswritable') || allProps) {
            var fsR = st.readable !== false;
            var fsW = st.writable !== false;
            // Check if this path is inside a read-only mount
            if (fsW) {
                var roRel = getDavRelPath(href);
                var roParts = roRel.split('/');
                // User mount: /username/mountName/... → key username/mountName
                if (roParts.length >= 3 && roParts[1] && roParts[2]) {
                    var roCfg = db.get(rcloneDbi, roParts[1] + '/' + roParts[2]);
                    if (roCfg && roCfg.readOnly) fsW = false;
                }
                // Root mount: /mountName/... → key _rootmount/mountName
                if (fsW && roParts.length >= 2 && roParts[1]) {
                    var roRoot = db.get(rcloneDbi, '_rootmount/' + roParts[1]);
                    if (roRoot && roRoot.readOnly) fsW = false;
                }
            }
            if (!fsR || !fsW) {
                foundProps += '<R:fsreadable>' + (fsR ? '1' : '0') + '</R:fsreadable>\n';
                foundProps += '<R:fswritable>' + (fsW ? '1' : '0') + '</R:fswritable>\n';
            }
        }

        // Shared link indicator
        if (wantProp('shared')) {
            var shDavRel = getDavRelPath(href);
            var sharedPaths = getSharedPaths();
            foundProps += '<R:shared>' + (sharedPaths[shDavRel] ? '1' : '0') + '</R:shared>\n';
        }
    }

    var propstats = [];
    if (foundProps) {
        propstats.push(xmlPropstat(foundProps, '200 OK'));
    }
    if (notFoundProps) {
        propstats.push(xmlPropstat(notFoundProps, '404 Not Found'));
    }

    return xmlResponse(href, propstats);
}

/* ============================================================
 * Section 6: Lock Manager
 * ============================================================ */

function acquireLock(davPath, scope, type, owner, depth, timeout) {
    thrlock.lock();
    try {
        var locks = rampart.thread.get(LOCK_KEY) || {};
        // Check for conflicts
        if (locks[davPath]) {
            var existing = locks[davPath];
            // Check if expired
            if (Date.now() - existing.created > existing.timeout * 1000) {
                delete locks[davPath];
            } else if (existing.scope === 'exclusive' || scope === 'exclusive') {
                thrlock.unlock();
                return null; // Conflict
            }
        }
        var token = 'urn:uuid:' + generateUUID();
        locks[davPath] = {
            token: token, scope: scope, type: type,
            owner: owner, depth: depth,
            timeout: timeout, created: Date.now()
        };
        rampart.thread.put(LOCK_KEY, locks);
        thrlock.unlock();
        return locks[davPath];
    } catch(e) {
        thrlock.unlock();
        throw e;
    }
}

function releaseLock(davPath, token) {
    thrlock.lock();
    try {
        var locks = rampart.thread.get(LOCK_KEY) || {};
        if (locks[davPath] && locks[davPath].token === token) {
            delete locks[davPath];
            rampart.thread.put(LOCK_KEY, locks);
            thrlock.unlock();
            return true;
        }
        thrlock.unlock();
        return false;
    } catch(e) {
        thrlock.unlock();
        throw e;
    }
}

function refreshLock(davPath, token, timeout) {
    thrlock.lock();
    try {
        var locks = rampart.thread.get(LOCK_KEY) || {};
        if (locks[davPath] && locks[davPath].token === token) {
            locks[davPath].timeout = timeout;
            locks[davPath].created = Date.now();
            rampart.thread.put(LOCK_KEY, locks);
            thrlock.unlock();
            return locks[davPath];
        }
        thrlock.unlock();
        return null;
    } catch(e) {
        thrlock.unlock();
        throw e;
    }
}

function getLock(davPath) {
    var locks = rampart.thread.get(LOCK_KEY) || {};
    var l = locks[davPath];
    if (l && (Date.now() - l.created > l.timeout * 1000)) {
        return null; // expired
    }
    return l || null;
}

function parseIfHeader(ifHeader) {
    if (!ifHeader) return [];
    var tokens = [];
    var re = /\(<([^>]+)>\)/g;
    var m;
    while ((m = re.exec(ifHeader)) !== null) {
        tokens.push(m[1]);
    }
    return tokens;
}

// Check if a write operation is allowed given current locks
function checkLockForWrite(davHref, headers) {
    var lockInfo = getLock(davHref);
    if (!lockInfo) return true; // no lock, write allowed

    // Lock exists - caller must present the lock token in If header
    var ifHeader = getHeader(headers, 'If');
    if (!ifHeader) return false;

    var tokens = parseIfHeader(ifHeader);
    for (var i = 0; i < tokens.length; i++) {
        if (tokens[i] === lockInfo.token) return true;
    }
    return false;
}

/* ============================================================
 * Section 7: Dead Properties Store (LMDB)
 * ============================================================ */

function loadDeadProps(davRelPath) {
    var val = db.get(propsDbi, davRelPath);
    return val || {};
}

function saveDeadProps(davRelPath, props) {
    if (Object.keys(props).length === 0) {
        db.del(propsDbi, davRelPath);
    } else {
        db.put(propsDbi, davRelPath, props);
    }
}

function removeDeadProps(davRelPath) {
    db.del(propsDbi, davRelPath);
}

function removeDeadPropsRecursive(davRelPath) {
    removeDeadProps(davRelPath);
    // Delete all props with this path as prefix (children of collection)
    var prefix = davRelPath;
    if (prefix.charAt(prefix.length - 1) !== '/') prefix += '/';
    var txn = new db.transaction(propsDbi, true);
    var entry = txn.cursorGet(db.op_setRange, prefix, true);
    while (entry) {
        if (entry.key === undefined || entry.key === null) break;
        var k = typeof entry.key === 'string' ? entry.key : bufferToString(entry.key);
        if (k.indexOf(prefix) !== 0) break;
        txn.cursorDel();
        entry = txn.cursorNext(true);
    }
    txn.commit();
}

function moveDeadProps(srcDavPath, dstDavPath) {
    // Move props for the resource itself
    var props = loadDeadProps(srcDavPath);
    removeDeadProps(srcDavPath);
    if (Object.keys(props).length > 0) {
        saveDeadProps(dstDavPath, props);
    }
    // Move all child props (collection subtree)
    var srcPrefix = srcDavPath;
    if (srcPrefix.charAt(srcPrefix.length - 1) !== '/') srcPrefix += '/';
    var txn = new db.transaction(propsDbi, true);
    var entry = txn.cursorGet(db.op_setRange, srcPrefix, true);
    var toMove = [];
    while (entry) {
        if (!entry.key) break;
        var k = typeof entry.key === 'string' ? entry.key : bufferToString(entry.key);
        if (k.indexOf(srcPrefix) !== 0) break;
        toMove.push({key: k, value: entry.value});
        entry = txn.cursorNext(true);
    }
    for (var i = 0; i < toMove.length; i++) {
        var newKey = dstDavPath + '/' + toMove[i].key.substring(srcPrefix.length);
        txn.put(newKey, toMove[i].value);
        txn.del(toMove[i].key);
    }
    txn.commit();
}

function copyDeadPropsRecursive(srcDavPath, dstDavPath) {
    // Copy the resource's own props
    var props = loadDeadProps(srcDavPath);
    if (Object.keys(props).length > 0) {
        saveDeadProps(dstDavPath, props);
    }
    // Copy all child props
    var srcPrefix = srcDavPath;
    if (srcPrefix.charAt(srcPrefix.length - 1) !== '/') srcPrefix += '/';
    var txn = new db.transaction(propsDbi, false);
    var entry = txn.cursorGet(db.op_setRange, srcPrefix, true);
    var toCopy = [];
    while (entry) {
        if (!entry.key) break;
        var k = typeof entry.key === 'string' ? entry.key : bufferToString(entry.key);
        if (k.indexOf(srcPrefix) !== 0) break;
        toCopy.push({key: k, value: entry.value});
        entry = txn.cursorNext(true);
    }
    txn.abort();
    // Write copies in a write transaction
    if (toCopy.length > 0) {
        var wtxn = new db.transaction(propsDbi, true);
        for (var i = 0; i < toCopy.length; i++) {
            var newKey = dstDavPath + '/' + toCopy[i].key.substring(srcPrefix.length);
            wtxn.put(newKey, toCopy[i].value);
        }
        wtxn.commit();
    }
}

// ---- Demo Mode helpers ----
function demoGetDirSize(dirPath) {
    var total = 0;
    var entries;
    try { entries = readdir(dirPath, true); } catch(e) { return 0; }
    if (!entries) return 0;
    for (var i = 0; i < entries.length; i++) {
        if (entries[i] === '.' || entries[i] === '..') continue;
        var p = dirPath + '/' + entries[i];
        var s = stat(p);
        if (!s) continue;
        if (s.isDirectory) total += demoGetDirSize(p);
        else total += s.size;
    }
    return total;
}

// Initialize demo quota tracking on first call
function demoInitQuota() {
    if (rampart.thread.get('demo_quota_init')) return;
    thrlock.lock();
    if (!rampart.thread.get('demo_quota_init')) {
        var demoHome = DAV_ROOT + '/demo';
        var used = demoGetDirSize(demoHome);
        rampart.thread.put('demo_quota_used', used);
        rampart.thread.put('demo_quota_init', true);
    }
    thrlock.unlock();
}

function demoAddQuota(bytes) {
    thrlock.lock();
    var used = rampart.thread.get('demo_quota_used') || 0;
    rampart.thread.put('demo_quota_used', used + bytes);
    thrlock.unlock();
}

function demoSubQuota(bytes) {
    thrlock.lock();
    var used = rampart.thread.get('demo_quota_used') || 0;
    rampart.thread.put('demo_quota_used', Math.max(0, used - bytes));
    thrlock.unlock();
}

function demoCheckQuota() {
    if (!DEMO_MODE) return true;
    demoInitQuota();
    var used = rampart.thread.get('demo_quota_used') || 0;
    return used < DEMO_MAX_QUOTA;
}

function demoIsProtectedPath(davRelPath) {
    if (!DEMO_MODE) return false;
    // Protect demo-files directory from writes
    return davRelPath === '/' + DEMO_FILES_DIR ||
           davRelPath.indexOf('/' + DEMO_FILES_DIR + '/') === 0;
}

/* ============================================================
 * Section 8: Method Handlers
 * ============================================================ */

function handleOPTIONS(req, davRelPath, fsPath) {
    return {
        status: 200,
        headers: {
            'DAV': '1, 2',
            'Allow': SUPPORTED_METHODS,
            'MS-Author-Via': 'DAV',
            'Content-Length': '0'
        },
        txt: ''
    };
}

function handleGET(req, davRelPath, fsPath) {
    var st = stat(fsPath);
    if (!st) return { status: 404, txt: 'Not Found' };
    if (st.isDirectory) {
        return { status: 405, txt: 'Method Not Allowed on collections' };
    }
    var etag = generateETag(st);
    var lastMod = formatRFC1123(st.mtime);

    var dot = fsPath.lastIndexOf('.');
    var ext = dot !== -1 ? fsPath.substring(dot + 1).toLowerCase() : '';
    var key = (ext && server.getMime(ext)) ? ext : 'bin';
    var ua = req.headers['User-Agent'] || req.headers['user-agent'] || '';
    var rangeHdr = req.headers['Range'] || req.headers['range'] || '';
    var resp = {
        status: 200,
        headers: {
            'ETag': etag,
            'Last-Modified': lastMod
        }
    };
    if (key === 'bin') {
        resp.headers['Content-Type'] = getMimeType(fsPath, davRelPath);
    }
    // For WebDAV mount clients:
    // - noRangeCap: don't cap open-ended Range responses (so they cache the full file)
    // - noAcceptRanges: suppress Accept-Ranges header on non-Range GETs to prevent
    //   macOS WebDAVFS from issuing parallel Range requests that starve the main download
    if (ua.indexOf('WebDAVFS') !== -1 || ua.indexOf('davfs2') !== -1) {
        resp.noRangeCap = true;
        if (!rangeHdr) {
            resp.noAcceptRanges = true;
        }
    }
    resp[key] = '@' + fsPath;
    return resp;
}

function handleHEAD(req, davRelPath, fsPath) {
    var st = stat(fsPath);
    if (!st) return { status: 404, txt: '' };
    if (st.isDirectory) {
        return { status: 405, txt: '' };
    }
    var etag = generateETag(st);
    var lastMod = formatRFC1123(st.mtime);

    return {
        status: 200,
        headers: {
            'Content-Type': getMimeType(fsPath, davRelPath),
            'Content-Length': String(st.size),
            'ETag': etag,
            'Last-Modified': lastMod,
            'Accept-Ranges': 'bytes'
        },
        txt: ''
    };
}

function handlePUT(req, davRelPath, fsPath) {
    // Demo mode: protect demo-files, enforce size limit and quota
    if (DEMO_MODE) {
        if (demoIsProtectedPath(davRelPath))
            return { status: 403, txt: 'Demo: this directory is read-only' };
        var contentLength = parseInt(getHeader(req.headers, 'Content-Length') || '0');
        if (contentLength > DEMO_MAX_FILE_SIZE)
            return { status: 413, txt: 'Demo: file exceeds ' + Math.round(DEMO_MAX_FILE_SIZE/1024/1024) + 'MB limit' };
        if (req.body && req.body.length > DEMO_MAX_FILE_SIZE)
            return { status: 413, txt: 'Demo: file exceeds ' + Math.round(DEMO_MAX_FILE_SIZE/1024/1024) + 'MB limit' };
        if (!demoCheckQuota())
            return { status: 507, txt: 'Demo: storage quota exceeded' };
    }

    var davHref = DAV_PREFIX + davRelPath;
    if (!checkLockForWrite(davHref, req.headers)) {
        return { status: 423, txt: 'Locked' };
    }

    // Chunked upload: presence of X-Upload-Id header triggers chunk handling
    var uploadId = getHeader(req.headers, 'X-Upload-Id');
    if (uploadId) {
        return handleChunkedPUT(req, davRelPath, fsPath, uploadId);
    }

    var parentDir = getParentDir(fsPath);
    if (!stat(parentDir)) {
        return { status: 409, txt: 'Conflict - parent collection does not exist' };
    }

    var existed = !!stat(fsPath);
    // Ensure we don't overwrite a collection
    if (existed) {
        var existSt = stat(fsPath);
        if (existSt.isDirectory) {
            return { status: 405, txt: 'Cannot PUT to a collection' };
        }
    }

    // Track old size for quota adjustment
    var oldSize = 0;
    if (DEMO_MODE && existed) {
        var oldSt = stat(fsPath);
        if (oldSt) oldSize = oldSt.size;
    }

    // Write the body to the file
    try {
        var fp = fopen(fsPath, 'w+');
        if (req.body && req.body.length > 0) {
            fwrite(fp, req.body);
        }
        fclose(fp);
    } catch(e) {
        return { status: 403, txt: 'Write failed: ' + (e.message || 'permission denied') };
    }

    // Update demo quota tracking
    if (DEMO_MODE) {
        var newSize = req.body ? req.body.length : 0;
        demoAddQuota(newSize - oldSize);
    }

    var status = existed ? 204 : 201;
    if (!existed) {
        createFileMeta(davRelPath, req.davUser.username, false);
    }
    generateThumbnail(fsPath, davRelPath);
    searchIndexFile(fsPath, davRelPath);

    var st = stat(fsPath);
    return {
        status: status,
        headers: {
            'ETag': generateETag(st)
        },
        txt: ''
    };
}

/*
 * Chunked upload handler.
 *
 * Client sends file in small chunks, each as a separate PUT with headers:
 *   X-Upload-Id:     unique ID for this upload session
 *   X-Chunk-Offset:  byte offset of this chunk within the file
 *   X-Total-Size:    total file size in bytes
 *
 * Chunks are written to a temp file at the correct offset via fseek.
 * When the temp file reaches the expected total size, it is renamed
 * to the final destination path.
 */
function handleChunkedPUT(req, davRelPath, fsPath, uploadId) {
    if (!/^[a-zA-Z0-9\-]+$/.test(uploadId)) {
        return { status: 400, txt: 'Invalid upload ID' };
    }

    var offset    = parseInt(getHeader(req.headers, 'X-Chunk-Offset') || '0', 10);
    var totalSize = parseInt(getHeader(req.headers, 'X-Total-Size')   || '0', 10);

    if (isNaN(offset) || isNaN(totalSize) || totalSize <= 0) {
        return { status: 400, txt: 'Missing or invalid chunk headers' };
    }

    // Demo mode: enforce file size limit on chunked uploads
    if (DEMO_MODE) {
        if (demoIsProtectedPath(davRelPath))
            return { status: 403, txt: 'Demo: this directory is read-only' };
        if (totalSize > DEMO_MAX_FILE_SIZE)
            return { status: 413, txt: 'Demo: file exceeds ' + Math.round(DEMO_MAX_FILE_SIZE/1024/1024) + 'MB limit' };
        if (!demoCheckQuota())
            return { status: 507, txt: 'Demo: storage quota exceeded' };
    }

    var parentDir = getParentDir(fsPath);
    if (!stat(parentDir)) {
        return { status: 409, txt: 'Conflict - parent collection does not exist' };
    }

    var existSt = stat(fsPath);
    if (existSt && existSt.isDirectory) {
        return { status: 405, txt: 'Cannot PUT to a collection' };
    }
    var existed = !!existSt;

    var tmpPath = UPLOAD_TMP + '/' + uploadId;
    if (offset > 0 && !stat(tmpPath)) {
        return { status: 409, txt: 'Upload session not found (missing earlier chunks)' };
    }

    var chunkSize = (req.body && req.body.length > 0) ? req.body.length : 0;
    if (chunkSize > 0) {
        fprintf(tmpPath, true, "%s", req.body);
    }

    // Demo mode: check actual accumulated size and update quota
    if (DEMO_MODE) {
        demoAddQuota(chunkSize);
        var tmpCheck = stat(tmpPath);
        if (tmpCheck && tmpCheck.size > DEMO_MAX_FILE_SIZE) {
            demoSubQuota(tmpCheck.size);
            try { rmFile(tmpPath); } catch(e) {}
            return { status: 413, txt: 'Demo: file exceeds ' + Math.round(DEMO_MAX_FILE_SIZE/1024/1024) + 'MB limit' };
        }
        if (!demoCheckQuota()) {
            demoSubQuota(tmpCheck ? tmpCheck.size : chunkSize);
            try { rmFile(tmpPath); } catch(e) {}
            return { status: 507, txt: 'Demo: storage quota exceeded' };
        }
    }

    // Check if the upload is now complete
    var tmpSt = stat(tmpPath);
    if (tmpSt && tmpSt.size >= totalSize) {
        rename(tmpPath, fsPath);
        if (!existed) {
            createFileMeta(davRelPath, req.davUser.username, false);
        }
        generateThumbnail(fsPath, davRelPath);
        searchIndexFile(fsPath, davRelPath);
        var st = stat(fsPath);
        return {
            status: existed ? 204 : 201,
            headers: { 'ETag': generateETag(st) },
            txt: ''
        };
    }

    return {
        status: 202,
        headers: { 'X-Bytes-Received': String(tmpSt ? tmpSt.size : 0) },
        txt: ''
    };
}

function handleDELETE(req, davRelPath, fsPath) {
    var lst = lstat(fsPath);
    if (!lst) return { status: 404, txt: 'Not Found' };

    var davHref = DAV_PREFIX + davRelPath;
    if (!checkLockForWrite(davHref, req.headers)) {
        return { status: 423, txt: 'Locked' };
    }

    // Track size for demo quota before deletion
    var deleteSize = 0;
    if (DEMO_MODE) {
        if (lst.isDirectory) deleteSize = demoGetDirSize(fsPath);
        else if (!lst.isSymbolicLink) deleteSize = lst.size || 0;
    }

    try {
        if (lst.isSymbolicLink) {
            // Delete the symlink itself, NOT the target
            rmFile(fsPath);
            deleteFileMeta(davRelPath);
            removeDeadProps(davRelPath);
            searchDeleteFile(davRelPath);
        } else if (lst.isDirectory) {
            // Clean up metadata before deleting (needs to read directory)
            deleteFileMetaRecursive(davRelPath, fsPath);
            deleteThumbnailsRecursive(davRelPath);
            removeDeadPropsRecursive(davRelPath);
            searchDeleteDir(davRelPath);
            rmdirRecursive(fsPath);
        } else {
            rmFile(fsPath);
            deleteFileMeta(davRelPath);
            deleteThumbnail(davRelPath);
            removeDeadProps(davRelPath);
            searchDeleteFile(davRelPath);
        }
    } catch(e) {
        return { status: 403, txt: 'Delete failed: ' + (e.message || 'Operation not permitted') };
    }
    if (DEMO_MODE && deleteSize > 0) demoSubQuota(deleteSize);
    return { status: 204, txt: '' };
}

function handleMKCOL(req, davRelPath, fsPath) {
    if (stat(fsPath)) {
        return { status: 405, txt: 'Resource already exists' };
    }
    if (req.body && req.body.length > 0) {
        return { status: 415, txt: 'Unsupported Media Type' };
    }
    var parentDir = getParentDir(fsPath);
    if (!stat(parentDir)) {
        return { status: 409, txt: 'Conflict - parent does not exist' };
    }
    try {
        mkdir(fsPath);
    } catch(e) {
        return { status: 500, txt: 'Failed to create collection: ' + e.message };
    }
    createFileMeta(davRelPath, req.davUser.username, true);
    pathIndexFile(davRelPath, true);
    return { status: 201, txt: '' };
}

function parseDestination(destHeader, reqHost) {
    if (!destHeader) return null;
    // Destination can be a full URL or an absolute path
    var match = destHeader.match(/^https?:\/\/[^\/]+(\/.*)/);
    if (match) return match[1];
    if (destHeader.charAt(0) === '/') return destHeader;
    return null;
}

function handleCOPY(req, davRelPath, fsPath) {
    var st = lstat(fsPath);
    if (!st) return { status: 404, txt: 'Not Found' };

    var destHeader = getHeader(req.headers, 'Destination');
    if (!destHeader) return { status: 400, txt: 'Missing Destination header' };

    var destFullPath = parseDestination(destHeader);
    if (!destFullPath) return { status: 400, txt: 'Invalid Destination' };

    var destDavRel = getDavRelPath(destFullPath);
    var destFsPath = buildFsPath(destDavRel);
    if (!destFsPath) return { status: 400, txt: 'Invalid Destination path' };
    if (!checkAllowedPath(destFsPath)) return { status: 403, txt: 'Forbidden' };

    // Demo mode: block copying INTO protected paths, enforce quota
    if (DEMO_MODE) {
        if (demoIsProtectedPath(destDavRel)) {
            return { status: 403, txt: 'Demo: this directory is read-only' };
        }
        if (!demoCheckQuota()) {
            return { status: 507, txt: 'Demo: storage quota exceeded' };
        }
    }

    if (req.davUser && !authorize(req.davUser, destDavRel, 'COPY')) {
        return { status: 403, txt: 'Forbidden' };
    }
    // Check file-level permissions on destination
    if (req.davUser) {
        var copyPermDenied = checkAccess(req.davUser, destDavRel, destFsPath, 'PUT');
        if (copyPermDenied) return { status: copyPermDenied.status, txt: copyPermDenied.msg };
    }

    var overwrite = (getHeader(req.headers, 'Overwrite') || 'T').toUpperCase() !== 'F';
    var destSt = stat(destFsPath) || lstat(destFsPath);
    var destExists = !!destSt;

    if (destExists && !overwrite) {
        return { status: 412, txt: 'Precondition Failed' };
    }
    if (destExists) {
        if (destSt.isDirectory && !destSt.isSymbolicLink) rmdirRecursive(destFsPath);
        else rmFile(destFsPath);
    }

    var destParent = getParentDir(destFsPath);
    if (!stat(destParent)) return { status: 409, txt: 'Conflict' };

    var copyOwner = req.davUser ? req.davUser.username : 'unknown';
    if (st.isSymbolicLink) {
        // Copy as symlink: resolve target, recompute relative path from destination
        var resolvedTarget;
        try { resolvedTarget = realPath(fsPath); } catch(e) { resolvedTarget = null; }
        if (!resolvedTarget) {
            return { status: 502, txt: 'Symlink target not found (broken link)' };
        }
        var relTarget = computeRelativePath(getParentDir(destFsPath), resolvedTarget);
        symlink({src: relTarget, target: destFsPath});
        copyFileMeta(davRelPath, destDavRel, copyOwner);
    } else if (st.isDirectory) {
        var depth = getHeader(req.headers, 'Depth') || 'infinity';
        if (depth.toLowerCase() === 'infinity') {
            copyRecursive(fsPath, destFsPath);
            copyDeadPropsRecursive(davRelPath, destDavRel);
            copyFileMetaRecursive(davRelPath, destDavRel, destFsPath, copyOwner);
            copyThumbnailsRecursive(davRelPath, destDavRel);
        } else {
            try { mkdir(destFsPath); } catch(e) {}
            copyFileMeta(davRelPath, destDavRel, copyOwner);
        }
    } else {
        copyFile(fsPath, destFsPath);
        copyFileMeta(davRelPath, destDavRel, copyOwner);
        copyThumbnail(davRelPath, destDavRel);
    }
    // Copy dead properties for the resource itself
    var srcProps = loadDeadProps(davRelPath);
    if (Object.keys(srcProps).length > 0) {
        saveDeadProps(destDavRel, srcProps);
    }

    // Update search index for copy
    if (st.isDirectory) {
        // Re-scan the destination directory to index copied files
        searchScanDir(destFsPath, destDavRel);
    } else {
        searchIndexFile(destFsPath, destDavRel);
    }

    return { status: destExists ? 204 : 201, txt: '' };
}

function handleMOVE(req, davRelPath, fsPath) {
    var st = lstat(fsPath);
    if (!st) return { status: 404, txt: 'Not Found' };

    var davHref = DAV_PREFIX + davRelPath;
    if (!checkLockForWrite(davHref, req.headers)) {
        return { status: 423, txt: 'Locked' };
    }

    var destHeader = getHeader(req.headers, 'Destination');
    if (!destHeader) return { status: 400, txt: 'Missing Destination header' };

    var destFullPath = parseDestination(destHeader);
    if (!destFullPath) return { status: 400, txt: 'Invalid Destination' };

    var destDavRel = getDavRelPath(destFullPath);
    var destFsPath = buildFsPath(destDavRel);
    if (!destFsPath) return { status: 400, txt: 'Invalid Destination path' };
    if (!checkAllowedPath(destFsPath)) return { status: 403, txt: 'Forbidden' };

    if (req.davUser && !authorize(req.davUser, destDavRel, 'MOVE')) {
        return { status: 403, txt: 'Forbidden' };
    }
    // Check file-level permissions on destination
    if (req.davUser) {
        var movePermDenied = checkAccess(req.davUser, destDavRel, destFsPath, 'PUT');
        if (movePermDenied) return { status: movePermDenied.status, txt: movePermDenied.msg };
    }

    var overwrite = (getHeader(req.headers, 'Overwrite') || 'T').toUpperCase() !== 'F';
    var destSt = stat(destFsPath) || lstat(destFsPath);
    var destExists = !!destSt;

    if (destExists && !overwrite) {
        return { status: 412, txt: 'Precondition Failed' };
    }
    try {
        if (destExists) {
            if (destSt.isDirectory && !destSt.isSymbolicLink) rmdirRecursive(destFsPath);
            else rmFile(destFsPath);
        }
    } catch(e) {
        return { status: 403, txt: 'Cannot replace destination: ' + (e.message || 'Operation not permitted') };
    }

    var destParent = getParentDir(destFsPath);
    if (!stat(destParent)) return { status: 409, txt: 'Conflict' };

    try {
        if (st.isSymbolicLink) {
            // Resolve absolute target before move, then recreate with corrected relative path
            var resolvedTarget;
            try { resolvedTarget = realPath(fsPath); } catch(e2) { resolvedTarget = null; }
            if (resolvedTarget) {
                var relTarget = computeRelativePath(getParentDir(destFsPath), resolvedTarget);
                symlink({src: relTarget, target: destFsPath});
            } else {
                // Dangling link — create placeholder, then delete source
                symlink({src: 'broken-link', target: destFsPath});
            }
            // Only delete source after destination symlink is created
            rmFile(fsPath);
            moveDeadProps(davRelPath, destDavRel);
            moveFileMeta(davRelPath, destDavRel);
            searchMovePath(davRelPath, destDavRel);
        } else {
            rename(fsPath, destFsPath);
            moveDeadProps(davRelPath, destDavRel);
            if (st.isDirectory) {
                moveFileMetaRecursive(davRelPath, destDavRel, destFsPath);
                moveThumbnailsRecursive(davRelPath, destDavRel);
                searchMoveDir(davRelPath, destDavRel);
            } else {
                moveFileMeta(davRelPath, destDavRel);
                moveThumbnail(davRelPath, destDavRel);
                searchMovePath(davRelPath, destDavRel);
            }
        }
    } catch(e) {
        return { status: 403, txt: 'Move failed: ' + (e.message || 'Operation not permitted') };
    }

    return { status: destExists ? 204 : 201, txt: '' };
}

function handlePROPFIND(req, davRelPath, fsPath) {
    var st = stat(fsPath);
    if (!st) {
        // Dangling symlink: stat fails but lstat works
        var lst = lstat(fsPath);
        if (lst && lst.isSymbolicLink) {
            st = lst;
            st.isDirectory = false;
            st.size = 0;
        }
    }
    if (!st) return { status: 404, txt: 'Not Found' };

    var depth = getHeader(req.headers, 'Depth');
    if (depth === undefined || depth === null) depth = 'infinity';
    depth = String(depth).toLowerCase();

    var bodyStr = '';
    if (req.body && req.body.length > 0) {
        bodyStr = bufferToString(req.body);
    }
    var propReq = parsePropfindBody(bodyStr);

    var responses = [];
    var href = DAV_PREFIX + davRelPath;

    // Add response for the resource itself
    responses.push(buildResourceResponse(href, fsPath, st, propReq));

    // If collection and depth > 0, add children
    if (st.isDirectory && depth !== '0') {
        var entries = readdirAll(fsPath);

        // Filter root listing: users see their own home + non-user directories
        if (davRelPath === '/' && req.davUser) {
            entries = entries.filter(function(name) {
                if (req.davUser.admin) return true;
                if (name === req.davUser.username) return true;
                // Non-user directories (shared, shared2, etc.) are visible to all
                if (!db.get(userDbi, name)) return true;
                return false;
            });
        }

        for (var i = 0; i < entries.length; i++) {
            var childName = entries[i];
            var childFsPath = fsPath + '/' + childName;
            // Ensure fsPath doesn't have trailing slash duplication
            if (fsPath.charAt(fsPath.length - 1) === '/') {
                childFsPath = fsPath + childName;
            }
            var childDavRel = davRelPath;
            if (childDavRel.charAt(childDavRel.length - 1) !== '/') {
                childDavRel += '/';
            }
            childDavRel += encodeURIComponent(childName);

            if (!checkAllowedPath(childFsPath)) continue;
            var childStat = stat(childFsPath);
            if (!childStat) {
                var childLstat = lstat(childFsPath);
                if (childLstat && childLstat.isSymbolicLink) {
                    childStat = childLstat;
                    childStat.isDirectory = false;
                    childStat.size = 0;
                }
            }
            if (childStat) {
                var childHref = DAV_PREFIX + childDavRel;
                if (childStat.isDirectory) childHref += '/';
                responses.push(buildResourceResponse(childHref, childFsPath, childStat, propReq));

                // Depth infinity: recurse into subdirectories
                if (depth === 'infinity' && childStat.isDirectory) {
                    addChildResponses(childDavRel, childFsPath, propReq, responses);
                }
            }
        }
    }

    var xml = xmlMultiStatus(responses);
    return {
        status: 207,
        headers: { 'Content-Type': 'application/xml; charset=utf-8' },
        txt: xml
    };
}

function addChildResponses(davRelPath, fsPath, propReq, responses) {
    var entries = readdirAll(fsPath);
    for (var i = 0; i < entries.length; i++) {
        var childName = entries[i];
        var childFsPath = fsPath + '/' + childName;
        var childDavRel = davRelPath;
        if (childDavRel.charAt(childDavRel.length - 1) !== '/') {
            childDavRel += '/';
        }
        childDavRel += encodeURIComponent(childName);

        if (!checkAllowedPath(childFsPath)) continue;
        var childStat = stat(childFsPath);
        if (!childStat) {
            var childLstat = lstat(childFsPath);
            if (childLstat && childLstat.isSymbolicLink) {
                childStat = childLstat;
                childStat.isDirectory = false;
                childStat.size = 0;
            }
        }
        if (childStat) {
            var childHref = DAV_PREFIX + childDavRel;
            if (childStat.isDirectory) childHref += '/';
            responses.push(buildResourceResponse(childHref, childFsPath, childStat, propReq));
            if (childStat.isDirectory) {
                addChildResponses(childDavRel, childFsPath, propReq, responses);
            }
        }
    }
}

function handlePROPPATCH(req, davRelPath, fsPath) {
    var st = stat(fsPath);
    if (!st) return { status: 404, txt: 'Not Found' };

    var davHref = DAV_PREFIX + davRelPath;
    if (!checkLockForWrite(davHref, req.headers)) {
        return { status: 423, txt: 'Locked' };
    }

    var bodyStr = '';
    if (req.body && req.body.length > 0) {
        bodyStr = bufferToString(req.body);
    }
    var ops = parseProppatchBody(bodyStr);

    var dead = loadDeadProps(davRelPath);
    var resultProps = '';

    for (var i = 0; i < ops.set.length; i++) {
        dead[ops.set[i].name] = ops.set[i].value;
        resultProps += '<' + ops.set[i].name + '/>\n';
    }
    for (var j = 0; j < ops.remove.length; j++) {
        delete dead[ops.remove[j].name];
        resultProps += '<' + ops.remove[j].name + '/>\n';
    }

    saveDeadProps(davRelPath, dead);

    var xml = xmlMultiStatus([
        xmlResponse(davHref, [
            xmlPropstat(resultProps, '200 OK')
        ])
    ]);
    return {
        status: 207,
        headers: { 'Content-Type': 'application/xml; charset=utf-8' },
        txt: xml
    };
}

function handleLOCK(req, davRelPath, fsPath) {
    var davHref = DAV_PREFIX + davRelPath;
    var st = stat(fsPath);

    var depth = getHeader(req.headers, 'Depth') || '0';
    var timeout = 3600; // default 1 hour
    var timeoutHeader = getHeader(req.headers, 'Timeout');
    if (timeoutHeader) {
        var match = timeoutHeader.match(/Second-(\d+)/);
        if (match) timeout = parseInt(match[1]);
    }

    // Check if this is a lock refresh (no body, If header with token)
    if ((!req.body || req.body.length === 0) && getHeader(req.headers, 'If')) {
        var tokens = parseIfHeader(getHeader(req.headers, 'If'));
        if (tokens.length > 0) {
            var refreshed = refreshLock(davHref, tokens[0], timeout);
            if (refreshed) {
                return {
                    status: 200,
                    headers: {
                        'Content-Type': 'application/xml; charset=utf-8',
                        'Lock-Token': '<' + refreshed.token + '>'
                    },
                    txt: buildLockResponseXml(refreshed)
                };
            }
            return { status: 412, txt: 'Precondition Failed' };
        }
    }

    var bodyStr = req.body ? bufferToString(req.body) : '';
    var lockReq = parseLockBody(bodyStr);

    // If resource does not exist, create a lock-null resource
    if (!st) {
        var parentDir = getParentDir(fsPath);
        if (!stat(parentDir)) return { status: 409, txt: 'Conflict' };
        // Create empty file
        var fp = fopen(fsPath, 'w+');
        fclose(fp);
    }

    var lockInfo = acquireLock(davHref, lockReq.scope, lockReq.type,
        lockReq.owner, depth, timeout);

    if (!lockInfo) {
        return { status: 423, txt: 'Locked' };
    }

    return {
        status: st ? 200 : 201,
        headers: {
            'Content-Type': 'application/xml; charset=utf-8',
            'Lock-Token': '<' + lockInfo.token + '>'
        },
        txt: buildLockResponseXml(lockInfo)
    };
}

function handleUNLOCK(req, davRelPath, fsPath) {
    var davHref = DAV_PREFIX + davRelPath;
    var tokenHeader = getHeader(req.headers, 'Lock-Token');
    if (!tokenHeader) return { status: 400, txt: 'Missing Lock-Token header' };

    // Strip angle brackets: <urn:uuid:xxx> -> urn:uuid:xxx
    var token = tokenHeader.replace(/^</, '').replace(/>$/, '');

    var released = releaseLock(davHref, token);
    if (!released) return { status: 409, txt: 'Lock token does not match' };

    return { status: 204, txt: '' };
}

// Attach session cookie to a response if needed
function attachSessionCookie(resp, davUser, req) {
    if (!davUser) return resp;
    if (davUser._basicAuth || davUser._needsRefresh) {
        var cookie = createSessionCookie(davUser.username, davUser._record || db.get(userDbi, davUser.username), req);
        if (!resp.headers) resp.headers = {};
        resp.headers['Set-Cookie'] = cookie;
    }
    return resp;
}

/* ============================================================
 * Section 8b: Plugins
 * ============================================================ */

var PLUGINS = {};
var PLUGIN_EXT_MAP = {};

// Background job tracking for plugins (drop downloads etc.)
// Jobs are stored in the thread clipboard as JSON under
// 'plugin_job/<jobId>'.
function pluginJobSet(jobId, data) {
    rampart.thread.put('plugin_job/' + jobId,
        JSON.stringify(data));
}
function pluginJobGet(jobId) {
    var raw = rampart.thread.get('plugin_job/' + jobId);
    if (!raw) return null;
    try { return JSON.parse(raw); } catch(e) { return null; }
}
function pluginJobDel(jobId) {
    rampart.thread.del('plugin_job/' + jobId);
}
var PLUGIN_MIME_MAP = {};
var PLUGIN_DROP_LIST = [];  // plugins with drop handlers, checked in order
(function loadPlugins() {
    var pluginDir = process.scriptPath + '/apps/webdav/plugins';
    if (!stat(pluginDir)) return;
    var files;
    try { files = readdir(pluginDir).sort(); } catch(e) { return; }
    if (!files) return;
    for (var i = 0; i < files.length; i++) {
        if (!/\.js$/.test(files[i])) continue;
        try {
            var plugin = require(pluginDir + '/' + files[i]);
            if (!plugin.name) continue;
            // Needs at least a render or drop function
            if (!plugin.render && !plugin.drop) continue;
            PLUGINS[plugin.name] = plugin;
            if (plugin.extensions) {
                for (var j = 0; j < plugin.extensions.length; j++) {
                    PLUGIN_EXT_MAP[plugin.extensions[j].toLowerCase()] = plugin.name;
                }
            }
            if (plugin.mimeTypes) {
                for (var k = 0; k < plugin.mimeTypes.length; k++) {
                    PLUGIN_MIME_MAP[plugin.mimeTypes[k].toLowerCase()] = plugin.name;
                }
            }
            if (plugin.drop && plugin.dropPattern) {
                // Normalize dropPattern to always be an array of RegExp
                if (!Array.isArray(plugin.dropPattern)) {
                    plugin.dropPattern = [plugin.dropPattern];
                }
                plugin.dropPattern = plugin.dropPattern.filter(function(re) {
                    if (re instanceof RegExp) return true;
                    fprintf(stderr, "Plugin '%s': dropPattern contains non-RegExp, skipping\n", plugin.name);
                    return false;
                });
                if (plugin.dropPattern.length > 0) {
                    PLUGIN_DROP_LIST.push(plugin.name);
                }
            }
        } catch(e) {
            fprintf(stderr, "Plugin load error (%s): %s\n", files[i], e.message || e);
        }
    }
})();

/* ============================================================
 * Section 8c: ONLYOFFICE JWT Helpers
 * ============================================================ */

var OO_JWT_SECRET = global.OO_JWT_SECRET || '';

// Base64url encode: %-0B gives url-safe base64 without padding
function _ooB64url(bufOrStr) {
    return sprintf("%-0B", bufOrStr);
}

// Base64url decode to string (for JWT payload JSON)
function _ooB64urlDecode(str) {
    var b64 = str.replace(/-/g, '+').replace(/_/g, '/');
    var pad = (4 - b64.length % 4) % 4;
    for (var i = 0; i < pad; i++) b64 += '=';
    return sprintf("%!B", b64);
}

function _ooJwtSign(payload) {
    var header = {alg: 'HS256', typ: 'JWT'};
    var hb64 = _ooB64url(stringToBuffer(JSON.stringify(header)));
    var pb64 = _ooB64url(stringToBuffer(JSON.stringify(payload)));
    var sigInput = hb64 + '.' + pb64;
    var sig = crypto.hmac(OO_JWT_SECRET, sigInput, 'sha256', true);
    return sigInput + '.' + _ooB64url(sig);
}

function _ooJwtVerify(token) {
    var parts = token.split('.');
    if (parts.length !== 3) return null;
    var sigInput = parts[0] + '.' + parts[1];
    // Compare base64url-encoded signatures (avoids binary string issues)
    var expectedSig = _ooB64url(crypto.hmac(OO_JWT_SECRET, sigInput, 'sha256', true));
    if (expectedSig !== parts[2]) return null;
    try {
        return JSON.parse(_ooB64urlDecode(parts[1]));
    } catch(e) {
        return null;
    }
}

// Token for fetch endpoint — allows ONLYOFFICE to download the document
// Valid for 24 hours (document editing sessions can be long)
function _ooSignFetchToken(davRelPath, nowSec) {
    return _ooJwtSign({
        purpose: 'oo-fetch',
        path: davRelPath,
        exp: nowSec + 86400
    });
}

function _ooVerifyFetchToken(token, davRelPath) {
    var claims = _ooJwtVerify(token);
    if (!claims) return null;
    if (claims.purpose !== 'oo-fetch') return null;
    if (claims.path !== davRelPath) return null;
    if (claims.exp && claims.exp < Math.floor(Date.now() / 1000)) return null;
    return claims;
}

// Token for callback endpoint — allows ONLYOFFICE to push saves
// Valid for 24 hours
function _ooSignCallbackToken(davRelPath, username, nowSec) {
    return _ooJwtSign({
        purpose: 'oo-callback',
        path: davRelPath,
        user: username,
        exp: nowSec + 86400
    });
}

function _ooVerifyCallbackToken(token, davRelPath) {
    var claims = _ooJwtVerify(token);
    if (!claims) return null;
    if (claims.purpose !== 'oo-callback') return null;
    if (claims.path !== davRelPath) return null;
    if (claims.exp && claims.exp < Math.floor(Date.now() / 1000)) return null;
    return claims;
}

function _htmlEsc(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/* ============================================================
 * Section 9: Main Dispatch
 * ============================================================ */

function main_dispatch(req) {
    var method = (req.methodRaw || req.method || 'GET').toUpperCase();
    // Bind req to cookie helpers so all session cookies match the request scheme
    var _attachCookie = function(resp, davUser) { return attachSessionCookie(resp, davUser, req); };
    var _createCookie = function(username, record) { return createSessionCookie(username, record, req); };
    var _clearCookie = function() { return clearSessionCookie(req); };

    // Extract DAV-relative path from the full request path
    var fullPath = req.path.path || '/';

    // Status endpoint: GET /dav/_status — check if any users/admins exist
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_status') {
        var allUsers = db.get(userDbi, "", 10000);
        var hasUsers = allUsers && typeof allUsers === 'object' && Object.keys(allUsers).length > 0;
        var hasAdmin = false;
        if (hasUsers) {
            var ukeys = Object.keys(allUsers);
            for (var ui = 0; ui < ukeys.length; ui++) {
                if (allUsers[ukeys[ui]].admin) { hasAdmin = true; break; }
            }
        }
        return { status: 200, json: {ok: true, hasUsers: hasUsers, hasAdmin: hasAdmin} };
    }

    // Search endpoint: POST /dav/_search with JSON {query, maxRows, skipRows}
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_search') {
        var srchUser = authenticate(req);
        if (!srchUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var srchBody;
        try { srchBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        if (!srchBody.query) return { status: 400, json: {ok: false, error: 'Missing query'} };
        var srchSubPath = srchBody.subPath || null;
        var srchResult = searchQuery(srchBody.query, srchUser.username, srchUser.admin, srchBody.maxRows, srchBody.skipRows, srchSubPath);
        return _attachCookie({ status: 200, json: {ok: true, query: srchBody.query, results: srchResult.results, total: srchResult.total} }, srchUser);
    }

    // Autocomplete suggestions: GET /dav/_search/suggest?q=term&mode=path|word&max=10
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_search/suggest') {
        var sugUser = authenticate(req);
        if (!sugUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var sugQuery = req.query || req.path.search || {};
        var sugQ = sugQuery.q || '';
        var sugMode = sugQuery.mode || 'path';
        var sugMax = parseInt(sugQuery.max) || 10;
        var sugSubPath = sugQuery.subPath ? decodeURIComponent(sugQuery.subPath).replace(/\/?$/, '/') : null;
        if (sugQ.length < 2) return _attachCookie({ status: 200, json: {ok: true, suggestions: []} }, sugUser);

        if (!searchSql && stat(SEARCH_DB_PATH)) {
            try { searchSql = new Sql.connection(SEARCH_DB_PATH); } catch(e) {}
        }
        if (!searchSql) return _attachCookie({ status: 200, json: {ok: true, suggestions: []} }, sugUser);

        var sugResults = [];

        if (sugMode === 'word') {
            // Word suggestions from fulltext index
            if (searchSql.one("SELECT * FROM SYSTABLES WHERE NAME = ?", ['docs'])) {
                try {
                    searchSql.set({indexaccess: true});
                    // Get the last word being typed for autocomplete
                    var sugWords = sugQ.trim().split(/\s+/);
                    var sugLastWord = sugWords[sugWords.length - 1];
                    if (sugLastWord.length >= 2) {
                        var sugRes = searchSql.exec(
                            "SELECT Word FROM docs_content_ftx WHERE Word MATCHES ?w ORDER BY Count DESC",
                            {maxRows: sugMax},
                            {w: sugLastWord + '%'}
                        );
                        if (sugRes.rows) {
                            for (var si = 0; si < sugRes.rows.length; si++) {
                                // Build the full suggestion: prefix words + completed word
                                var prefix = sugWords.length > 1 ? sugWords.slice(0, -1).join(' ') + ' ' : '';
                                var fullValue = prefix + sugRes.rows[si].Word;
                                sugResults.push({
                                    label: fullValue,
                                    value: fullValue
                                });
                            }
                        }
                    }
                    searchSql.set({indexaccess: false});
                } catch(e) {
                    try { searchSql.set({indexaccess: false}); } catch(e2) {}
                }
            }
        } else if (sugMode === 'pathword') {
            // Fulltext path suggestions — for filename search without leading /
            if (searchSql.one("SELECT * FROM SYSTABLES WHERE NAME = ?", ['paths'])) {
                try {
                    searchSql.set({suffixproc: false, likepRows: 100});
                    var sugSql, sugParams;
                    if (sugSubPath) {
                        sugSql = "SELECT path, isdir FROM paths WHERE path LIKEP ?q AND path MATCHES ?dir";
                        sugParams = {q: sugQ, dir: sugSubPath + '%'};
                    } else {
                        sugSql = "SELECT path, isdir FROM paths WHERE path LIKEP ?q";
                        sugParams = {q: sugQ};
                    }
                    var sugRes = searchSql.exec(sugSql, {maxRows: sugMax * 3}, sugParams);
                    if (sugRes.rows) {
                        for (var si = 0; si < sugRes.rows.length; si++) {
                            if (!sugSubPath && !userCanSeePath(sugUser.username, sugUser.admin, sugRes.rows[si].path)) continue;
                            sugResults.push({path: sugRes.rows[si].path, isDir: !!sugRes.rows[si].isdir});
                            if (sugResults.length >= sugMax) break;
                        }
                    }
                } catch(e) {}
            }
        } else {
            // Path prefix suggestions (starts with /)
            if (searchSql.one("SELECT * FROM SYSTABLES WHERE NAME = ?", ['paths'])) {
                try {
                    var sugPrefix = sugQ;
                    // If subPath filter and query doesn't already include it, prepend it
                    if (sugSubPath && sugQ.indexOf(sugSubPath) !== 0) {
                        sugPrefix = sugSubPath + sugQ.substring(1); // replace leading / with subPath
                    }
                    var sugRes = searchSql.exec(
                        "SELECT path, isdir FROM paths WHERE path MATCHES ?p ORDER BY length(path)",
                        {maxRows: sugMax * 3},
                        {p: sugPrefix + '%'}
                    );
                    if (sugRes.rows) {
                        for (var si = 0; si < sugRes.rows.length; si++) {
                            if (!userCanSeePath(sugUser.username, sugUser.admin, sugRes.rows[si].path)) continue;
                            sugResults.push({path: sugRes.rows[si].path, isDir: !!sugRes.rows[si].isdir});
                            if (sugResults.length >= sugMax) break;
                        }
                    }
                } catch(e) {}
            }
        }

        return _attachCookie({ status: 200, json: {ok: true, suggestions: sugResults} }, sugUser);
    }

    // Filename search endpoint: POST /dav/_search/files with JSON {query, subPath, maxRows, skipRows}
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_search/files') {
        var fnUser = authenticate(req);
        if (!fnUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var fnBody;
        try { fnBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        if (!fnBody.query) return { status: 400, json: {ok: false, error: 'Missing query'} };
        var fnSubPath = fnBody.subPath || null;
        var fnResult = filenameSearch(fnBody.query, fnUser.username, fnUser.admin, fnBody.maxRows, fnBody.skipRows, fnSubPath);
        return _attachCookie({ status: 200, json: {ok: true, query: fnBody.query, results: fnResult.results, total: fnResult.total} }, fnUser);
    }

    // Reindex endpoint: POST /dav/_search/reindex with JSON {path} (admin only)
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_search/reindex') {
        var riUser = authenticate(req);
        if (!riUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!riUser.admin) return { status: 403, json: {ok: false, error: 'Admin only'} };
        var riBody;
        try { riBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var riDavPath = riBody.path || ('/' + riUser.username + '/' + SEARCH_DEFAULT_DIR);
        var riFsPath = DAV_ROOT + riDavPath;
        if (!stat(riFsPath)) return { status: 404, json: {ok: false, error: 'Directory not found'} };
        var riTable = searchTableName(riDavPath);
        searchScanDir(riFsPath, riDavPath, riTable);
        return _attachCookie({ status: 200, json: {ok: true, message: 'Reindex started for ' + riDavPath} }, riUser);
    }

    // Search index status: GET /dav/_search/status?path=/aaron/somedir
    // Returns whether this dir is indexed, has an indexed parent, or indexed children
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_search/status') {
        var siUser = authenticate(req);
        if (!siUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var siQuery = req.query || req.path.search || {};
        var siPath = siQuery.path;
        if (!siPath) return { status: 400, json: {ok: false, error: 'Missing path'} };
        siPath = decodeURIComponent(siPath);
        var siFsPath = DAV_ROOT + siPath;
        var siIsMounted = _isMounted(siFsPath);
        if (siIsMounted && !allowMountedSearch) {
            return _attachCookie({ status: 200, json: {ok: true, indexed: false, parentIndexed: false, mountBlocked: true} }, siUser);
        }
        var siIndexed = !!db.get(searchDbi, siPath);
        var siParent = searchGetIndexedParent(siPath);
        return _attachCookie({ status: 200, json: {
            ok: true,
            indexed: siIndexed,
            parentIndexed: siParent || false
        }}, siUser);
    }

    // Toggle search indexing: POST /dav/_search/toggle with JSON {path, enable}
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_search/toggle') {
        var stUser = authenticate(req);
        if (!stUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var stBody;
        try { stBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var stPath = stBody.path;
        if (!stPath) return { status: 400, json: {ok: false, error: 'Missing path'} };
        // Verify user owns this directory or is admin
        if (!stUser.admin && stPath.indexOf('/' + stUser.username + '/') !== 0) {
            return { status: 403, json: {ok: false, error: 'Permission denied'} };
        }
        var stFsPath = DAV_ROOT + stPath;
        if (!stat(stFsPath) || !stat(stFsPath).isDirectory) {
            return { status: 404, json: {ok: false, error: 'Directory not found'} };
        }
        // Block mounted directories unless allowed
        if (_isMounted(stFsPath) && !allowMountedSearch) {
            return { status: 403, json: {ok: false, error: 'Search indexing of mounted directories is disabled'} };
        }
        // Can't toggle if a parent is indexed
        var stParent = searchGetIndexedParent(stPath);
        if (stParent) {
            return { status: 400, json: {ok: false, error: 'Parent directory "' + stParent + '" is already indexed'} };
        }
        if (stBody.enable) {
            searchEnableDir(stPath);
            return _attachCookie({ status: 200, json: {ok: true, message: 'Indexing enabled for ' + stPath} }, stUser);
        } else {
            searchDisableDir(stPath);
            return _attachCookie({ status: 200, json: {ok: true, message: 'Indexing disabled for ' + stPath} }, stUser);
        }
    }

    // Login endpoint: POST /dav/_login with JSON {username, password}
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_login') {
        var body;
        try {
            body = JSON.parse(bufferToString(req.body));
        } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var username = body.username;
        var password = body.password;
        if (!username || !password) {
            return { status: 400, json: {ok: false, error: 'Missing username or password'} };
        }
        var userRecord = db.get(userDbi, username);
        if (!userRecord || !crypto.passwdCheck(userRecord.hash_line, password)) {
            return { status: 401, json: {ok: false, error: 'Invalid username or password'} };
        }
        ensureUserSessionKey(userRecord, username);
        var cookie = _createCookie(username, userRecord);

        // Auto-remount any unmounted SFTP mounts for this user
        var loginRemounted = [];
        if (HAS_RCLONE) {
            var loginMounts = db.get(rcloneDbi, "", 10000);
            if (loginMounts && typeof loginMounts === 'object') {
                var lmKeys = Object.keys(loginMounts);
                for (var lmi = 0; lmi < lmKeys.length; lmi++) {
                    if (lmKeys[lmi].indexOf(username + '/') !== 0) continue;
                    var lmCfg = loginMounts[lmKeys[lmi]];
                    if (!lmCfg || lmCfg.provider !== 'sftp' || !lmCfg.encryptedCreds) continue;
                    var lmName = lmKeys[lmi].split('/')[1];
                    // Skip if actually mounted (don't trust active flag)
                    if (_isMounted(getUserMountDir(username, lmName, lmCfg.rootMount))) continue;
                    try {
                        var lmEncBuf = sprintf("%!B", lmCfg.encryptedCreds);
                        var lmDecBuf = crypto.decrypt({pass: password, data: lmEncBuf, cipher: 'aes-256-cbc'});
                        var lmCreds = JSON.parse(bufferToString(lmDecBuf));
                        var lmEnv = buildSftpMountEnv(lmCreds);
                        var lmRes = rcloneMountRemote(username, lmName, lmName, lmCfg.remotePath || '', '', lmEnv, lmCfg.rootMount);
                        lmCfg.active = lmRes.ok;
                        db.put(rcloneDbi, lmKeys[lmi], lmCfg);
                        if (lmRes.ok) loginRemounted.push(lmName);
                    } catch(e) {
                        // Decryption failed — password mismatch (shouldn't happen since we just verified)
                    }
                }
            }
        }

        // Demo wipe thread is started at module load (see top-level init)

        return {
            status: 200,
            headers: { 'Set-Cookie': cookie },
            json: {ok: true, username: username, admin: !!userRecord.admin, remounted: loginRemounted, requirePasswordChange: !!userRecord.requirePasswordChange}
        };
    }

    // Logout endpoint: POST /dav/_logout
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_logout') {
        return {
            status: 200,
            headers: { 'Set-Cookie': _clearCookie() },
            json: {ok: true}
        };
    }

    // File metadata: POST /dav/_filemeta — update file permissions and/or group
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_filemeta') {
        var fmUser = authenticate(req);
        if (!fmUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var fmBody;
        try { fmBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var fmPath = (fmBody.path || '').trim();
        if (!fmPath) {
            return { status: 400, json: {ok: false, error: 'path required'} };
        }
        var fmDavRel = getDavRelPath(fmPath);
        var fmFsPath = buildFsPath(fmDavRel);
        if (!fmFsPath || !stat(fmFsPath)) {
            return { status: 404, json: {ok: false, error: 'File not found'} };
        }
        // User-path files have fixed permissions — no changes allowed
        if (isUserPath(fmDavRel)) {
            return { status: 403, json: {ok: false, error: 'Permissions cannot be changed for files in your home directory'} };
        }
        var fmMeta = ensureFileMeta(fmDavRel, fmFsPath);
        if (!fmMeta) {
            return { status: 404, json: {ok: false, error: 'No metadata for file'} };
        }
        // Authorization: only owner or admin
        if (!fmUser.admin && fmMeta.owner !== fmUser.username) {
            return { status: 403, json: {ok: false, error: 'Only the file owner or an admin can change metadata'} };
        }
        if (fmBody.permissions !== undefined) {
            var fmPerms = parseInt(fmBody.permissions);
            if (isNaN(fmPerms) || fmPerms < 0 || fmPerms > 777) {
                return { status: 400, json: {ok: false, error: 'Permissions must be 0-777'} };
            }
            // Validate each octal digit is 0-7
            var p1 = Math.floor(fmPerms / 100), p2 = Math.floor(fmPerms / 10) % 10, p3 = fmPerms % 10;
            if (p1 > 7 || p2 > 7 || p3 > 7) {
                return { status: 400, json: {ok: false, error: 'Each permission digit must be 0-7'} };
            }
            // Group must be superset of other (prevent inversion like 0604)
            if ((p2 & p3) !== p3) {
                return { status: 400, json: {ok: false, error: 'Group permissions must include all other permissions'} };
            }
            fmMeta.permissions = fmPerms;
        }
        if (fmBody.group !== undefined) {
            var fmGroup = (fmBody.group || '').trim();
            if (fmGroup !== 'nogroup' && fmGroup !== 'everyone') {
                var fmGrp = db.get(groupDbi, fmGroup);
                if (!fmGrp) {
                    return { status: 400, json: {ok: false, error: 'Group does not exist'} };
                }
            }
            fmMeta.group = fmGroup;
        }
        if (fmBody.owner !== undefined) {
            if (!fmUser.admin) {
                return { status: 403, json: {ok: false, error: 'Only administrators can change file ownership'} };
            }
            var fmNewOwner = (fmBody.owner || '').trim();
            if (!fmNewOwner || !db.get(userDbi, fmNewOwner)) {
                return { status: 400, json: {ok: false, error: 'User does not exist'} };
            }
            fmMeta.owner = fmNewOwner;
        }
        setFileMeta(fmDavRel, fmMeta);
        return { status: 200, json: {ok: true, permissions: fmMeta.permissions, group: fmMeta.group, owner: fmMeta.owner} };
    }

    // Fetch a URL and save it as a file: POST /dav/_fetchurl
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_fetchurl') {
        var fuUser = authenticate(req);
        if (!fuUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var fuBody;
        try { fuBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var fuUrl = (fuBody.url || '').trim();
        if (!fuUrl || !/^https?:\/\//i.test(fuUrl)) {
            return { status: 400, json: {ok: false, error: 'Invalid URL'} };
        }
        // Block requests to private/internal IPs to prevent SSRF
        var fuHostMatch = fuUrl.match(/^https?:\/\/([^:\/\[\]]+|\[[^\]]+\])/i);
        if (fuHostMatch) {
            var fuHost = fuHostMatch[1].replace(/^\[|\]$/g, '');
            var fuResolved;
            try { fuResolved = net.resolve(fuHost); } catch(e) {
                return { status: 400, json: {ok: false, error: 'Could not resolve hostname'} };
            }
            var fuAddrs = (fuResolved && fuResolved.ipaddrs) || [];
            for (var fai = 0; fai < fuAddrs.length; fai++) {
                var addr = fuAddrs[fai];
                if (addr === '127.0.0.1' || addr === '::1' ||
                    /^10\./.test(addr) ||
                    /^172\.(1[6-9]|2[0-9]|3[01])\./.test(addr) ||
                    /^192\.168\./.test(addr) ||
                    /^169\.254\./.test(addr) ||
                    /^0\./.test(addr) ||
                    /^fe80:/i.test(addr) ||
                    /^fc00:/i.test(addr) ||
                    /^fd/i.test(addr)) {
                    return { status: 403, json: {ok: false, error: 'Requests to private/internal addresses are not allowed'} };
                }
            }
        }
        var fuDir = (fuBody.dir || '').trim();
        if (!fuDir) return { status: 400, json: {ok: false, error: 'Missing target directory'} };

        // Resolve target directory to filesystem path
        var fuDavRel = getDavRelPath(fuDir);
        var fuFsDir = buildFsPath(fuDavRel);
        if (!fuFsDir || !stat(fuFsDir)) {
            return { status: 404, json: {ok: false, error: 'Target directory not found'} };
        }

        // Determine filename from URL or Content-Disposition
        var fuFilename = (fuBody.filename || '').trim();
        if (!fuFilename) {
            // Extract from URL path
            try {
                var fuPathMatch = fuUrl.match(/^https?:\/\/[^\/]+(\/[^?#]*)/i);
                var fuPath = fuPathMatch ? fuPathMatch[1].replace(/\/+$/, '') : '';
                fuFilename = fuPath ? decodeURIComponent(fuPath.split('/').pop()) : '';
            } catch(e) {}
            if (!fuFilename || fuFilename === '' || fuFilename.indexOf('.') === -1) {
                fuFilename = 'index.html';
            }
        }

        // Sanitize filename
        fuFilename = fuFilename.replace(/[\/\\:*?"<>|]/g, '_');
        if (!fuFilename) fuFilename = 'download';

        try {
            var fuResp = curl.fetch(fuUrl, {
                location: true,
                "max-time": 120,
                "max-redirs": 10,
                returnText: false
            });

            if (fuResp.status < 200 || fuResp.status >= 400) {
                return { status: 502, json: {ok: false, error: 'Remote server returned ' + fuResp.status} };
            }

            // Check Content-Disposition for filename if we used the URL-derived one
            if (!fuBody.filename && fuResp.headers) {
                var fuCD = fuResp.headers['content-disposition'] || '';
                var fuCDMatch = fuCD.match(/filename[*]?=["']?(?:UTF-8'')?([^"';\n]+)/i);
                if (fuCDMatch) {
                    var fuCDName = decodeURIComponent(fuCDMatch[1].trim());
                    if (fuCDName) fuFilename = fuCDName.replace(/[\/\\:*?"<>|]/g, '_');
                }
            }

            var fuFsPath = fuFsDir + '/' + fuFilename;
            // Avoid overwriting
            if (stat(fuFsPath)) {
                var fuBase = fuFilename.replace(/\.[^.]+$/, '');
                var fuExt = fuFilename.indexOf('.') > 0 ? fuFilename.substring(fuFilename.lastIndexOf('.')) : '';
                var fuN = 1;
                while (stat(fuFsDir + '/' + fuBase + ' (' + fuN + ')' + fuExt)) fuN++;
                fuFilename = fuBase + ' (' + fuN + ')' + fuExt;
                fuFsPath = fuFsDir + '/' + fuFilename;
            }

            try {
                var fuFp = fopen(fuFsPath, 'w+');
                if (fuResp.body && fuResp.body.length > 0) fwrite(fuFp, fuResp.body);
                fclose(fuFp);
            } catch(e) {
                return { status: 403, json: {ok: false, error: 'Write failed: ' + (e.message || 'permission denied')} };
            }

            var fuDavPath = fuDavRel.replace(/\/?$/, '/') + fuFilename;
            createFileMeta(fuDavPath, fuUser.username, false);
            generateThumbnail(fuFsPath, fuDavPath);

            return _attachCookie({
                status: 200,
                json: {ok: true, filename: fuFilename, size: fuResp.body ? fuResp.body.length : 0}
            }, fuUser);
        } catch(e) {
            return { status: 502, json: {ok: false, error: 'Fetch failed: ' + (e.message || 'unknown error')} };
        }
    }

    // Share link management: POST /dav/_share
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_share') {
        var shUser = authenticate(req);
        if (!shUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var shBody;
        try { shBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var shAction = shBody.action;

        // Create a share link
        if (shAction === 'create') {
            var shPath = (shBody.path || '').trim();
            if (!shPath) return { status: 400, json: {ok: false, error: 'path required'} };
            var shDavRel = getDavRelPath(shPath);
            var shFsPath = buildFsPath(shDavRel);
            if (!shFsPath) return { status: 400, json: {ok: false, error: 'Bad path'} };
            var shSt = stat(shFsPath);
            if (!shSt) return { status: 404, json: {ok: false, error: 'File not found'} };
            // Authorization: owner or admin
            if (!shUser.admin) {
                var shMeta = ensureFileMeta(shDavRel, shFsPath);
                if (shMeta && shMeta.owner !== shUser.username) {
                    var shAccess = checkAccess(shUser, shDavRel, shFsPath, 'GET');
                    if (shAccess) return { status: 403, json: {ok: false, error: 'Access denied'} };
                }
            }
            var shDuration = shBody.duration; // seconds or 0 for forever
            var shExpires = null;
            if (shBody.expires) {
                // Absolute expiration date passed directly
                shExpires = shBody.expires;
            } else if (shDuration && shDuration > 0) {
                shExpires = new Date(Date.now() + shDuration * 1000).toISOString();
            }
            var shToken = generateShareToken();
            var shRecord = {
                path: shDavRel,
                owner: shUser.username,
                created: new Date().toISOString(),
                expires: shExpires,
                isDir: shSt.isDirectory
            };
            db.put(sharesDbi, shToken, shRecord);
            invalidateSharedPathsCache();
            return { status: 200, json: {ok: true, token: shToken, share: shRecord} };
        }

        // List shares for a path
        if (shAction === 'list') {
            var slPath = (shBody.path || '').trim();
            if (!slPath) return { status: 400, json: {ok: false, error: 'path required'} };
            var slDavRel = getDavRelPath(slPath);
            var allShares = db.get(sharesDbi, "", 10000);
            var shares = [];
            if (allShares) {
                var shKeys = Object.keys(allShares);
                for (var si = 0; si < shKeys.length; si++) {
                    var shRec = allShares[shKeys[si]];
                    if (shRec.path === slDavRel && (shUser.admin || shRec.owner === shUser.username)) {
                        shares.push({token: shKeys[si], share: shRec});
                    }
                }
            }
            return { status: 200, json: {ok: true, shares: shares} };
        }

        // Delete a share link
        if (shAction === 'delete') {
            var delToken = (shBody.token || '').trim();
            if (!delToken) return { status: 400, json: {ok: false, error: 'token required'} };
            var delRec = db.get(sharesDbi, delToken);
            if (!delRec) return { status: 404, json: {ok: false, error: 'Share not found'} };
            if (!shUser.admin && delRec.owner !== shUser.username) {
                return { status: 403, json: {ok: false, error: 'Access denied'} };
            }
            db.del(sharesDbi, delToken);
            invalidateSharedPathsCache();
            return { status: 200, json: {ok: true} };
        }

        // Parse duration (for custom time preview)
        if (shAction === 'parse') {
            var pdInput = (shBody.text || '').trim();
            if (!pdInput) return { status: 400, json: {ok: false, error: 'text required'} };
            var pdResult = parseDuration(pdInput);
            if (pdResult.error) return { status: 200, json: {ok: false, error: pdResult.error} };
            return { status: 200, json: {ok: true, seconds: pdResult.seconds, expires: pdResult.expires} };
        }

        return { status: 400, json: {ok: false, error: 'Unknown action'} };
    }

    // Playlist: POST /dav/_playlist — list recent playlists or append tracks
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_playlist') {
        var plUser = authenticate(req);
        if (!plUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var plBody;
        try { plBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var plAction = (plBody.action || '').trim();

        if (plAction === 'list') {
            // List .m3u files in user's Music directory, newest 5
            var plMusicFs = DAV_ROOT + '/' + plUser.username + '/Music';
            var plFiles = [];
            var plEntries = readdir(plMusicFs);
            if (plEntries) {
                for (var pi = 0; pi < plEntries.length; pi++) {
                    if (!/\.m3u$/i.test(plEntries[pi])) continue;
                    var plSt = stat(plMusicFs + '/' + plEntries[pi]);
                    if (plSt && plSt.isFile) {
                        plFiles.push({
                            name: plEntries[pi],
                            path: DAV_PREFIX + '/' + plUser.username + '/Music/' + plEntries[pi],
                            mtime: plSt.mtime.getTime()
                        });
                    }
                }
            }
            plFiles.sort(function(a, b) { return b.mtime - a.mtime; });
            return { status: 200, json: {ok: true, playlists: plFiles.slice(0, 5)} };
        }

        if (plAction === 'append') {
            // Append tracks to an existing .m3u file
            var plPath = (plBody.path || '').trim();
            var plTracks = plBody.tracks; // [{path, title, duration}]
            if (!plPath || !plTracks || !plTracks.length) {
                return { status: 400, json: {ok: false, error: 'path and tracks required'} };
            }
            var plDavRel = getDavRelPath(plPath);
            var plFsPath = buildFsPath(plDavRel);
            if (!plFsPath || !stat(plFsPath)) {
                return { status: 404, json: {ok: false, error: 'Playlist file not found'} };
            }
            // Check write permission
            var plPermDenied = checkAccess(plUser, plDavRel, plFsPath, 'PUT');
            if (plPermDenied) {
                return { status: plPermDenied.status, json: {ok: false, error: 'Write permission denied'} };
            }
            var plExisting = readFile(plFsPath, {returnString: true}) || '';
            var plAppend = '';
            for (var ti = 0; ti < plTracks.length; ti++) {
                var t = plTracks[ti];
                var dur = t.duration ? Math.round(t.duration) : -1;
                var tTitle = t.title || t.path.split('/').pop();
                plAppend += '#EXTINF:' + dur + ',' + tTitle + '\n' + t.path + '\n';
            }
            // Ensure file starts with #EXTM3U
            if (plExisting.indexOf('#EXTM3U') !== 0) {
                plExisting = '#EXTM3U\n' + plExisting;
            }
            // Ensure trailing newline
            if (plExisting.length && plExisting[plExisting.length - 1] !== '\n') {
                plExisting += '\n';
            }
            var plFd = fopen(plFsPath, 'w');
            fwrite(plFd, stringToBuffer(plExisting + plAppend));
            fclose(plFd);
            return { status: 200, json: {ok: true} };
        }

        return { status: 400, json: {ok: false, error: 'Unknown action'} };
    }

    /* ============================================================
     * Plugin Endpoints
     * ============================================================ */

    // GET /dav/_plugins — list registered plugins and their extensions
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_plugins') {
        var plgList = [];
        var plgNames = Object.keys(PLUGINS);
        for (var pi = 0; pi < plgNames.length; pi++) {
            var plg = PLUGINS[plgNames[pi]];
            plgList.push({
                name: plg.name,
                extensions: plg.extensions || [],
                mimeTypes: plg.mimeTypes || [],
                mode: plg.mode || 'viewer',
                icon: plg.icon || null,
                singleton: !!plg.singleton
            });
        }
        // Build drop list in sorted order (from PLUGIN_DROP_LIST)
        var plgDropList = [];
        for (var di = 0; di < PLUGIN_DROP_LIST.length; di++) {
            var dplg = PLUGINS[PLUGIN_DROP_LIST[di]];
            if (dplg && dplg.dropPattern) {
                plgDropList.push({
                    name: dplg.name,
                    patterns: dplg.dropPattern.map(function(re) {
                        return { source: re.source, flags: re.flags };
                    })
                });
            }
        }
        return { status: 200, json: {ok: true, plugins: plgList, extMap: PLUGIN_EXT_MAP, mimeMap: PLUGIN_MIME_MAP, dropPlugins: plgDropList} };
    }

    // GET /dav/_plugin/render?file=<davPath>&plugin=<name> — render file with plugin
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_plugin/render') {
        var prUser = authenticate(req);
        if (!prUser) return { status: 401, headers: make401Headers(req), html: '<h1>401 Unauthorized</h1>' };

        var prQuery = req.query || req.path.search || {};
        var prFile = prQuery.file;
        var prPluginName = prQuery.plugin;
        if (!prFile || !prPluginName) return { status: 400, html: '<h1>Missing parameters</h1>' };
        prFile = decodeURIComponent(prFile);

        var prPlugin = PLUGINS[prPluginName];
        if (!prPlugin) return { status: 404, html: '<h1>Plugin not found</h1>' };

        var prDavRel = getDavRelPath(prFile);
        var prFsPath = buildFsPath(prDavRel);
        if (!prFsPath || !stat(prFsPath)) return { status: 404, html: '<h1>File not found</h1>' };

        // Check read permission
        var prMeta = ensureFileMeta(prDavRel, prFsPath);
        if (!checkPermission(prMeta, prUser, 'r')) return { status: 403, html: '<h1>Permission denied</h1>' };

        var prCanEdit = checkPermission(prMeta, prUser, 'w');
        var prContent = bufferToString(readFile(prFsPath));
        var prFileName = prDavRel.split('/').pop();

        try {
            var prHtml = prPlugin.render(prContent, prFileName, prCanEdit, prFile);
            return { status: 200, html: prHtml };
        } catch(e) {
            fprintf(stderr, "Plugin render error (%s): %s\n", prPluginName, e.message || e);
            return { status: 500, html: '<h1>Plugin error: ' + _htmlEsc(e.message || 'unknown') + '</h1>' };
        }
    }

    // GET /dav/_plugin/job?id=<jobId> — check background job status
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_plugin/job') {
        var pjUser = authenticate(req);
        if (!pjUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };

        var pjQuery = req.query || req.path.search || {};
        var pjId = (pjQuery.id || '').trim();
        if (!pjId) return { status: 400, json: {ok: false, error: 'Missing job id'} };

        var pjJob = pluginJobGet(pjId);
        if (!pjJob) return { status: 404, json: {ok: false, error: 'Job not found'} };

        // Check output files (while running and on completion)
        if (pjJob.dir) {
            var pjBytes = 0;
            var pjFiles = [];
            try {
                var pjEntries = readdir(pjJob.dir);
                if (pjEntries) {
                    for (var pji = 0; pji < pjEntries.length; pji++) {
                        var pjSt = stat(pjJob.dir + '/' + pjEntries[pji]);
                        if (pjSt && pjSt.isFile && pjSt.mtime.getTime() >= pjJob.startTime - 1000) {
                            pjBytes += pjSt.size;
                            pjFiles.push(pjEntries[pji]);
                        }
                    }
                }
            } catch(e) {}
            pjJob.bytes = pjBytes;
            pjJob.files = pjFiles;
        }

        return _attachCookie({ status: 200, json: {ok: true, job: pjJob} }, pjUser);
    }

    // POST /dav/_plugin/drop — handle URL drop via plugin
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_plugin/drop') {
        var pdUser = authenticate(req);
        if (!pdUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };

        var pdBody;
        try { pdBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }

        var pdUrl = (pdBody.url || '').trim();
        var pdDavDir = (pdBody.dir || '').trim();
        var pdPluginName = (pdBody.plugin || '').trim();
        var pdChoice = pdBody.choice || null;

        if (!pdUrl || !pdDavDir || !pdPluginName) {
            return { status: 400, json: {ok: false, error: 'Missing url, dir, or plugin'} };
        }

        var pdPlugin = PLUGINS[pdPluginName];
        if (!pdPlugin || !pdPlugin.drop) {
            return { status: 404, json: {ok: false, error: 'Plugin not found or has no drop handler'} };
        }

        // Verify write permission on target directory
        var pdDavRel = getDavRelPath(pdDavDir);
        var pdFsDir = buildFsPath(pdDavRel);
        if (!pdFsDir || !stat(pdFsDir)) {
            return { status: 404, json: {ok: false, error: 'Directory not found'} };
        }
        var pdMeta = ensureFileMeta(pdDavRel, pdFsDir);
        if (!checkPermission(pdMeta, pdUser, 'w')) {
            return { status: 403, json: {ok: false, error: 'Permission denied'} };
        }

        try {
            var pdResult = pdPlugin.drop(pdUrl, pdFsDir, pdDavRel, pdChoice);

            // Plugin can't handle this URL — pass to next plugin
            if (pdResult && pdResult.pass) {
                return _attachCookie({ status: 200, json: {pass: true} }, pdUser);
            }

            // Plugin wants to prompt the user for a choice
            if (pdResult && pdResult.prompt) {
                return _attachCookie({ status: 200, json: {
                    ok: true,
                    prompt: true,
                    title: pdResult.title || 'Choose an option',
                    choices: pdResult.choices || []
                }}, pdUser);
            }

            // Plugin wants to run in background
            // Returns: { background: true, cmd: 'shell command',
            //   dir: '/path', open: true/false }
            if (pdResult && pdResult.background) {
                var pdJobId = hexify(crypto.rand(8));
                var pdStartTime = Date.now();
                pluginJobSet(pdJobId, {
                    status: 'running',
                    plugin: pdPluginName,
                    url: pdUrl,
                    dir: pdFsDir,
                    davDir: pdDavRel,
                    startTime: pdStartTime,
                    open: !!pdResult.open,
                    bytes: 0,
                    files: [],
                    user: pdUser.username
                });

                // Run the command in a thread
                var pdBgThr = new rampart.thread();
                pdBgThr.exec(function(cfg) {
                    var u = rampart.utils;
                    var job;
                    try {
                        var res = u.shell(cfg.cmd,
                            cfg.shellOpts);
                        job = JSON.parse(
                            rampart.thread.get(
                                'plugin_job/' + cfg.jobId)
                            || '{}');
                        job.status = 'complete';
                        job.output =
                            ((res.stderr || '') +
                             (res.stdout || ''))
                            .trim().substring(0, 1000);
                        rampart.thread.put(
                            'plugin_job/' + cfg.jobId,
                            JSON.stringify(job));
                    } catch(e) {
                        job = JSON.parse(
                            rampart.thread.get(
                                'plugin_job/' + cfg.jobId)
                            || '{}');
                        job.status = 'error';
                        job.error = (e.message || 'unknown')
                            .substring(0, 500);
                        rampart.thread.put(
                            'plugin_job/' + cfg.jobId,
                            JSON.stringify(job));
                    }
                }, {
                    cmd: pdResult.cmd,
                    shellOpts: pdResult.shellOpts || {
                        timeout: 600000
                    },
                    jobId: pdJobId
                });

                return _attachCookie({ status: 200, json: {
                    ok: true,
                    background: true,
                    jobId: pdJobId
                }}, pdUser);
            }

            // Check if plugin wants the file opened after creation
            var pdOpen = !!pdResult.open;
            // If the plugin has a render handler, tell the frontend to use it
            var pdOpenPlugin = (pdOpen && pdPlugin.render && pdPlugin.extensions) ? pdPluginName : null;

            // Plugin returned content to save
            if (pdResult && pdResult.name && pdResult.content) {
                var pdFileName = autoRenameFile(pdFsDir, pdResult.name);
                var pdFilePath = pdFsDir + '/' + pdFileName;
                var pdFd = fopen(pdFilePath, 'w');
                fwrite(pdFd, pdResult.content);
                fclose(pdFd);
                createFileMeta(pdDavRel + '/' + pdFileName, pdUser.username, false);
                var pdResp1 = { ok: true, created: true, name: pdFileName, open: pdOpen, openPlugin: pdOpenPlugin };
                return _attachCookie({ status: 200, json: pdResp1 }, pdUser);
            }

            // Plugin created the file directly
            if (pdResult && pdResult.name && pdResult.created) {
                createFileMeta(pdDavRel + '/' + pdResult.name, pdUser.username, false);
                var pdResp2 = { ok: true, created: true, name: pdResult.name, open: pdOpen, openPlugin: pdOpenPlugin };
                return _attachCookie({ status: 200, json: pdResp2 }, pdUser);
            }

            // Plugin returned an error
            if (pdResult && pdResult.error) {
                return _attachCookie({ status: 200, json: {ok: false, error: pdResult.error} }, pdUser);
            }

            return _attachCookie({ status: 200, json: {ok: false, error: 'Plugin returned no result'} }, pdUser);
        } catch(e) {
            fprintf(stderr, "Plugin drop error (%s): %s\n", pdPluginName, e.message || e);
            return { status: 500, json: {ok: false, error: 'Plugin error: ' + (e.message || 'unknown')} };
        }
    }

    /* ============================================================
     * ONLYOFFICE Document Server Integration
     * ============================================================ */

    // Guard: if ONLYOFFICE is not available, return 503 for all _office endpoints
    if (fullPath.indexOf(DAV_PREFIX + '/_office') === 0 && !global.OO_AVAILABLE) {
        return { status: 503, html: '<h1>ONLYOFFICE document server is not available</h1>' };
    }

    // GET /dav/_office?file=<davPath> — serve the editor page
    if (method === 'GET' && fullPath.indexOf(DAV_PREFIX + '/_office') === 0
        && fullPath.indexOf(DAV_PREFIX + '/_office/') !== 0) {
        var ooUser = authenticate(req);
        if (!ooUser) return { status: 401, headers: make401Headers(req), html: '<h1>401 Unauthorized</h1>' };

        var ooFileParam = (req.query || req.path.search || {}).file;
        if (!ooFileParam) return { status: 400, html: '<h1>Missing file parameter</h1>' };
        ooFileParam = decodeURIComponent(ooFileParam);

        var ooDavRel = getDavRelPath(ooFileParam);
        var ooFsPath = buildFsPath(ooDavRel);
        if (!ooFsPath || !stat(ooFsPath)) {
            return { status: 404, html: '<h1>File not found</h1>' };
        }

        // Check read permission
        var ooMeta = ensureFileMeta(ooDavRel, ooFsPath);
        if (!checkPermission(ooMeta, ooUser, 'r')) {
            return { status: 403, html: '<h1>Permission denied</h1>' };
        }

        // Check write permission (determines edit vs view mode)
        var ooCanEdit = checkPermission(ooMeta, ooUser, 'w');
        // Demo mode: force view-only for protected paths
        if (DEMO_MODE && demoIsProtectedPath(ooDavRel)) ooCanEdit = false;

        var ooFileName = ooDavRel.split('/').pop();
        var ooExt = (ooFileName.match(/\.([^.]+)$/) || [])[1] || '';
        ooExt = ooExt.toLowerCase();

        // Map extension to ONLYOFFICE documentType
        var ooDocType = 'word';
        if (/^(xls[xm]?|ods|csv|fods|et|ett)$/.test(ooExt)) ooDocType = 'cell';
        else if (/^(ppt[xm]?|odp|fodp|dps|dpt)$/.test(ooExt)) ooDocType = 'slide';

        // Generate a document key based on file path + mtime (changes when file is modified)
        var ooSt = stat(ooFsPath);
        var ooKey = crypto.sha256(ooDavRel + '|' + ooSt.mtime).substring(0, 20);

        // Build JWT for the editor config
        var ooNow = Math.floor(Date.now() / 1000);
        // Respect X-Forwarded-Proto/Host from upstream reverse proxy
        var ooProto = getHeader(req.headers, 'X-Forwarded-Proto');
        var ooHost = getHeader(req.headers, 'X-Forwarded-Host') || req.path.host;
        var ooOrigin = (ooProto ? ooProto + '://' : req.path.scheme) + ooHost;
        // For ONLYOFFICE callback/fetch URLs: use the appropriate internal hostname
        // Docker compose mode: both containers on same network, use service name
        // Standalone mode: ONLYOFFICE in Docker reaches host via host.docker.internal
        var ooRampartPort = (global.serverConf && global.serverConf.port > 0) ? global.serverConf.port : 8088;
        var ooDockerScheme = (global.serverConf && global.serverConf.secure) ? 'https://' : 'http://';
        var ooDockerOrigin;
        if (global.OO_CALLBACK_HOST) {
            ooDockerOrigin = ooDockerScheme + global.OO_CALLBACK_HOST + (ooRampartPort !== 443 && ooRampartPort !== 80 ? ':' + ooRampartPort : '');
        } else if (global.OO_DOCKER_MODE) {
            ooDockerOrigin = 'http://rampart:' + ooRampartPort;
        } else {
            ooDockerOrigin = ooDockerScheme + 'host.docker.internal:' + ooRampartPort;
        }
        var ooFetchUrl = ooDockerOrigin + '/dav/_office/fetch?file='
            + encodeURIComponent(ooFileParam)
            + '&token=' + encodeURIComponent(_ooSignFetchToken(ooDavRel, ooNow));
        var ooCallbackUrl = ooDockerOrigin + '/dav/_office/callback?file='
            + encodeURIComponent(ooFileParam)
            + '&token=' + encodeURIComponent(_ooSignCallbackToken(ooDavRel, ooUser.username, ooNow));

        var ooEditorConfig = {
            document: {
                fileType: ooExt,
                key: ooKey,
                title: ooFileName,
                url: ooFetchUrl,
                permissions: {
                    edit: ooCanEdit,
                    download: true,
                    print: true,
                    comment: false,
                    review: false
                }
            },
            documentType: ooDocType,
            editorConfig: {
                mode: ooCanEdit ? 'edit' : 'view',
                callbackUrl: ooCallbackUrl,
                lang: 'en',
                user: {
                    id: ooUser.username,
                    name: ooUser.username
                },
                customization: {
                    forcesave: true,
                    autosave: (function() {
                        var ooRec = db.get(userDbi, ooUser.username);
                        return ooRec && typeof ooRec.ooAutosave === 'boolean' ? ooRec.ooAutosave : true;
                    })()
                }
            }
        };

        // Sign the config with JWT
        var ooToken = _ooJwtSign(ooEditorConfig);
        ooEditorConfig.token = ooToken;

        var ooHtml = '<!DOCTYPE html>\n<html><head>\n'
            + '<meta charset="utf-8">\n'
            + '<meta name="viewport" content="width=device-width,initial-scale=1">\n'
            + '<title>' + _htmlEsc(ooFileName) + ' — Editor</title>\n'
            + '<style>html,body{margin:0;padding:0;height:100%;overflow:hidden;background:#f4f4f4}'
            + '#oo-editor{width:100%;height:100%}</style>\n'
            + '</head><body>\n'
            + '<div id="oo-editor"></div>\n'
            + '<script src="/web-apps/apps/api/documents/api.js"><\/script>\n'
            + '<script>\n'
            + 'var ooConfig = ' + JSON.stringify(ooEditorConfig) + ';\n'
            + 'ooConfig.events = {\n'
            + '  onDocumentStateChange: function(e) {\n'
            + '    window.parent.postMessage({type:"oo-dirty", dirty:e.data}, "*");\n'
            + '  }\n'
            + '};\n'
            + 'new DocsAPI.DocEditor("oo-editor", ooConfig);\n'
            + '<\/script>\n</body></html>';

        return { status: 200, html: ooHtml };
    }

    // GET /dav/_office/fetch?file=<davPath>&token=<jwt> — ONLYOFFICE fetches the document
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_office/fetch') {
        var ofQuery = req.query || req.path.search || {};
        var ofFile = ofQuery.file;
        var ofToken = ofQuery.token;
        if (!ofFile || !ofToken) {
            return { status: 400, json: {error: 'Missing parameters'} };
        }
        ofFile = decodeURIComponent(ofFile);

        // Verify the fetch token
        var ofClaims = _ooVerifyFetchToken(ofToken, getDavRelPath(ofFile));
        if (!ofClaims) {
            return { status: 403, json: {error: 'Invalid or expired token'} };
        }

        var ofDavRel = getDavRelPath(ofFile);
        var ofFsPath = buildFsPath(ofDavRel);
        if (!ofFsPath) return { status: 400, json: {error: 'Invalid path'} };
        var ofStat = stat(ofFsPath);
        if (!ofStat || ofStat.isDirectory) return { status: 404, json: {error: 'File not found'} };

        var ofBuf = readFile(ofFsPath);
        if (!ofBuf) return { status: 500, json: {error: 'Failed to read file'} };

        var ofMime = getMimeType(ofFsPath, ofDavRel);
        return {
            status: 200,
            headers: {
                'Content-Type': ofMime || 'application/octet-stream',
                'Content-Disposition': 'attachment; filename="' + encodeURIComponent(ofDavRel.split('/').pop()) + '"'
            },
            bin: ofBuf
        };
    }

    // POST /dav/_office/callback?file=<davPath>&token=<jwt> — ONLYOFFICE save callback
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_office/callback') {
        var ocQuery = req.query || req.path.search || {};
        var ocFile = ocQuery.file;
        var ocToken = ocQuery.token;
        if (!ocFile || !ocToken) {
            return { status: 400, json: {error: 1} };
        }
        ocFile = decodeURIComponent(ocFile);

        var ocDavRel = getDavRelPath(ocFile);

        // Verify callback token
        var ocClaims = _ooVerifyCallbackToken(ocToken, ocDavRel);
        if (!ocClaims) {
            return { status: 403, json: {error: 1} };
        }

        // Also verify the JWT in the Authorization header from ONLYOFFICE
        var ocAuthHeader = getHeader(req.headers, 'Authorization');
        if (ocAuthHeader) {
            var ocAuthParts = ocAuthHeader.split(' ');
            if (ocAuthParts.length === 2 && ocAuthParts[0] === 'Bearer') {
                var ocJwtBody = _ooJwtVerify(ocAuthParts[1]);
                if (!ocJwtBody) {
                    return { status: 403, json: {error: 1} };
                }
            }
        }

        var ocBody;
        try {
            ocBody = JSON.parse(bufferToString(req.body));
        } catch(e) {
            return { status: 400, json: {error: 1} };
        }

        var ocStatus = ocBody.status;

        // Demo mode: block saves to protected paths
        if (DEMO_MODE && demoIsProtectedPath(ocDavRel)) {
            return { status: 200, json: {error: 1} };
        }

        // Status 2 = ready to save, 6 = force save
        if (ocStatus === 2 || ocStatus === 6) {
            var ocDownloadUrl = ocBody.url;
            if (!ocDownloadUrl) {
                return { status: 400, json: {error: 1} };
            }

            // Download the saved document from ONLYOFFICE
            try {
                var ocResp = curl.fetch(ocDownloadUrl, {
                    location: true,
                    insecure: true,
                    "max-time": 60,
                    returnText: false
                });
                if (ocResp.status !== 200) {
                    fprintf(stderr, "ONLYOFFICE callback: download failed with status %d for %s\n", ocResp.status, ocDavRel);
                    return { status: 500, json: {error: 1} };
                }

                var ocFsPath = buildFsPath(ocDavRel);
                if (!ocFsPath) return { status: 400, json: {error: 1} };

                var ocFd = fopen(ocFsPath, 'w');
                fwrite(ocFd, ocResp.body);
                fclose(ocFd);

                // Invalidate thumbnail cache
                var ocThumbPath = THUMB_DIR + '/' + ocDavRel.replace(/\.[^.]+$/, '.jpg');
                try { rmFile(ocThumbPath); } catch(e) {}

                // Update search index
                searchIndexFile(ocFsPath, ocDavRel);

            } catch(e) {
                fprintf(stderr, "ONLYOFFICE callback: error saving %s: %s\n", ocDavRel, e.message || e);
                return { status: 500, json: {error: 1} };
            }
        }

        // Must respond with {"error": 0} for ONLYOFFICE to accept
        return { status: 200, json: {error: 0} };
    }

    // Themes: GET /dav/_themes — list available custom theme CSS files
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_themes') {
        var themesDir = dataRoot.replace(/\/data$/, '') + '/html/css/themes';
        var themeList = [];
        try {
            var entries = readdir(themesDir);
            for (var ti = 0; ti < entries.length; ti++) {
                var tn = entries[ti];
                if (tn.match(/\.css$/i)) {
                    var name = tn.replace(/\.css$/i, '');
                    // Convert filename to display label: "solarized-dark" -> "Solarized Dark"
                    var label = name.replace(/[-_]/g, ' ').replace(/\b\w/g, function(c) { return c.toUpperCase(); });
                    themeList.push({name: name, label: label, file: 'css/themes/' + tn});
                }
            }
        } catch(e) { /* themes dir may not exist */ }
        themeList.sort(function(a, b) { return a.label < b.label ? -1 : a.label > b.label ? 1 : 0; });
        return { status: 200, json: {ok: true, themes: themeList} };
    }

    // Settings: GET /dav/_settings — get current user settings
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_settings') {
        var settingsUser = authenticate(req);
        if (!settingsUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var settingsRecord = db.get(userDbi, settingsUser.username);
        var settingsJson = {
                ok: true,
                username: settingsUser.username,
                admin: DEMO_MODE ? false : settingsUser.admin,
                sessionTimeout: (settingsRecord && typeof settingsRecord.sessionTimeout === 'number') ? settingsRecord.sessionTimeout : DEFAULT_SESSION_SECONDS,
                rcloneAvailable: DEMO_MODE ? false : HAS_RCLONE,
                userGroups: (settingsRecord && settingsRecord.groups) || [],
                theme: (settingsRecord && settingsRecord.theme) || 'auto',
                terminal: DEMO_MODE ? false : !!(settingsRecord && settingsRecord.terminal),
                vnc: DEMO_MODE ? false : !!(settingsRecord && settingsRecord.vnc),
                termTheme: (settingsRecord && settingsRecord.termTheme) || 'auto',
                cmTheme: (settingsRecord && settingsRecord.cmTheme) || 'auto',
                ooAutosave: settingsRecord && typeof settingsRecord.ooAutosave === 'boolean' ? settingsRecord.ooAutosave : true,
                sshHosts: (settingsRecord && settingsRecord.sshHosts) || [],
                demoMode: DEMO_MODE,
                demoClearTime: DEMO_MODE ? DEMO_CLEAR_TIME : 0,
                searchDirs: searchGetUserDirs(settingsUser.username, settingsUser.admin),
                themes: (function() {
                    // Derive the app's filesystem path from the Referer header
                    var htmlRoot = serverConf.htmlRoot || (process.scriptPath + '/html');
                    var referer = getHeader(req.headers, 'Referer') || '';
                    var refPath = referer.replace(/^https?:\/\/[^\/]+/, '').replace(/[?#].*/, '').replace(/\/[^\/]*$/, '');
                    if (!refPath || refPath === '/') refPath = '/filemanager';
                    var themesDir = htmlRoot + refPath + '/css/themes';
                    var themes = [];
                    try {
                        var files = readdir(themesDir);
                        if (files) {
                            for (var ti = 0; ti < files.length; ti++) {
                                if (files[ti].match(/\.css$/i)) {
                                    var name = files[ti].replace(/\.css$/i, '');
                                    var label = name.replace(/[-_]/g, ' ').replace(/\b\w/g, function(c) { return c.toUpperCase(); });
                                    themes.push({value: name, label: label});
                                }
                            }
                            themes.sort(function(a, b) { return a.label < b.label ? -1 : 1; });
                        }
                    } catch(e) {}
                    return themes;
                })()
        };
        return { status: 200, json: settingsJson };
    }

    // Demo mode: block sensitive settings changes
    if (DEMO_MODE && method === 'POST' && fullPath.indexOf(DAV_PREFIX + '/_settings/') === 0) {
        var demoBlockedSettings = ['password', 'timeout', 'revoke'];
        var settingName = fullPath.substring((DAV_PREFIX + '/_settings/').length);
        if (demoBlockedSettings.indexOf(settingName) !== -1) {
            return { status: 403, json: {ok: false, error: 'Disabled in demo mode'} };
        }
    }

    // Demo mode: block all admin endpoints
    if (DEMO_MODE && fullPath.indexOf(DAV_PREFIX + '/_admin/') === 0) {
        return { status: 403, json: {ok: false, error: 'Disabled in demo mode'} };
    }

    // Demo mode: block rclone operations
    if (DEMO_MODE && fullPath.indexOf(DAV_PREFIX + '/_rclone/') === 0 && method === 'POST') {
        return { status: 403, json: {ok: false, error: 'Disabled in demo mode'} };
    }

    // Settings: POST /dav/_settings/password — change password
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_settings/password') {
        var pwUser = authenticate(req);
        if (!pwUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var pwBody;
        try { pwBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        if (!pwBody.currentPassword || !pwBody.newPassword) {
            return { status: 400, json: {ok: false, error: 'Missing fields'} };
        }
        var pwRecord = db.get(userDbi, pwUser.username);
        if (!pwRecord || !crypto.passwdCheck(pwRecord.hash_line, pwBody.currentPassword)) {
            return { status: 403, json: {ok: false, error: 'Current password is incorrect'} };
        }
        if (pwBody.newPassword.length < 4) {
            return { status: 400, json: {ok: false, error: 'Password must be at least 4 characters'} };
        }
        var newHash = crypto.passwd(pwBody.newPassword, null, 'sha512');
        pwRecord.hash_line = newHash.line;
        delete pwRecord.requirePasswordChange;
        db.put(userDbi, pwUser.username, pwRecord);

        // Re-encrypt all SFTP credentials with the new password
        if (HAS_RCLONE) {
            var pwMounts = db.get(rcloneDbi, "", 10000);
            if (pwMounts && typeof pwMounts === 'object') {
                var pwKeys = Object.keys(pwMounts);
                for (var pwi = 0; pwi < pwKeys.length; pwi++) {
                    if (pwKeys[pwi].indexOf(pwUser.username + '/') !== 0) continue;
                    var pwCfg = pwMounts[pwKeys[pwi]];
                    if (!pwCfg || pwCfg.provider !== 'sftp' || !pwCfg.encryptedCreds) continue;
                    try {
                        var pwEncBuf = sprintf("%!B", pwCfg.encryptedCreds);
                        var pwDecBuf = crypto.decrypt({pass: pwBody.currentPassword, data: pwEncBuf, cipher: 'aes-256-cbc'});
                        // Re-encrypt with new password
                        var pwReEnc = crypto.encrypt({pass: pwBody.newPassword, data: bufferToString(pwDecBuf), cipher: 'aes-256-cbc'});
                        pwCfg.encryptedCreds = sprintf("%B", pwReEnc);
                        db.put(rcloneDbi, pwKeys[pwi], pwCfg);
                    } catch(e) {
                        // If decryption fails, leave as-is (shouldn't happen since currentPassword was verified)
                    }
                }
            }
        }

        return { status: 200, json: {ok: true} };
    }

    // Settings: POST /dav/_settings/timeout — change session timeout
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_settings/timeout') {
        var toUser = authenticate(req);
        if (!toUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var toBody;
        try { toBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var timeout = parseInt(toBody.timeout);
        // 0 = never expire, otherwise minimum 300 seconds (5 min)
        if (isNaN(timeout) || (timeout !== 0 && timeout < 300)) {
            return { status: 400, json: {ok: false, error: 'Invalid timeout'} };
        }
        var toRecord = db.get(userDbi, toUser.username);
        if (!toRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        toRecord.sessionTimeout = timeout;
        db.put(userDbi, toUser.username, toRecord);
        // Refresh cookie with new timeout
        var cookie = _createCookie(toUser.username, toRecord);
        return { status: 200, headers: { 'Set-Cookie': cookie }, json: {ok: true} };
    }

    // Settings: POST /dav/_settings/theme — change theme preference
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_settings/theme') {
        var thUser = authenticate(req);
        if (!thUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var thBody;
        try { thBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var theme = thBody.theme;
        if (!theme || typeof theme !== 'string' || !/^[a-zA-Z0-9_-]+$/.test(theme)) {
            return { status: 400, json: {ok: false, error: 'Invalid theme name'} };
        }
        var thRecord = db.get(userDbi, thUser.username);
        if (!thRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        thRecord.theme = theme;
        db.put(userDbi, thUser.username, thRecord);
        return { status: 200, json: {ok: true} };
    }

    // Settings: POST /dav/_settings/termTheme — change terminal theme
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_settings/termTheme') {
        var ttUser = authenticate(req);
        if (!ttUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var ttBody;
        try { ttBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var ttRecord = db.get(userDbi, ttUser.username);
        if (!ttRecord || !ttRecord.terminal) {
            return { status: 403, json: {ok: false, error: 'Terminal not enabled'} };
        }
        ttRecord.termTheme = ttBody.termTheme || 'auto';
        db.put(userDbi, ttUser.username, ttRecord);
        return { status: 200, json: {ok: true} };
    }

    // Settings: POST /dav/_settings/cmTheme — change code editor theme
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_settings/cmTheme') {
        var cmUser = authenticate(req);
        if (!cmUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var cmBody;
        try { cmBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var cmTheme = cmBody.cmTheme;
        if (!cmTheme || typeof cmTheme !== 'string' || !/^[a-zA-Z0-9_-]+$/.test(cmTheme)) {
            return { status: 400, json: {ok: false, error: 'Invalid theme name'} };
        }
        var cmRecord = db.get(userDbi, cmUser.username);
        if (!cmRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        cmRecord.cmTheme = cmTheme;
        db.put(userDbi, cmUser.username, cmRecord);
        return { status: 200, json: {ok: true} };
    }

    // Settings: POST /dav/_settings/ooAutosave — toggle ONLYOFFICE autosave
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_settings/ooAutosave') {
        var oaUser = authenticate(req);
        if (!oaUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var oaBody;
        try { oaBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        if (typeof oaBody.ooAutosave !== 'boolean') {
            return { status: 400, json: {ok: false, error: 'Invalid value'} };
        }
        var oaRecord = db.get(userDbi, oaUser.username);
        if (!oaRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        oaRecord.ooAutosave = oaBody.ooAutosave;
        db.put(userDbi, oaUser.username, oaRecord);
        return { status: 200, json: {ok: true} };
    }

    // Settings: POST /dav/_settings/sshHosts — save recent SSH hosts list
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_settings/sshHosts') {
        var shUser = authenticate(req);
        if (!shUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var shBody;
        try { shBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        if (!Array.isArray(shBody.hosts)) {
            return { status: 400, json: {ok: false, error: 'hosts must be an array'} };
        }
        var shRecord = db.get(userDbi, shUser.username);
        if (!shRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        // Keep at most 10 entries, strings only
        shRecord.sshHosts = shBody.hosts.filter(function(h) { return typeof h === 'string'; }).slice(0, 10);
        db.put(userDbi, shUser.username, shRecord);
        return { status: 200, json: {ok: true} };
    }

    // Settings: POST /dav/_settings/revoke — revoke all sessions (regenerate key)
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_settings/revoke') {
        var rvUser = authenticate(req);
        if (!rvUser) {
            return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        }
        var rvRecord = db.get(userDbi, rvUser.username);
        if (!rvRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        // Regenerate session key — all existing cookies become invalid
        rvRecord.sessionKey = hexify(crypto.rand(32));
        db.put(userDbi, rvUser.username, rvRecord);
        return {
            status: 200,
            headers: { 'Set-Cookie': _clearCookie() },
            json: {ok: true}
        };
    }

    // Admin: GET /dav/_admin/users — list all users
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_admin/users') {
        var adUser = authenticate(req);
        if (!adUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!adUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var allUsers = db.get(userDbi, "", 10000);
        var list = [];
        if (allUsers && typeof allUsers === 'object') {
            var keys = Object.keys(allUsers);
            for (var ai = 0; ai < keys.length; ai++) {
                var au = allUsers[keys[ai]];
                list.push({
                    username: keys[ai],
                    admin: !!au.admin,
                    created: au.created || null,
                    groups: au.groups || [],
                    terminal: !!au.terminal,
                    vnc: !!au.vnc
                });
            }
        }
        return { status: 200, json: {ok: true, users: list} };
    }

    // Admin: POST /dav/_admin/adduser — create a new user
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/adduser') {
        var adUser = authenticate(req);
        if (!adUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!adUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var addBody;
        try { addBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var newUsername = (addBody.username || '').trim().toLowerCase();
        var newPassword = addBody.password || '';
        if (!newUsername || !newPassword) {
            return { status: 400, json: {ok: false, error: 'Username and password required'} };
        }
        if (!/^[a-zA-Z0-9_-]{1,32}$/.test(newUsername)) {
            return { status: 400, json: {ok: false, error: 'Username must be 1-32 chars: letters, digits, underscore, hyphen'} };
        }
        if (newUsername === 'shared') {
            return { status: 400, json: {ok: false, error: "'shared' is reserved"} };
        }
        if (newPassword.length < 4) {
            return { status: 400, json: {ok: false, error: 'Password must be at least 4 characters'} };
        }
        var existingUser = db.get(userDbi, newUsername);
        if (existingUser) {
            return { status: 409, json: {ok: false, error: 'User already exists'} };
        }
        var hashResult = crypto.passwd(newPassword, null, 'sha512');
        var newRecord = {
            hash_line: hashResult.line,
            admin: false,
            created: new Date().toISOString(),
            groups: []
        };
        if (addBody.requirePasswordChange) newRecord.requirePasswordChange = true;
        db.put(userDbi, newUsername, newRecord);
        ensureUserHome(newUsername);
        return { status: 200, json: {ok: true} };
    }

    // Admin: POST /dav/_admin/deluser — delete a user (preserves home dir)
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/deluser') {
        var adUser = authenticate(req);
        if (!adUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!adUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var delBody;
        try { delBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var delUsername = (delBody.username || '').trim();
        if (!delUsername) {
            return { status: 400, json: {ok: false, error: 'Username required'} };
        }
        if (delUsername === adUser.username) {
            return { status: 400, json: {ok: false, error: 'Cannot delete yourself'} };
        }
        var delRecord = db.get(userDbi, delUsername);
        if (!delRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        if (delRecord.admin) {
            return { status: 400, json: {ok: false, error: 'Cannot delete an admin — demote first'} };
        }
        db.del(userDbi, delUsername);
        return { status: 200, json: {ok: true} };
    }

    // Admin: POST /dav/_admin/resetpass — reset a user's password
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/resetpass') {
        var adUser = authenticate(req);
        if (!adUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!adUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var rpBody;
        try { rpBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var rpUsername = (rpBody.username || '').trim();
        var rpPassword = rpBody.password || '';
        if (!rpUsername || !rpPassword) {
            return { status: 400, json: {ok: false, error: 'Username and password required'} };
        }
        if (rpPassword.length < 4) {
            return { status: 400, json: {ok: false, error: 'Password must be at least 4 characters'} };
        }
        var rpRecord = db.get(userDbi, rpUsername);
        if (!rpRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        var rpHash = crypto.passwd(rpPassword, null, 'sha512');
        rpRecord.hash_line = rpHash.line;
        if (rpBody.requirePasswordChange) rpRecord.requirePasswordChange = true;
        else delete rpRecord.requirePasswordChange;
        // Revoke their sessions too
        rpRecord.sessionKey = hexify(crypto.rand(32));
        db.put(userDbi, rpUsername, rpRecord);
        return { status: 200, json: {ok: true} };
    }

    // Admin: POST /dav/_admin/toggleadmin — change admin status
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/toggleadmin') {
        var adUser = authenticate(req);
        if (!adUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!adUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var taBody;
        try { taBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var taUsername = (taBody.username || '').trim();
        if (!taUsername) {
            return { status: 400, json: {ok: false, error: 'Username required'} };
        }
        if (taUsername === adUser.username) {
            return { status: 400, json: {ok: false, error: 'Cannot change your own admin status'} };
        }
        var taRecord = db.get(userDbi, taUsername);
        if (!taRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        var newAdmin = (taBody.admin !== undefined) ? !!taBody.admin : !taRecord.admin;
        // Prevent demoting the last admin
        if (taRecord.admin && !newAdmin) {
            var allU = db.get(userDbi, "", 10000);
            var adminCount = 0;
            if (allU) { var uk = Object.keys(allU); for (var ui = 0; ui < uk.length; ui++) { if (allU[uk[ui]].admin) adminCount++; } }
            if (adminCount <= 1) {
                return { status: 400, json: {ok: false, error: 'Cannot demote — at least one admin must exist'} };
            }
        }
        taRecord.admin = newAdmin;
        db.put(userDbi, taUsername, taRecord);
        return { status: 200, json: {ok: true, admin: taRecord.admin} };
    }

    // Admin: POST /dav/_admin/terminal — toggle terminal access for a user
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/terminal') {
        var adUser = authenticate(req);
        if (!adUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!adUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var ttBody;
        try { ttBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var ttUsername = (ttBody.username || '').trim();
        if (!ttUsername) {
            return { status: 400, json: {ok: false, error: 'Username required'} };
        }
        var ttRecord = db.get(userDbi, ttUsername);
        if (!ttRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        ttRecord.terminal = !ttRecord.terminal;
        db.put(userDbi, ttUsername, ttRecord);
        return { status: 200, json: {ok: true, terminal: ttRecord.terminal} };
    }

    // Admin: POST /dav/_admin/vnc — toggle VNC access for a user
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/vnc') {
        var adUser = authenticate(req);
        if (!adUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!adUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var vnBody;
        try { vnBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var vnUsername = (vnBody.username || '').trim();
        if (!vnUsername) {
            return { status: 400, json: {ok: false, error: 'Username required'} };
        }
        var vnRecord = db.get(userDbi, vnUsername);
        if (!vnRecord) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        vnRecord.vnc = !vnRecord.vnc;
        db.put(userDbi, vnUsername, vnRecord);
        return { status: 200, json: {ok: true, vnc: vnRecord.vnc} };
    }

    // Admin: GET /dav/_admin/groups — list all groups with members
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_admin/groups') {
        var grpUser = authenticate(req);
        if (!grpUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!grpUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var allGroups = db.get(groupDbi, "", 10000);
        var allUsersForGroups = db.get(userDbi, "", 10000);
        var groupList = [];
        if (allGroups && typeof allGroups === 'object') {
            var gKeys = Object.keys(allGroups);
            for (var gi = 0; gi < gKeys.length; gi++) {
                var grp = allGroups[gKeys[gi]];
                // Find members by scanning users
                var members = [];
                if (gKeys[gi] === 'everyone') {
                    // "everyone" implicitly includes all users
                    if (allUsersForGroups && typeof allUsersForGroups === 'object') {
                        members = Object.keys(allUsersForGroups);
                    }
                } else if (allUsersForGroups && typeof allUsersForGroups === 'object') {
                    var uKeys = Object.keys(allUsersForGroups);
                    for (var ui = 0; ui < uKeys.length; ui++) {
                        var uRec = allUsersForGroups[uKeys[ui]];
                        if (uRec.groups && uRec.groups.indexOf(gKeys[gi]) !== -1) {
                            members.push(uKeys[ui]);
                        }
                    }
                }
                groupList.push({ name: gKeys[gi], members: members, created: grp.created || null });
            }
        }
        return { status: 200, json: {ok: true, groups: groupList} };
    }

    // Admin: POST /dav/_admin/addgroup — create a group
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/addgroup') {
        var agUser = authenticate(req);
        if (!agUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!agUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var agBody;
        try { agBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var groupName = (agBody.name || '').trim().toLowerCase();
        if (!groupName || !/^[a-z0-9_-]{1,32}$/.test(groupName)) {
            return { status: 400, json: {ok: false, error: 'Group name must be 1-32 chars: lowercase letters, digits, underscore, hyphen'} };
        }
        if (groupName === 'nogroup' || groupName === 'everyone' || groupName === 'shared') {
            return { status: 400, json: {ok: false, error: "'" + groupName + "' is reserved"} };
        }
        var existingGroup = db.get(groupDbi, groupName);
        if (existingGroup) {
            return { status: 409, json: {ok: false, error: 'Group already exists'} };
        }
        db.put(groupDbi, groupName, { name: groupName, created: new Date().toISOString() });
        return { status: 200, json: {ok: true} };
    }

    // Admin: POST /dav/_admin/delgroup — delete a group
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/delgroup') {
        var dgUser = authenticate(req);
        if (!dgUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!dgUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var dgBody;
        try { dgBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var delGroupName = (dgBody.name || '').trim();
        if (!delGroupName) {
            return { status: 400, json: {ok: false, error: 'Group name required'} };
        }
        if (delGroupName === 'nogroup' || delGroupName === 'everyone') {
            return { status: 400, json: {ok: false, error: "Cannot delete '" + delGroupName + "'"} };
        }
        var delGrp = db.get(groupDbi, delGroupName);
        if (!delGrp) {
            return { status: 404, json: {ok: false, error: 'Group not found'} };
        }
        // Remove group from all users' groups arrays
        var dgAllUsers = db.get(userDbi, "", 10000);
        if (dgAllUsers && typeof dgAllUsers === 'object') {
            var dgUKeys = Object.keys(dgAllUsers);
            for (var dgi = 0; dgi < dgUKeys.length; dgi++) {
                var dgRec = dgAllUsers[dgUKeys[dgi]];
                if (dgRec.groups && dgRec.groups.indexOf(delGroupName) !== -1) {
                    dgRec.groups = dgRec.groups.filter(function(g) { return g !== delGroupName; });
                    db.put(userDbi, dgUKeys[dgi], dgRec);
                }
            }
        }
        // Reset any file metadata with this group to 'nogroup'
        var allMeta = db.get(filemetaDbi, "", 100000);
        if (allMeta && typeof allMeta === 'object') {
            var mKeys = Object.keys(allMeta);
            for (var mi = 0; mi < mKeys.length; mi++) {
                var fm = allMeta[mKeys[mi]];
                if (fm.group === delGroupName) {
                    fm.group = 'nogroup';
                    db.put(filemetaDbi, mKeys[mi], fm);
                }
            }
        }
        db.del(groupDbi, delGroupName);
        return { status: 200, json: {ok: true} };
    }

    // Admin: POST /dav/_admin/groupmember — add/remove user from group
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/groupmember') {
        var gmUser = authenticate(req);
        if (!gmUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!gmUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var gmBody;
        try { gmBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var gmGroup = (gmBody.group || '').trim();
        var gmUsername = (gmBody.username || '').trim();
        var gmAction = (gmBody.action || '').trim();
        if (!gmGroup || !gmUsername || (gmAction !== 'add' && gmAction !== 'remove')) {
            return { status: 400, json: {ok: false, error: 'group, username, and action (add/remove) required'} };
        }
        if (gmGroup === 'everyone') {
            return { status: 400, json: {ok: false, error: '"everyone" includes all users automatically'} };
        }
        // Validate group exists
        var gmGrp = db.get(groupDbi, gmGroup);
        if (!gmGrp) {
            return { status: 404, json: {ok: false, error: 'Group not found'} };
        }
        // Validate user exists
        var gmRec = db.get(userDbi, gmUsername);
        if (!gmRec) {
            return { status: 404, json: {ok: false, error: 'User not found'} };
        }
        if (!gmRec.groups) gmRec.groups = [];
        if (gmAction === 'add') {
            if (gmRec.groups.indexOf(gmGroup) === -1) {
                gmRec.groups.push(gmGroup);
            }
        } else {
            gmRec.groups = gmRec.groups.filter(function(g) { return g !== gmGroup; });
        }
        db.put(userDbi, gmUsername, gmRec);
        return { status: 200, json: {ok: true} };
    }

    // Admin: GET /dav/_admin/extpaths — list allowed external paths
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_admin/extpaths') {
        var epUser = authenticate(req);
        if (!epUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!epUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var stored = db.get(extpathsDbi, "", 10000);
        var paths = [];
        if (stored && typeof stored === 'object') {
            var keys = Object.keys(stored);
            for (var ei = 0; ei < keys.length; ei++) {
                paths.push(stored[keys[ei]]);
            }
        }
        return { status: 200, json: {ok: true, paths: paths} };
    }

    // Admin: POST /dav/_admin/addextpath — add an external allowed path
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/addextpath') {
        var aepUser = authenticate(req);
        if (!aepUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!aepUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var aepBody;
        try { aepBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var addPath = (aepBody.path || '').trim();
        if (!addPath || addPath.charAt(0) !== '/') {
            return { status: 400, json: {ok: false, error: 'Absolute path required'} };
        }
        if (!stat(addPath)) {
            return { status: 404, json: {ok: false, error: 'Path does not exist on filesystem'} };
        }
        var resolvedAdd;
        try { resolvedAdd = realPath(addPath); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Cannot resolve path'} };
        }
        _addExternalPath(resolvedAdd);
        return { status: 200, json: {ok: true, resolvedPath: resolvedAdd} };
    }

    // Admin: POST /dav/_admin/delextpath — remove an external allowed path
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/delextpath') {
        var depUser = authenticate(req);
        if (!depUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!depUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var depBody;
        try { depBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var delPath = (depBody.path || '').trim();
        if (!delPath) {
            return { status: 400, json: {ok: false, error: 'Path required'} };
        }
        _removeExternalPath(delPath);
        return { status: 200, json: {ok: true} };
    }

    // Admin: POST /dav/_admin/symlink — create symlink to external path
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_admin/symlink') {
        var esUser = authenticate(req);
        if (!esUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!esUser.admin) return { status: 403, json: {ok: false, error: 'Admin required'} };
        var esBody;
        try { esBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var extTarget = (esBody.target || '').trim();
        var extLinkDav = (esBody.link || '').trim();
        if (!extTarget || !extLinkDav) {
            return { status: 400, json: {ok: false, error: 'Both target and link paths required'} };
        }
        if (!stat(extTarget)) {
            return { status: 404, json: {ok: false, error: 'Target path does not exist on filesystem'} };
        }
        var resolvedExtTarget;
        try { resolvedExtTarget = realPath(extTarget); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Cannot resolve target path'} };
        }
        _addExternalPath(resolvedExtTarget);
        var extLinkRel = getDavRelPath(extLinkDav);
        var extLinkFsPath = buildFsPath(extLinkRel);
        if (!extLinkFsPath) {
            return { status: 400, json: {ok: false, error: 'Invalid link path'} };
        }
        if (stat(extLinkFsPath) || lstat(extLinkFsPath)) {
            return { status: 409, json: {ok: false, error: 'A file already exists at the link path'} };
        }
        var extLinkParent = getParentDir(extLinkFsPath);
        if (!stat(extLinkParent)) {
            return { status: 409, json: {ok: false, error: 'Parent directory does not exist'} };
        }
        var extRelTarget = computeRelativePath(extLinkParent, resolvedExtTarget);
        try {
            symlink({src: extRelTarget, target: extLinkFsPath});
        } catch(e) {
            return { status: 500, json: {ok: false, error: 'Failed to create symlink: ' + e.message} };
        }
        var extIsDir = stat(resolvedExtTarget).isDirectory;
        createFileMeta(extLinkRel + (extIsDir ? '/' : ''), esUser.username, extIsDir);
        return { status: 201, json: {ok: true} };
    }

    // Archive creation: POST /dav/_archive
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_archive') {
        var arcUser = authenticate(req);
        if (!arcUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var arcBody;
        try { arcBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var arcAction = arcBody.action;

        // --- STATUS: check if background archive job is still running ---
        if (arcAction === 'status') {
            var sjId = arcBody.jobId;
            if (!sjId) return { status: 400, json: {ok: false, error: 'Missing jobId'} };
            var sjob = rampart.thread.get('archive_' + sjId);
            if (!sjob) return { status: 404, json: {ok: false, error: 'Job not found'} };
            var sjRunning = false;
            try {
                sjRunning = kill(parseInt(sjob.pid), 0);
            } catch(e) {}
            var sjSize = 0;
            try { var sjSt = stat(sjob.tempPath); if (sjSt) sjSize = sjSt.size; } catch(e) {}
            if (!sjRunning) {
                // Process finished — check if archive was created successfully
                var sjOk = false;
                try { var sjFinal = stat(sjob.tempPath); sjOk = sjFinal && sjFinal.size > 0; } catch(e) {}
                if (sjOk) {
                    // Rename temp file to final destination
                    try {
                        rename(sjob.tempPath, sjob.destPath);
                    } catch(e) {
                        // If rename fails (e.g. cross-device), try copy+delete
                        try {
                            copyFile(sjob.tempPath, sjob.destPath);
                            rmFile(sjob.tempPath);
                        } catch(e2) {
                            sjOk = false;
                        }
                    }
                    sjSize = 0;
                    try { var sjDest = stat(sjob.destPath); if (sjDest) sjSize = sjDest.size; } catch(e) {}
                } else {
                    // Clean up failed temp file
                    try { if (stat(sjob.tempPath)) rmFile(sjob.tempPath); } catch(e) {}
                }
                // Clean up job record
                rampart.thread.put('archive_' + sjId, null);
                return { status: 200, json: {ok: true, running: false, success: sjOk, size: sjSize} };
            }
            return { status: 200, json: {ok: true, running: true, size: sjSize} };
        }

        // --- CANCEL: kill running archive job ---
        if (arcAction === 'cancel') {
            var cjId = arcBody.jobId;
            if (!cjId) return { status: 400, json: {ok: false, error: 'Missing jobId'} };
            var cjob = rampart.thread.get('archive_' + cjId);
            if (!cjob) return { status: 404, json: {ok: false, error: 'Job not found'} };
            // Kill the process tree (negative PID kills the process group)
            kill(parseInt(cjob.pid));
            // Remove partial output
            try { if (stat(cjob.tempPath)) rmFile(cjob.tempPath); } catch(e) {}
            rampart.thread.put('archive_' + cjId, null);
            return { status: 200, json: {ok: true} };
        }

        // --- START: begin archive creation ---
        if (arcAction !== 'start') {
            return { status: 400, json: {ok: false, error: 'Unknown action: ' + arcAction} };
        }
        var arcPaths = arcBody.paths;   // array of DAV paths e.g. ["/dav/aaron/file.txt", "/dav/aaron/dir/"]
        var arcFormat = arcBody.format;  // "zip" or "tar.gz"
        var arcDest = arcBody.dest;      // DAV path for the output file e.g. "/dav/aaron/archive.zip"
        if (!arcPaths || !arcPaths.length || !arcFormat || !arcDest) {
            return { status: 400, json: {ok: false, error: 'Missing paths, format, or dest'} };
        }
        if (arcFormat !== 'zip' && arcFormat !== 'tar.gz') {
            return { status: 400, json: {ok: false, error: 'Format must be zip or tar.gz'} };
        }

        // Resolve destination path
        var arcDestRel = getDavRelPath(arcDest);
        var arcDestFs = buildFsPath(arcDestRel);
        if (!arcDestFs) return { status: 400, json: {ok: false, error: 'Invalid destination path'} };
        var arcDestParent = arcDestFs.substring(0, arcDestFs.lastIndexOf('/'));
        if (!stat(arcDestParent)) {
            return { status: 400, json: {ok: false, error: 'Destination directory does not exist'} };
        }
        // Check if destination already exists
        if (stat(arcDestFs)) {
            return { status: 409, json: {ok: false, error: 'Destination file already exists'} };
        }
        // Check write permission on destination
        if (!authorize(arcUser, arcDestRel, 'PUT')) {
            return { status: 403, json: {ok: false, error: 'No write access to destination'} };
        }
        var arcDestPermDenied = checkAccess(arcUser, getDavRelPath(arcDest.substring(0, arcDest.lastIndexOf('/') + 1)), arcDestParent, 'PUT');
        if (arcDestPermDenied) {
            return { status: 403, json: {ok: false, error: arcDestPermDenied.msg} };
        }

        // Resolve all source paths and find common base directory
        var arcNames = [];       // names relative to base dir
        var arcBaseFsPath = null;
        for (var ai = 0; ai < arcPaths.length; ai++) {
            var arcSrcRel = getDavRelPath(arcPaths[ai].replace(/\/$/, ''));
            var arcSrcFs = buildFsPath(arcSrcRel);
            if (!arcSrcFs) return { status: 400, json: {ok: false, error: 'Invalid source path: ' + arcPaths[ai]} };
            if (!stat(arcSrcFs) && !lstat(arcSrcFs)) {
                return { status: 404, json: {ok: false, error: 'Source not found: ' + arcPaths[ai]} };
            }
            if (!checkAllowedPath(arcSrcFs)) {
                return { status: 403, json: {ok: false, error: 'Source path not allowed'} };
            }
            if (!authorize(arcUser, arcSrcRel, 'GET')) {
                return { status: 403, json: {ok: false, error: 'No read access to source'} };
            }
            // Extract parent dir and filename
            var arcSrcParent = arcSrcFs.substring(0, arcSrcFs.lastIndexOf('/'));
            var arcSrcName = arcSrcFs.substring(arcSrcFs.lastIndexOf('/') + 1);
            if (arcBaseFsPath === null) {
                arcBaseFsPath = arcSrcParent;
            } else if (arcBaseFsPath !== arcSrcParent) {
                return { status: 400, json: {ok: false, error: 'All source files must be in the same directory'} };
            }
            arcNames.push(arcSrcName);
        }

        // Use a temp file so partial archives don't appear as real files
        var arcTempName = '.~archive-' + Date.now() + '-' + hexify(crypto.rand(4)) +
            (arcFormat === 'zip' ? '.zip' : '.tar.gz');
        var arcTempPath = arcDestParent + '/' + arcTempName;

        // Build the command
        var arcCmd;
        if (arcFormat === 'tar.gz') {
            arcCmd = 'tar czf ' + _shellEscape(arcTempPath) + ' -C ' + _shellEscape(arcBaseFsPath);
            for (var ti = 0; ti < arcNames.length; ti++) {
                arcCmd += ' ' + _shellEscape(arcNames[ti]);
            }
        } else {
            arcCmd = 'cd ' + _shellEscape(arcBaseFsPath) + ' && zip -r -q ' + _shellEscape(arcTempPath);
            for (var zi = 0; zi < arcNames.length; zi++) {
                arcCmd += ' ' + _shellEscape(arcNames[zi]);
            }
        }
        // Run synchronously first to capture errors, then if the file set is large
        // we can consider background later.  For now, test with shell() to see errors.
        try {
            shell(arcCmd, {timeout: 300000});  // 5 minute timeout
        } catch(e) {}

        // Verify the archive was created
        var arcTempStat = stat(arcTempPath);
        if (!arcTempStat || arcTempStat.size === 0) {
            try { if (stat(arcTempPath)) rmFile(arcTempPath); } catch(e2) {}
            return { status: 500, json: {ok: false, error: 'Archive creation failed'} };
        }

        // Rename temp file to final destination
        var arcOk = false;
        try {
            var arcTmpSt = stat(arcTempPath);
            if (arcTmpSt && arcTmpSt.size > 0) {
                try {
                    rename(arcTempPath, arcDestFs);
                } catch(e) {
                    copyFile(arcTempPath, arcDestFs);
                    rmFile(arcTempPath);
                }
                arcOk = true;
            }
        } catch(e) {
            return { status: 500, json: {ok: false, error: 'Failed to finalize archive: ' + e.message} };
        }

        if (!arcOk) {
            return { status: 500, json: {ok: false, error: 'Archive produced empty file. cmd: ' + arcCmd} };
        }

        var arcFinalSize = 0;
        try { var fs2 = stat(arcDestFs); if (fs2) arcFinalSize = fs2.size; } catch(e) {}

        return { status: 200, json: {ok: true, sync: true, size: arcFinalSize} };
    }

    // Symlink creation: POST /dav/_symlink
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_symlink') {
        var slUser = authenticate(req);
        if (!slUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        var slBody;
        try { slBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        var targetDavPath = (slBody.target || '').trim();
        var linkDavPath = (slBody.link || '').trim();
        if (!targetDavPath || !linkDavPath) {
            return { status: 400, json: {ok: false, error: 'Both target and link paths required'} };
        }
        var targetRel = getDavRelPath(targetDavPath);
        var linkRel = getDavRelPath(linkDavPath);
        var targetFsPath = buildFsPath(targetRel);
        var linkFsPath = buildFsPath(linkRel);
        if (!targetFsPath || !linkFsPath) {
            return { status: 400, json: {ok: false, error: 'Invalid path'} };
        }
        // Permission: user must be able to read the target
        if (!authorize(slUser, targetRel, 'GET')) {
            return { status: 403, json: {ok: false, error: 'No read access to target'} };
        }
        // File-level read permission on target
        var slTargetPerm = checkAccess(slUser, targetRel, targetFsPath, 'GET');
        if (slTargetPerm) return { status: 403, json: {ok: false, error: 'No read access to target'} };
        // Permission: user must be able to write to the link's parent
        var linkParentRel = linkRel.substring(0, linkRel.lastIndexOf('/')) || '/';
        if (!authorize(slUser, linkParentRel, 'PUT')) {
            return { status: 403, json: {ok: false, error: 'No write access to destination'} };
        }
        // File-level write permission on link parent
        var linkParentFs = buildFsPath(linkParentRel);
        if (linkParentFs) {
            var slLinkPerm = checkAccess(slUser, linkParentRel, linkParentFs, 'PUT');
            if (slLinkPerm) return { status: 403, json: {ok: false, error: 'No write access to destination'} };
        }
        if (!stat(targetFsPath)) {
            return { status: 404, json: {ok: false, error: 'Target does not exist'} };
        }
        if (!checkAllowedPath(targetFsPath)) {
            return { status: 403, json: {ok: false, error: 'Target path not allowed'} };
        }
        if (stat(linkFsPath) || lstat(linkFsPath)) {
            return { status: 409, json: {ok: false, error: 'A file already exists at the link path'} };
        }
        var linkParentFsPath = getParentDir(linkFsPath);
        if (!stat(linkParentFsPath)) {
            return { status: 409, json: {ok: false, error: 'Parent directory does not exist'} };
        }
        if (!checkAllowedPath(linkFsPath)) {
            return { status: 403, json: {ok: false, error: 'Link path not allowed'} };
        }
        var relTarget = computeRelativePath(linkParentFsPath, targetFsPath);
        try {
            symlink({src: relTarget, target: linkFsPath});
        } catch(e) {
            return { status: 500, json: {ok: false, error: 'Failed to create symlink: ' + e.message} };
        }
        var linkIsDir = stat(targetFsPath).isDirectory;
        createFileMeta(linkRel + (linkIsDir ? '/' : ''), slUser.username, linkIsDir);
        return { status: 201, json: {ok: true} };
    }

    // ---- rclone Cloud Storage Endpoints ----

    // GET /dav/_rclone/status — rclone availability and provider list
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_rclone/status') {
        var rcUser = authenticate(req);
        if (!rcUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };

        return _attachCookie({
            status: 200,
            json: {
                ok: true,
                available: HAS_RCLONE,
                version: RCLONE_VERSION,
                providers: RCLONE_PROVIDERS
            }
        }, rcUser);
    }

    // GET /dav/_rclone/mounts — list current user's mounts with live status
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_rclone/mounts') {
        var rmUser = authenticate(req);
        if (!rmUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!HAS_RCLONE) return _attachCookie({ status: 200, json: {ok: true, mounts: []} }, rmUser);

        var mounts = [];
        var rmPrefix = rmUser.username + '/';
        var allMounts = db.get(rcloneDbi, "", 10000);
        if (allMounts && typeof allMounts === 'object') {
            var rmKeys = Object.keys(allMounts);
            for (var rmi = 0; rmi < rmKeys.length; rmi++) {
                if (rmKeys[rmi].indexOf(rmPrefix) === 0 && rmKeys[rmi].indexOf('_oauth_') !== 0) {
                    var rmCfg = allMounts[rmKeys[rmi]];
                    var rmMountPoint = getUserMountDir(rmUser.username, rmCfg.name, rmCfg.rootMount);
                    var rmMounted = _isMounted(rmMountPoint);
                    var rmStale = false;
                    // Detect and recover stale FUSE mounts
                    if (rmMounted && _isStaleMount(rmMountPoint)) {
                        fprintf(stderr, "Stale mount detected for '%s', recovering...\n", rmCfg.name);
                        _recoverStaleMount(rmMountPoint);
                        // Try to remount
                        var rmEnv = rmCfg.envPrefix || '';
                        var rmResult = rcloneMountRemote(rmUser.username, rmCfg.name, rmCfg.name,
                            rmCfg.remotePath || '', '', rmEnv, rmCfg.rootMount);
                        rmMounted = rmResult && rmResult.ok;
                        if (!rmMounted) rmStale = true;
                    }

                    mounts.push({
                        name: rmCfg.name,
                        type: rmCfg.type,
                        provider: rmCfg.provider,
                        active: rmCfg.active,
                        mounted: rmMounted,
                        stale: rmStale,
                        created: rmCfg.created,
                        mountPath: rmCfg.mountPath,
                        readOnly: rmCfg.readOnly || false,
                        rootMount: rmCfg.rootMount || false
                    });
                }
            }
        }
        return _attachCookie({ status: 200, json: {ok: true, mounts: mounts} }, rmUser);
    }

    // POST /dav/_rclone/create — create new rclone remote and mount it
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_rclone/create') {
        var crUser = authenticate(req);
        if (!crUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!HAS_RCLONE) return { status: 400, json: {ok: false, error: 'rclone not available'} };

        var crBody;
        try { crBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }

        var crMountName = (crBody.name || '').trim().toLowerCase().replace(/[^a-z0-9._-]/g, '');
        var crProviderType = crBody.type;
        var crTier = crBody.tier;
        var crParams = crBody.params || {};

        if (!crMountName || crMountName.length < 1 || crMountName.length > 32) {
            return { status: 400, json: {ok: false, error: 'Invalid mount name (1-32 chars, a-z0-9._-)'} };
        }

        // Reserved names
        if (crMountName === 'trash' || crMountName === 'shared') {
            return { status: 400, json: {ok: false, error: 'Reserved name'} };
        }

        // Root mount (admin-only): mount in DAV root visible to all users
        var crRootMount = !!crBody.rootMount;
        if (crRootMount) {
            if (!crUser.admin) {
                return { status: 403, json: {ok: false, error: 'Only admins can create root-level mounts'} };
            }
            // Ensure mount name doesn't collide with a username
            if (db.get(userDbi, crMountName)) {
                return { status: 409, json: {ok: false, error: 'Mount name conflicts with an existing username'} };
            }
        }

        var crMountPoint = getUserMountDir(crUser.username, crMountName, crRootMount);
        var crExisting = db.get(rcloneDbi, crUser.username + '/' + crMountName);
        if (crExisting) {
            // Check if this is a stale FUSE mount and recover it
            if (_isStaleMount(crMountPoint)) {
                fprintf(stderr, "Stale mount detected at %s during create, recovering...\n", crMountPoint);
                _recoverStaleMount(crMountPoint);
            }
            // Check if this is a dead mount (not actually mounted, empty or missing dir)
            var crIsMounted = _isMounted(crMountPoint);
            var crDirEmpty = true;
            var crDirStat = stat(crMountPoint);
            if (crDirStat && crDirStat.isDirectory) {
                try {
                    var crDirContents = readdir(crMountPoint);
                    if (crDirContents && crDirContents.length > 0) crDirEmpty = false;
                } catch(e) {}
            }
            if (crIsMounted || !crDirEmpty) {
                return { status: 409, json: {ok: false, error: 'Mount name already exists and is active'} };
            }
            // Dead mount — clean it up
            rcloneUnmount(crUser.username, crMountName, crExisting.rootMount);
            var crOldConf = getUserRcloneConf(crUser.username);
            try {
                shell((RCLONE_PATH || 'rclone') + ' config delete --config ' + _shellEscape(crOldConf) + ' ' + _shellEscape(crMountName),
                      {timeout: 5000});
            } catch(e) {}
            db.del(rcloneDbi, crUser.username + '/' + crMountName);
            // Remove empty mount point
            if (crDirStat && crDirStat.isDirectory) {
                try { rmdir(crMountPoint); } catch(e) {}
            }
        } else {
            // No LMDB entry — check if directory exists and is non-empty
            var crMpStat = stat(crMountPoint);
            if (crMpStat && crMpStat.isDirectory) {
                try {
                    var crContents = readdir(crMountPoint);
                    if (crContents && crContents.length > 0) {
                        return { status: 409, json: {ok: false, error: 'Directory already exists and is not empty'} };
                    }
                } catch(e) {}
            }
        }

        var crConf = getUserRcloneConf(crUser.username);

        var configCmd = (RCLONE_PATH || 'rclone') + ' config create' +
            ' --config ' + _shellEscape(crConf) +
            ' ' + _shellEscape(crMountName) +
            ' ' + _shellEscape(crProviderType);

        if (crTier === 's3') {
            if (crParams.access_key_id) configCmd += ' access_key_id=' + _shellEscape(crParams.access_key_id);
            if (crParams.secret_access_key) configCmd += ' secret_access_key=' + _shellEscape(crParams.secret_access_key);
            if (crParams.endpoint) configCmd += ' endpoint=' + _shellEscape(crParams.endpoint);
            if (crParams.region) configCmd += ' region=' + _shellEscape(crParams.region);
            if (crProviderType === 's3' && crParams.provider) configCmd += ' provider=' + _shellEscape(crParams.provider);
        } else if (crTier === 'oauth') {
            if (crParams.token) {
                configCmd += ' token=' + _shellEscape(JSON.stringify(crParams.token));
            }
        } else if (crTier === 'sftp') {
            // Only non-sensitive params go in rclone.conf
            if (crParams.host) configCmd += ' host=' + _shellEscape(crParams.host);
            if (crParams.user) configCmd += ' user=' + _shellEscape(crParams.user);
            if (crParams.port && crParams.port !== '22') configCmd += ' port=' + _shellEscape(crParams.port);
            // Encrypt sensitive credentials and store in LMDB
            var sftpCreds = {};
            if (crParams.pass) sftpCreds.pass = crParams.pass;
            if (crParams.key_pem) {
                sftpCreds.key_pem = crParams.key_pem;
                if (crParams.key_file_pass) sftpCreds.key_file_pass = crParams.key_file_pass;
            }
            if (!crParams.loginPassword) {
                return { status: 400, json: {ok: false, error: 'Login password required for SFTP'} };
            }
            // Verify login password against user's hash
            var crUserRecord = db.get(userDbi, crUser.username);
            if (!crUserRecord || !crypto.passwdCheck(crUserRecord.hash_line, crParams.loginPassword)) {
                return { status: 403, json: {ok: false, error: 'Incorrect login password'} };
            }
            var encData = crypto.encrypt({pass: crParams.loginPassword, data: JSON.stringify(sftpCreds), cipher: 'aes-256-cbc'});
            var encB64 = sprintf("%B", encData);
            // Store encrypted creds — will be saved in LMDB record below
            var crEncryptedCreds = encB64;
        } else if (crTier === 'manual') {
            var crPkeys = Object.keys(crParams);
            for (var cpi = 0; cpi < crPkeys.length; cpi++) {
                configCmd += ' ' + _shellEscape(crPkeys[cpi]) + '=' + _shellEscape(crParams[crPkeys[cpi]]);
            }
        }

        if (crTier === 'oauth' && crParams.token) {
            // Write rclone.conf directly for OAuth — rclone config create
            // with token= is unreliable across rclone versions
            var crTokenJson = JSON.stringify(crParams.token);
            var crConfContent = '[' + crMountName + ']\ntype = ' + crProviderType + '\ntoken = ' + crTokenJson + '\n';
            var crFd = fopen(crConf, 'w');
            fwrite(crFd, crConfContent);
            fclose(crFd);
        } else {
            try {
                shell(configCmd, {timeout: 15000});
            } catch(e) {}
            // Verify the config was written
            var crConfStat = stat(crConf);
            if (!crConfStat || crConfStat.size === 0) {
                return { status: 500, json: {ok: false, error: 'Config creation failed: ' + crConf + ' missing or empty'} };
            }
        }


        var crRemotePath = crParams.remotePath || '';
        // For SFTP, pass credentials via CLI flags (not in rclone.conf)
        var crSftpEnv = '';
        if (crTier === 'sftp') {
            crSftpEnv = buildSftpMountEnv(sftpCreds);
        }
        var crMountResult = rcloneMountRemote(crUser.username, crMountName, crMountName, crRemotePath, '', crSftpEnv, crRootMount);

        var crDavPath = crRootMount
            ? '/' + crMountName + '/'
            : '/' + crUser.username + '/' + crMountName + '/';
        var crRecord = {
            name: crMountName,
            type: crProviderType,
            provider: crTier,
            params: {},
            remotePath: crRemotePath,
            active: crMountResult.ok,
            created: new Date().toISOString(),
            mountPath: crDavPath,
            readOnly: !!crBody.readOnly,
            rootMount: crRootMount
        };
        if (crTier === 'sftp') crRecord.encryptedCreds = crEncryptedCreds;
        db.put(rcloneDbi, crUser.username + '/' + crMountName, crRecord);
        // Secondary index for root mounts (used by PROPFIND to detect read-only)
        if (crRootMount) {
            db.put(rcloneDbi, '_rootmount/' + crMountName, { owner: crUser.username, readOnly: !!crBody.readOnly });
        }


        // Register mount point so WebDAV can serve it
        _addExternalPath(crMountPoint);

        return _attachCookie({
            status: crMountResult.ok ? 201 : 200,
            json: {
                ok: true,
                mounted: crMountResult.ok,
                mountError: crMountResult.error || null,
                name: crMountName,
                mountPath: crDavPath
            }
        }, crUser);
    }

    // POST /dav/_rclone/remove — unmount and remove a mount
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_rclone/remove') {
        var delRcUser = authenticate(req);
        if (!delRcUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };

        var delRcBody;
        try { delRcBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }

        var delMountName = (delRcBody.name || '').trim();
        var delKey = delRcUser.username + '/' + delMountName;
        var delCfg = db.get(rcloneDbi, delKey);
        if (!delCfg) return { status: 404, json: {ok: false, error: 'Mount not found'} };

        rcloneUnmount(delRcUser.username, delMountName, delCfg.rootMount);

        var delConf = getUserRcloneConf(delRcUser.username);
        try {
            shell((RCLONE_PATH || 'rclone') + ' config delete --config ' + _shellEscape(delConf) + ' ' + _shellEscape(delMountName),
                  {timeout: 5000});
        } catch(e) {}

        db.del(rcloneDbi, delKey);
        if (delCfg.rootMount) db.del(rcloneDbi, '_rootmount/' + delMountName);

        var delMountPoint = getUserMountDir(delRcUser.username, delMountName, delCfg.rootMount);
        _removeExternalPath(delMountPoint);

        // Clean up thumbnails for the removed mount
        var delDavRel = delRcUser.username + '/' + delMountName;
        rmdirThumbnails(thumbDir(delDavRel));
        deleteThumbnail(delDavRel);

        // Clean up dead props for the removed mount
        removeDeadPropsRecursive(delDavRel + '/');

        return _attachCookie({ status: 200, json: {ok: true} }, delRcUser);
    }

    // POST /dav/_rclone/remount — re-mount a configured mount
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_rclone/remount') {
        var reUser = authenticate(req);
        if (!reUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!HAS_RCLONE) return { status: 400, json: {ok: false, error: 'rclone not available'} };

        var reBody;
        try { reBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }

        var reMountName = (reBody.name || '').trim();
        var reKey = reUser.username + '/' + reMountName;
        var reCfg = db.get(rcloneDbi, reKey);
        if (!reCfg) return { status: 404, json: {ok: false, error: 'Mount not found'} };

        // SFTP mounts require login password to decrypt credentials
        var reSftpEnv = '';
        if (reCfg.provider === 'sftp' && reCfg.encryptedCreds) {
            if (!reBody.password) {
                return { status: 400, json: {ok: false, error: 'Login password required to mount SFTP', needsPassword: true} };
            }
            // Verify login password against user's hash
            var reUserRecord = db.get(userDbi, reUser.username);
            if (!reUserRecord || !crypto.passwdCheck(reUserRecord.hash_line, reBody.password)) {
                return { status: 403, json: {ok: false, error: 'Incorrect password', needsPassword: true} };
            }
            try {
                var reEncBuf = sprintf("%!B", reCfg.encryptedCreds);
                var reDecBuf = crypto.decrypt({pass: reBody.password, data: reEncBuf, cipher: 'aes-256-cbc'});
                var reCreds = JSON.parse(bufferToString(reDecBuf));
                reSftpEnv = buildSftpMountEnv(reCreds);
            } catch(e) {
                return { status: 403, json: {ok: false, error: 'Incorrect password', needsPassword: true} };
            }
        }

        rcloneUnmount(reUser.username, reMountName, reCfg.rootMount);
        var reResult = rcloneMountRemote(reUser.username, reMountName, reMountName, reCfg.remotePath || '', '', reSftpEnv, reCfg.rootMount);

        reCfg.active = reResult.ok;
        db.put(rcloneDbi, reKey, reCfg);

        return _attachCookie({
            status: 200,
            json: {ok: reResult.ok, error: reResult.error || null}
        }, reUser);
    }

    // POST /dav/_rclone/unmount — unmount without removing configuration
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_rclone/unmount') {
        var umUser = authenticate(req);
        if (!umUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!HAS_RCLONE) return { status: 400, json: {ok: false, error: 'rclone not available'} };

        var umBody;
        try { umBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }

        var umMountName = (umBody.name || '').trim();
        var umKey = umUser.username + '/' + umMountName;
        var umCfg = db.get(rcloneDbi, umKey);
        if (!umCfg) return { status: 404, json: {ok: false, error: 'Mount not found'} };

        var umResult = rcloneUnmount(umUser.username, umMountName, umCfg.rootMount);
        // Set inactive so it won't auto-remount on login
        umCfg.active = false;
        db.put(rcloneDbi, umKey, umCfg);

        return _attachCookie({
            status: 200,
            json: {ok: umResult.ok, error: umResult.error || null}
        }, umUser);
    }

    // POST /dav/_rclone/remountAll — re-mount all unmounted SFTP mounts for a user
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_rclone/remountAll') {
        var raUser = authenticate(req);
        if (!raUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!HAS_RCLONE) return { status: 400, json: {ok: false, error: 'rclone not available'} };

        var raBody;
        try { raBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }
        if (!raBody.password) {
            return { status: 400, json: {ok: false, error: 'Password required'} };
        }
        // Verify login password
        var raRecord = db.get(userDbi, raUser.username);
        if (!raRecord || !crypto.passwdCheck(raRecord.hash_line, raBody.password)) {
            return { status: 403, json: {ok: false, error: 'Incorrect password'} };
        }

        var raAll = db.get(rcloneDbi, "", 10000);
        var raResults = [];
        if (raAll && typeof raAll === 'object') {
            var raKeys = Object.keys(raAll);
            for (var rai = 0; rai < raKeys.length; rai++) {
                if (raKeys[rai].indexOf(raUser.username + '/') !== 0) continue;
                var raCfg = raAll[raKeys[rai]];
                if (!raCfg || raCfg.provider !== 'sftp' || !raCfg.encryptedCreds) continue;
                var raMountName = raKeys[rai].split('/')[1];
                // Skip if actually mounted
                var raMp = getUserMountDir(raUser.username, raMountName, raCfg.rootMount);
                if (_isMounted(raMp)) {
                    raResults.push({name: raMountName, ok: true, skipped: true});
                    continue;
                }
                try {
                    var raEncBuf = sprintf("%!B", raCfg.encryptedCreds);
                    var raDecBuf = crypto.decrypt({pass: raBody.password, data: raEncBuf, cipher: 'aes-256-cbc'});
                    var raCreds = JSON.parse(bufferToString(raDecBuf));
                    var raEnv = buildSftpMountEnv(raCreds);
                    rcloneUnmount(raUser.username, raMountName, raCfg.rootMount);
                    var raRes = rcloneMountRemote(raUser.username, raMountName, raMountName, raCfg.remotePath || '', '', raEnv, raCfg.rootMount);
                    raCfg.active = raRes.ok;
                    db.put(rcloneDbi, raKeys[rai], raCfg);
                    raResults.push({name: raMountName, ok: raRes.ok, error: raRes.error || null});
                } catch(e) {
                    raResults.push({name: raMountName, ok: false, error: 'Decryption failed'});
                }
            }
        }
        return _attachCookie({
            status: 200,
            json: {ok: true, results: raResults}
        }, raUser);
    }

    // POST /dav/_oauth/start — start rclone authorize for OAuth provider
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_oauth/start') {
        var oaUser = authenticate(req);
        if (!oaUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };
        if (!HAS_RCLONE) return { status: 400, json: {ok: false, error: 'rclone not available'} };

        var oaBody;
        try { oaBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }

        var oaProvider = (oaBody.provider || '').trim();
        if (!oaProvider || !RCLONE_PROVIDERS[oaProvider] || RCLONE_PROVIDERS[oaProvider].tier !== 'oauth') {
            return { status: 400, json: {ok: false, error: 'Invalid OAuth provider'} };
        }

        // Check if another user has an active OAuth session (port 53682 is shared)
        var oaAllSessions = db.get(rcloneDbi, "", 10000);
        if (oaAllSessions && typeof oaAllSessions === 'object') {
            var oaSessKeys = Object.keys(oaAllSessions);
            for (var oasi = 0; oasi < oaSessKeys.length; oasi++) {
                if (oaSessKeys[oasi].indexOf('_oauth_session/') !== 0) continue;
                var oaSessUser = oaSessKeys[oasi].substring('_oauth_session/'.length);
                if (oaSessUser === oaUser.username) continue;
                var oaOtherSession = oaAllSessions[oaSessKeys[oasi]];
                // Check if session is still active (less than 5 minutes old and process alive)
                if (oaOtherSession && Date.now() - oaOtherSession.created < 300000 &&
                    kill(parseInt(oaOtherSession.pid), 0)) {
                    return _attachCookie({ status: 409, json: {
                        ok: false,
                        error: 'Another user is currently authorizing cloud storage. Please try again in a few minutes.'
                    }}, oaUser);
                }
                // Stale session — clean it up
                kill(parseInt(oaOtherSession.pid));
                try { rmFile(oaOtherSession.stdoutFile); } catch(e) {}
                try { rmFile(oaOtherSession.stderrFile); } catch(e) {}
                db.del(rcloneDbi, oaSessKeys[oasi]);
            }
        }

        // Clean up any existing OAuth session for this user
        var oaOldSession = db.get(rcloneDbi, '_oauth_session/' + oaUser.username);
        if (oaOldSession && oaOldSession.pid) {
            kill(parseInt(oaOldSession.pid));
            try { rmFile(oaOldSession.stdoutFile); } catch(e) {} try { rmFile(oaOldSession.stderrFile); } catch(e) {}
        }

        // Generate session ID and temp file paths
        var oaSid = crypto.sha256(oaUser.username + '_' + Date.now() + '_' + hexify(crypto.rand(8))).substring(0, 16);
        var oaDir = getUserRcloneDir(oaUser.username);
        var oaStdout = oaDir + '/oauth_' + oaSid + '.stdout';
        var oaStderr = oaDir + '/oauth_' + oaSid + '.stderr';

        // Start rclone authorize in a thread (keeps process alive)
        var oaConf = getUserRcloneConf(oaUser.username);
        var oaNoOpen = (RCLONE_VERSION && RCLONE_VERSION >= '1.53') ? ' --auth-no-open-browser' : '';
        var _oaThr = new rampart.thread();
        var oaBgCmd = (RCLONE_PATH || 'rclone') + ' authorize ' + _shellEscape(oaProvider) +
            ' --config ' + _shellEscape(oaConf) +
            oaNoOpen +
            ' > ' + _shellEscape(oaStdout) +
            ' 2> ' + _shellEscape(oaStderr);
        _oaThr.exec(function(cfg) {
            var u = rampart.utils;
            if (cfg.log) u.fprintf(cfg.log, true, "thread running: %s\n", cfg.cmd);
            u.shell(cfg.cmd, {timeout: 300000});
        }, {
            cmd: oaBgCmd
        });
        // Give rclone a moment to start its listener
        rampart.utils.sleep(1);
        var oaPid = 'thread';

        // Wait for auth URL to appear in stdout or stderr (up to 10 seconds)
        var oaWaitCmd = 'for i in $(seq 1 20); do ' +
            'cat ' + _shellEscape(oaStderr) + ' ' + _shellEscape(oaStdout) + ' 2>/dev/null | ' +
            'grep -ohE "http://127\\.0\\.0\\.1:[0-9]+/auth\\?state=[^ ]*" && break; ' +
            'sleep 0.5; done';
        var oaAuthUrl = '';
        try {
            var oaWaitRes = shell(oaWaitCmd, {timeout: 15000});
            oaAuthUrl = oaWaitRes.stdout.trim();
        } catch(e) {}

        if (!oaAuthUrl) {
            kill(parseInt(oaPid));
            var oaErrContent = '';
            try { oaErrContent = bufferToString(readFile(oaStderr)); } catch(e) {}
            try { rmFile(oaStdout); } catch(e) {} try { rmFile(oaStderr); } catch(e) {}
            return { status: 500, json: {ok: false, error: 'rclone authorize failed: ' + (oaErrContent || 'unknown error').substring(0, 200)} };
        }

        // Fetch the rclone local auth URL server-side to get the actual
        // OAuth provider consent URL (e.g. accounts.google.com/...).
        // rclone's listener returns a 302 redirect to the provider.
        var oaProviderUrl = '';
        try {
            var oaRedirCmd = '/usr/bin/curl -s -o /dev/null -w "%{redirect_url}" ' + _shellEscape(oaAuthUrl);
            var oaRedirRes = shell(oaRedirCmd, {timeout: 10000});
            oaProviderUrl = oaRedirRes.stdout.trim();
        } catch(e) {}

        if (!oaProviderUrl) {
            try { rmFile(oaStdout); } catch(e) {} try { rmFile(oaStderr); } catch(e) {}
            return { status: 500, json: {ok: false, error: 'Failed to get OAuth provider URL from rclone'} };
        }

        // Store session info in LMDB
        db.put(rcloneDbi, '_oauth_session/' + oaUser.username, {
            sessionId: oaSid,
            provider: oaProvider,
            pid: oaPid,
            stdoutFile: oaStdout,
            stderrFile: oaStderr,
            created: Date.now()
        });

        return _attachCookie({
            status: 200,
            json: {ok: true, sessionId: oaSid, authUrl: oaProviderUrl}
        }, oaUser);
    }

    // POST /dav/_oauth/relay — relay OAuth callback URL to rclone's local listener
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_oauth/relay') {
        var rlUser = authenticate(req);
        if (!rlUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };

        var rlBody;
        try { rlBody = JSON.parse(bufferToString(req.body)); } catch(e) {
            return { status: 400, json: {ok: false, error: 'Invalid JSON'} };
        }

        var rlUrl = (rlBody.url || '').trim();
        // Security: only relay to rclone's local listener
        if (!/^http:\/\/(127\.0\.0\.1|localhost):53682\//.test(rlUrl)) {
            return { status: 400, json: {ok: false, error: 'Invalid relay URL — must be http://127.0.0.1:53682/...'} };
        }

        // Relay the callback to rclone's local listener
        var rlResp = curl.fetch(rlUrl, {returnText: true, "max-time": 10, location: true});
        if (rlResp.status !== 200) {
            return _attachCookie({ status: 502, json: {ok: false, error: 'Relay failed: ' + (rlResp.statusText || rlResp.status) + ' ' + (rlResp.errMsg || '')} }, rlUser);
        }

        // Wait for rclone to finish writing the token
        var rlToken = null;
        var rlConf = getUserRcloneConf(rlUser.username);

        // First check: token might already be there
        try {
            var rlConfContent = bufferToString(readFile(rlConf));
            var rlTokens = rlConfContent.match(/token\s*=\s*(\{[^\n]+\})/g);
            if (rlTokens && rlTokens.length > 0) {
                var rlLastToken = rlTokens[rlTokens.length - 1].replace(/^token\s*=\s*/, '');
                try { rlToken = JSON.parse(rlLastToken); } catch(e) {}
            }
        } catch(e) {}

        // If not, wait and retry
        if (!rlToken) {
            for (var rlRetry = 0; rlRetry < 10; rlRetry++) {
                rampart.utils.sleep(1);
                try {
                    var rlConfContent2 = bufferToString(readFile(rlConf));
                    var rlTokens2 = rlConfContent2.match(/token\s*=\s*(\{[^\n]+\})/g);
                    if (rlTokens2 && rlTokens2.length > 0) {
                        var rlLastToken2 = rlTokens2[rlTokens2.length - 1].replace(/^token\s*=\s*/, '');
                        try { rlToken = JSON.parse(rlLastToken2); } catch(e) {}
                        if (rlToken) break;
                    }
                } catch(e) {}
            }
        }

        if (rlToken) {
            // Clean up oauth session
            try {
                var rlSession = db.get(rcloneDbi, '_oauth_session/' + rlUser.username);
                if (rlSession) {
                    try { rmFile(rlSession.stdoutFile); } catch(e) {} try { rmFile(rlSession.stderrFile); } catch(e) {}
                    db.del(rcloneDbi, '_oauth_session/' + rlUser.username);
                }
            } catch(e) {}
            return _attachCookie({ status: 200, json: {ok: true, token: rlToken} }, rlUser);
        }

        return _attachCookie({ status: 200, json: {ok: true} }, rlUser);
    }

    // GET /dav/_oauth/poll — check if rclone authorize has produced a token
    if (method === 'GET' && fullPath === DAV_PREFIX + '/_oauth/poll') {
        var plUser = authenticate(req);
        if (!plUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };

        var plSession = db.get(rcloneDbi, '_oauth_session/' + plUser.username);
        if (!plSession) {
            return _attachCookie({ status: 200, json: {ok: false, error: 'No active OAuth session'} }, plUser);
        }

        // Check if session expired (10 min max)
        if (Date.now() - plSession.created > 600000) {
            kill(parseInt(plSession.pid));
            try { rmFile(plSession.stdoutFile); } catch(e) {} try { rmFile(plSession.stderrFile); } catch(e) {}
            db.del(rcloneDbi, '_oauth_session/' + plUser.username);
            return _attachCookie({ status: 200, json: {ok: false, error: 'OAuth session expired'} }, plUser);
        }

        // Read stdout file for token (method 1: rclone authorize output)
        var plContent = '';
        try { plContent = bufferToString(readFile(plSession.stdoutFile)); } catch(e) {}

        var plToken = null;
        var plTokenMatch = plContent.match(/Paste the following into your remote machine --->\s*([\s\S]*?)\s*<---End paste/);
        if (plTokenMatch) {
            try { plToken = JSON.parse(plTokenMatch[1].trim()); } catch(e) {}
        }

        // Fallback: read token from rclone.conf (method 2: rclone writes directly to config)
        if (!plToken) {
            try {
                var plConf = getUserRcloneConf(plUser.username);
                var plConfContent = bufferToString(readFile(plConf));
                // Find the most recently added section's token
                var plConfTokens = plConfContent.match(/token\s*=\s*(\{[^\n]+\})/g);
                if (plConfTokens && plConfTokens.length > 0) {
                    var plLastToken = plConfTokens[plConfTokens.length - 1].replace(/^token\s*=\s*/, '');
                    try { plToken = JSON.parse(plLastToken); } catch(e) {}
                }
            } catch(e) {}
        }

        if (plToken) {
            // Clean up
            try { rmFile(plSession.stdoutFile); } catch(e) {} try { rmFile(plSession.stderrFile); } catch(e) {}
            db.del(rcloneDbi, '_oauth_session/' + plUser.username);
            return _attachCookie({ status: 200, json: {ok: true, token: plToken} }, plUser);
        }

        // Token not yet available
        return _attachCookie({ status: 200, json: {ok: false, pending: true} }, plUser);
    }

    // POST /dav/_oauth/cancel — kill rclone authorize and clean up session
    if (method === 'POST' && fullPath === DAV_PREFIX + '/_oauth/cancel') {
        var caUser = authenticate(req);
        if (!caUser) return { status: 401, json: {ok: false, error: 'Not authenticated'} };

        var caSession = db.get(rcloneDbi, '_oauth_session/' + caUser.username);
        if (caSession) {
            kill(parseInt(caSession.pid));
            try { rmFile(caSession.stdoutFile); } catch(e) {}
            try { rmFile(caSession.stderrFile); } catch(e) {}
            db.del(rcloneDbi, '_oauth_session/' + caUser.username);
        }
        return _attachCookie({ status: 200, json: {ok: true} }, caUser);
    }

    // Public share access: GET /dav/_s/<token>[/subpath...]
    var sharePrefix = '/_s/';
    if (method === 'GET' && fullPath.indexOf(DAV_PREFIX + sharePrefix) === 0) {
        var shareRest = fullPath.substring((DAV_PREFIX + sharePrefix).length);
        var slashIdx = shareRest.indexOf('/');
        var shareToken = slashIdx === -1 ? shareRest : shareRest.substring(0, slashIdx);
        var shareSubPath = slashIdx === -1 ? '' : shareRest.substring(slashIdx);
        if (!shareToken) return { status: 400, txt: 'Bad Request' };
        var shareRec = db.get(sharesDbi, shareToken);
        if (!shareRec) return { status: 404, txt: 'Share not found or expired' };
        // Check expiration
        if (shareRec.expires && new Date(shareRec.expires) < new Date()) {
            db.del(sharesDbi, shareToken);
            invalidateSharedPathsCache();
            return { status: 410, txt: 'This share link has expired' };
        }
        var shareRelPath = shareRec.path;
        if (shareSubPath) {
            // Append subpath for directory browsing
            shareRelPath = shareRelPath.replace(/\/$/, '') + decodeURIComponent(shareSubPath);
        }
        var shareFsPath = buildFsPath(shareRelPath);
        if (!shareFsPath) return { status: 400, txt: 'Bad Request' };
        if (!checkAllowedPath(shareFsPath)) return { status: 403, txt: 'Forbidden' };
        var shareSt = stat(shareFsPath);
        if (!shareSt) return { status: 404, txt: 'Not Found' };
        var videoExts = {mp4:1, webm:1, ogg:1, ogv:1, mkv:1, avi:1, mov:1, m4v:1};

        if (shareSt.isDirectory) {
            // Serve a simple HTML directory listing
            if (!shareRec.isDir && shareSubPath) {
                return { status: 403, txt: 'This share link is for a single file' };
            }
            var dirEntries = readdir(shareFsPath);
            var dirItems = [];
            for (var di = 0; di < dirEntries.length; di++) {
                var deName = dirEntries[di];
                if (deName === '.' || deName === '..' || deName === '.Trash') continue;
                var deSt = stat(shareFsPath + '/' + deName);
                if (!deSt) continue;
                dirItems.push({
                    name: deName,
                    isDir: deSt.isDirectory,
                    size: deSt.size || 0,
                    mtime: deSt.mtime ? new Date(deSt.mtime).toISOString() : ''
                });
            }
            // Sort: directories first, then by name
            dirItems.sort(function(a, b) {
                if (a.isDir !== b.isDir) return a.isDir ? -1 : 1;
                return a.name.toLowerCase().localeCompare(b.name.toLowerCase());
            });
            var shareBaseUrl = DAV_PREFIX + sharePrefix + shareToken;
            var currentSub = shareSubPath || '/';
            var shareHtml = '<!DOCTYPE html><html><head><meta charset="utf-8">' +
                '<meta name="viewport" content="width=device-width,initial-scale=1">' +
                '<title>Shared: ' + xmlEscape(shareRec.path.split('/').filter(Boolean).pop() || '/') + '</title>' +
                '<style>' +
                'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;margin:0;padding:20px;background:#f8f9fa;color:#1f2328}' +
                '.container{max-width:800px;margin:0 auto;background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,0.1);overflow:hidden}' +
                '.header{padding:16px 20px;border-bottom:1px solid #e1e4e8;background:#f6f8fa}' +
                '.header h1{margin:0;font-size:1.1rem;font-weight:600}' +
                '.header .meta{font-size:0.8rem;color:#656d76;margin-top:4px}' +
                '.breadcrumb{font-size:0.85rem;padding:8px 20px;border-bottom:1px solid #e1e4e8;background:#fafbfc}' +
                '.breadcrumb a{color:#0969da;text-decoration:none}' +
                '.breadcrumb a:hover{text-decoration:underline}' +
                'table{width:100%;border-collapse:collapse}' +
                'th{text-align:left;padding:8px 20px;font-size:0.75rem;color:#656d76;border-bottom:1px solid #e1e4e8;background:#fafbfc}' +
                'td{padding:8px 20px;border-bottom:1px solid #f0f0f0;font-size:0.85rem}' +
                'tr:hover{background:#f6f8fa}' +
                'a{color:#0969da;text-decoration:none}a:hover{text-decoration:underline}' +
                '.icon{margin-right:6px;opacity:0.6}' +
                '.size{color:#656d76;text-align:right}.date{color:#656d76}' +
                '</style></head><body><div class="container">';
            shareHtml += '<div class="header"><h1>' + xmlEscape(shareRec.path.split('/').filter(Boolean).pop() || 'Shared Folder') + '</h1>';
            shareHtml += '<div class="meta">Shared by ' + xmlEscape(shareRec.owner);
            if (shareRec.expires) {
                shareHtml += ' &middot; Expires ' + xmlEscape(new Date(shareRec.expires).toLocaleString());
            }
            shareHtml += '</div></div>';
            // Breadcrumb for subdirectories
            if (shareSubPath && shareSubPath !== '/') {
                var bcParts = shareSubPath.split('/').filter(Boolean);
                shareHtml += '<div class="breadcrumb"><a href="' + shareBaseUrl + '/">Root</a>';
                var bcPath = '';
                for (var bi = 0; bi < bcParts.length; bi++) {
                    bcPath += '/' + bcParts[bi];
                    shareHtml += ' / <a href="' + shareBaseUrl + bcPath + '/">' + xmlEscape(decodeURIComponent(bcParts[bi])) + '</a>';
                }
                shareHtml += '</div>';
            }
            shareHtml += '<table><thead><tr><th>Name</th><th>Size</th><th>Modified</th></tr></thead><tbody>';
            // Parent directory link
            if (shareSubPath && shareSubPath !== '/' && shareSubPath !== '') {
                var parentSub = shareSubPath.replace(/\/[^\/]*\/?$/, '/');
                if (parentSub === '/') parentSub = '/';
                shareHtml += '<tr><td><a href="' + shareBaseUrl + parentSub + '">..</a></td><td></td><td></td></tr>';
            }
            for (var dj = 0; dj < dirItems.length; dj++) {
                var de = dirItems[dj];
                var deNameEnc = encodeURIComponent(de.name);
                var deExt = de.name.split('.').pop().toLowerCase();
                var deIsVideo = !de.isDir && videoExts[deExt];
                var deUrl = shareBaseUrl + (currentSub === '/' ? '/' : currentSub.replace(/\/?$/, '/')) + deNameEnc + (de.isDir ? '/' : '') + (deIsVideo ? '?player=1' : '');
                var deSize = de.isDir ? '' : _formatSize(de.size);
                var deDate = de.mtime ? de.mtime.substring(0, 10) : '';
                shareHtml += '<tr><td><span class="icon">' + (de.isDir ? '&#128193;' : '&#128196;') + '</span>';
                shareHtml += '<a href="' + deUrl + '">' + xmlEscape(de.name) + '</a></td>';
                shareHtml += '<td class="size">' + deSize + '</td>';
                shareHtml += '<td class="date">' + deDate + '</td></tr>';
            }
            shareHtml += '</tbody></table></div></body></html>';
            return { status: 200, html: shareHtml };
        } else {
            // Check if this is a video file and serve a player page
            var shareExt = shareFsPath.split('.').pop().toLowerCase();
            if (videoExts[shareExt] && req.query && req.query.player) {
                var rawUrl = fullPath;
                var videoMime = {mp4:'video/mp4',webm:'video/webm',ogg:'video/ogg',ogv:'video/ogg',mkv:'video/x-matroska',avi:'video/x-msvideo',mov:'video/quicktime',m4v:'video/mp4'};
                var vMime = videoMime[shareExt] || 'video/' + shareExt;
                var vName = decodeURIComponent(shareFsPath.split('/').pop());
                var playerHtml = '<!DOCTYPE html><html><head><meta charset="utf-8">' +
                    '<meta name="viewport" content="width=device-width,initial-scale=1">' +
                    '<title>' + xmlEscape(vName) + '</title>' +
                    '<link href="https://cdn.jsdelivr.net/npm/video.js@8/dist/video-js.min.css" rel="stylesheet">' +
                    '<style>' +
                    'html,body{margin:0;padding:0;width:100%;height:100%;background:#000;overflow:hidden}' +
                    '.video-js{width:100%;height:100%}' +
                    '</style></head><body>' +
                    '<video id="player" class="video-js vjs-big-play-centered" controls preload="auto">' +
                    '<source src="' + xmlEscape(rawUrl) + '" type="' + vMime + '">' +
                    '</video>' +
                    '<script src="https://cdn.jsdelivr.net/npm/video.js@8/dist/video.min.js"><\/script>' +
                    '<script>videojs("player",{fill:true})<\/script>' +
                    '</body></html>';
                return { status: 200, html: playerHtml };
            }
            // Serve the file directly
            return handleGET(req, shareRelPath, shareFsPath);
        }
    }

    // Thumbnail endpoint: GET /dav/_thumb/path/to/file.jpg
    var thumbPrefix = '/_thumb';
    if (method === 'GET' && fullPath.indexOf(DAV_PREFIX + thumbPrefix) === 0) {
        var thumbRelPath = fullPath.substring((DAV_PREFIX + thumbPrefix).length) || '/';
        var thumbFsPath = buildFsPath(thumbRelPath);
        if (!thumbFsPath || !checkAllowedPath(thumbFsPath)) {
            return { status: 403, txt: 'Forbidden' };
        }
        var thumbUser = authenticate(req);
        if (!thumbUser) {
            return { status: 401, headers: make401Headers(req), txt: 'Authentication required' };
        }
        if (!authorize(thumbUser, thumbRelPath, 'GET')) {
            return { status: 403, txt: 'Forbidden' };
        }
        // Check file-level read permission
        var thumbPermDenied = checkAccess(thumbUser, thumbRelPath, thumbFsPath, 'GET');
        if (thumbPermDenied) return { status: thumbPermDenied.status, txt: thumbPermDenied.msg };
        return handleThumb(req, thumbRelPath, thumbFsPath);
    }

    var davRelPath = getDavRelPath(fullPath);

    // Build filesystem path with traversal protection
    var fsPath = buildFsPath(davRelPath);
    if (!fsPath) {
        return { status: 400, txt: 'Bad Request' };
    }

    // Symlink traversal check: reject paths that resolve outside allowed directories
    if (!checkAllowedPath(fsPath)) {
        return { status: 403, txt: 'Forbidden' };
    }

    // Authentication & authorization (OPTIONS exempt for client discovery)
    if (method !== 'OPTIONS') {
        var davUser = authenticate(req);
        if (!davUser) {
            return {
                status: 401,
                headers: make401Headers(req),
                txt: 'Authentication required'
            };
        }
        req.davUser = davUser;

        if (!authorize(davUser, davRelPath, method)) {
            return { status: 403, txt: 'Forbidden' };
        }

        // Check file-level permissions (owner/group/other)
        var permDenied = checkAccess(davUser, davRelPath, fsPath, method);
        if (permDenied) {
            return { status: permDenied.status, txt: permDenied.msg };
        }

        ensureUserHome(davUser.username);
    }

    // Auto-create metadata for existing files that lack an entry
    ensureFileMeta(davRelPath, fsPath);

    // Demo mode: block write operations on protected paths
    if (DEMO_MODE && demoIsProtectedPath(davRelPath)) {
        var writeMethods = ['PUT', 'DELETE', 'MKCOL', 'MOVE', 'PROPPATCH'];
        if (writeMethods.indexOf(method) !== -1) {
            return { status: 403, txt: 'Demo: this directory is read-only' };
        }
    }

    var resp;
    switch (method) {
        case 'OPTIONS':   resp = handleOPTIONS(req, davRelPath, fsPath); break;
        case 'GET':       resp = handleGET(req, davRelPath, fsPath); break;
        case 'HEAD':      resp = handleHEAD(req, davRelPath, fsPath); break;
        case 'PUT':       resp = handlePUT(req, davRelPath, fsPath); break;
        case 'DELETE':    resp = handleDELETE(req, davRelPath, fsPath); break;
        case 'MKCOL':     resp = handleMKCOL(req, davRelPath, fsPath); break;
        case 'COPY':      resp = handleCOPY(req, davRelPath, fsPath); break;
        case 'MOVE':      resp = handleMOVE(req, davRelPath, fsPath); break;
        case 'PROPFIND':  resp = handlePROPFIND(req, davRelPath, fsPath); break;
        case 'PROPPATCH': resp = handlePROPPATCH(req, davRelPath, fsPath); break;
        case 'LOCK':      resp = handleLOCK(req, davRelPath, fsPath); break;
        case 'UNLOCK':    resp = handleUNLOCK(req, davRelPath, fsPath); break;
        default:
            return {
                status: 405,
                headers: { 'Allow': SUPPORTED_METHODS },
                txt: 'Method Not Allowed'
            };
    }
    return _attachCookie(resp, req.davUser);
};

function admin() {
    var args = process.argv.slice(2);

    function readLine_() {
        var val = readLine(stdin).next();
        if (val === undefined || val === null) {
            printf("\n");
            process.exit(1);
        }
        return trim(val);
    }

    function readPassword(prompt) {
        printf('%s', prompt);
        stdout.fflush();
        try { shell('stty -echo'); } catch(e) {}
        var pw = readLine_();
        try { shell('stty echo'); } catch(e) {}
        printf('\n');
        return pw;
    }

    function usage() {
        printf("WebDAV Administration Tool\n\n");
        printf("Usage: rampart %s <command> [args]\n\n", process.argv[1]);
        printf("Commands:\n");
        printf("  add    <username> <password>         Create a new user\n");
        printf("  del    <username>                    Delete a user (preserves home dir)\n");
        printf("  list                                 List all users\n");
        printf("  passwd <username> <password>          Change password\n");
        printf("  admin  <username> [true|false]        Toggle or set admin status\n");
        process.exit(1);
    }

    function addUser(username, password) {
        if (!username || !password) {
            printf("Error: username and password required\n");
            process.exit(1);
        }
        username = username.trim().toLowerCase();
        if (!/^[a-zA-Z0-9_-]{1,32}$/.test(username)) {
            printf("Error: username must be 1-32 chars: letters, digits, underscore, hyphen\n");
            process.exit(1);
        }
        if (password.length < 4) {
            printf("Error: password must be at least 4 characters\n");
            process.exit(1);
        }
        var existing = db.get(userDbi, username);
        if (existing) {
            printf("Error: user '%s' already exists\n", username);
            process.exit(1);
        }
        var result = crypto.passwd(password, null, "sha512");
        db.put(userDbi, username, {
            hash_line: result.line,
            admin: false,
            created: new Date().toISOString(),
            groups: []
        });
        ensureUserHome(username);
        printf("User '%s' created.\n", username);
    }

    function delUser(username) {
        if (!username) {
            printf("Error: username required\n");
            process.exit(1);
        }
        var existing = db.get(userDbi, username);
        if (!existing) {
            printf("Error: user '%s' does not exist\n", username);
            process.exit(1);
        }
        db.del(userDbi, username);
        printf("User '%s' deleted. Home directory preserved at %s/webdav_root/%s/\n", username, dataRoot, username);
    }

    function listUsers() {
        var all = db.get(userDbi, "", 10000);
        if (!all || typeof all !== 'object') {
            printf("No users found.\n");
            return;
        }
        var keys = Object.keys(all);
        if (keys.length === 0) {
            printf("No users found.\n");
            return;
        }
        printf("%-20s %-6s %-24s\n", "USERNAME", "ADMIN", "CREATED");
        printf("%-20s %-6s %-24s\n", "--------", "-----", "-------");
        for (var i = 0; i < keys.length; i++) {
            var u = all[keys[i]];
            printf("%-20s %-6s %-24s\n", keys[i], u.admin ? 'yes' : 'no', u.created || 'unknown');
        }
        printf("\nTotal: %d user(s)\n", keys.length);
    }

    function changePassword(username, password) {
        if (!username || !password) {
            printf("Error: username and password required\n");
            process.exit(1);
        }
        var existing = db.get(userDbi, username);
        if (!existing) {
            printf("Error: user '%s' does not exist\n", username);
            process.exit(1);
        }
        if (password.length < 4) {
            printf("Error: password must be at least 4 characters\n");
            process.exit(1);
        }
        var result = crypto.passwd(password, null, "sha512");
        existing.hash_line = result.line;
        db.put(userDbi, username, existing);
        printf("Password changed for '%s'\n", username);
    }

    function setAdmin(username, flag) {
        if (!username) {
            printf("Error: username required\n");
            process.exit(1);
        }
        var existing = db.get(userDbi, username);
        if (!existing) {
            printf("Error: user '%s' does not exist\n", username);
            process.exit(1);
        }
        if (flag === undefined || flag === null) {
            existing.admin = !existing.admin;
        } else {
            existing.admin = (flag === 'true');
        }
        db.put(userDbi, username, existing);
        printf("User '%s' admin status: %s\n", username, existing.admin ? 'yes' : 'no');
    }

    // Check if any users exist — if not, run initial setup
    var allUsers = db.get(userDbi, "", 10000);
    var hasUsers = allUsers && typeof allUsers === 'object' && Object.keys(allUsers).length > 0;

    if (!hasUsers && (!args[0] || args[0] !== 'add')) {
        printf("\n");
        printf("===========================================\n");
        printf("   WebDAV Server — Initial Setup\n");
        printf("===========================================\n");
        printf("\nNo users exist yet. Let's create an administrator account.\n\n");

        var username;
        while (true) {
            printf("Admin username: ");
            stdout.fflush();
            username = readLine_().toLowerCase();
            if (!username) {
                printf("Username cannot be empty.\n");
                continue;
            }
            if (!/^[a-zA-Z0-9_-]{1,32}$/.test(username)) {
                printf("Username must be 1-32 chars: letters, digits, underscore, hyphen.\n");
                continue;
            }
            break;
        }

        var password;
        while (true) {
            password = readPassword("Password: ");
            if (password.length < 4) {
                printf("Password must be at least 4 characters.\n");
                continue;
            }
            var confirm = readPassword("Confirm password: ");
            if (password !== confirm) {
                printf("Passwords do not match. Try again.\n");
                continue;
            }
            break;
        }

        var result = crypto.passwd(password, null, "sha512");
        db.put(userDbi, username, {
            hash_line: result.line,
            admin: true,
            created: new Date().toISOString(),
            groups: []
        });
        ensureUserHome(username);

        // Create default "everyone" group
        if (!db.get(groupDbi, 'everyone')) {
            db.put(groupDbi, 'everyone', { name: 'everyone', created: new Date().toISOString() });
        }

        // Create shared directory with 777 permissions, group "everyone"
        var sharedPath = DAV_ROOT + '/shared';
        if (!stat(sharedPath)) mkdir(sharedPath);
        var sharedRel = '/shared/';
        if (!getFileMeta(sharedRel)) {
            var sharedMeta = {
                path: sharedRel,
                owner: username,
                group: 'everyone',
                permissions: 777,
                isDir: true,
                created: new Date().toISOString()
            };
            setFileMeta(sharedRel, sharedMeta);
        }

        printf("\nAdmin user '%s' created successfully.\n", username);
        printf("You can now start the web server and log in.\n\n");
        return;
    }

    // Normal CLI command dispatch
    var command = args[0];
    switch (command) {
        case 'add':    addUser(args[1], args[2]); break;
        case 'del':    delUser(args[1]); break;
        case 'list':   listUsers(); break;
        case 'passwd': changePassword(args[1], args[2]); break;
        case 'admin':  setAdmin(args[1], args[2]); break;
        default:       usage();
    }
}

// Command line, or module?
if(module && module.exports) {
    module.exports = main_dispatch;
} else {
    admin();
}