#!/usr/bin/env rampart

/* ************************************************************** *
 * ****** RAMPART FILE MANAGER WEB APP WITH WEBDAV BACKEND  ***** *
 * ************************************************************** */

/*
the server can be started by running:
  rampart web_server_config.js
         or
  rampart web_server_config.js start

Help:
  ./web_server_conf.js help
  usage:
    rampart web_server_conf.js [start|stop|restart|letssetup|status|dump|help]
        start     -- start the http(s) server
        stop      -- stop the http(s) server
        restart   -- stop and restart the http(s) server
        letssetup -- start http only to allow letsencrypt verification
        status    -- show status of server processes
        dump      -- dump the config object used for server.start()
        help      -- show this message
*/

// the filemanager requires rampart 0.6.2 or greater
if(rampart.versionNumber<602) {
    rampart.utils.fprintf(rampart.utils.stderr,
      "Rampart File Manager requires rampart >= 0.6.2\nThis version is %s\n",
       rampart.version);
    process.exit(1);
}


// For running in demo mode:
var demoMode =        false;
var demoClearTime =   30;             //ten minutes
var demoMaxFileSize = 50*1024*1024;   //50MB per file
var demoMaxQuota =    500*1024*1024;  //500MB total

//set working directory to the location of this script
var working_directory = process.scriptPath;

/* ****************************************************** *
 *  UNCOMMENT AND CHANGE DEFAULTS BELOW TO CONFIG SERVER  *
 * ****************************************************** */

var serverConf = {
    // Settings for file manager:

    // required  for onlyoffice
    bindAll: true,
    user: "myaccountname",  // nobody may make rclone fail.  Make an account and put that name here

    // dav endpoint
    appendMap: {
        "/dav/": {module: working_directory + '/apps/webdav/webdav.js'}
        // ONLYOFFICE proxy routes are added dynamically below
    },

    // normally we bind to 127.0.0.1:8088, root not required.
    // you can uncomment and edit one the sections below to change that

    // as http on port 80.
    /*
    bindAll:  true,
    port:     80,  // requires start as root
    user:     'nobody',
    */

    // as https using letsencrypt:
    // if you don't have a cert yet, run with rampart web_server_conf letssetup

    /*
    secure:      true,
    letsencrypt: "example.com",
    port:        443,  // requires start as root
    user:        'nobody',
    redir:       true, //necessary for letsencrypt certbot to verify
    */
    
    // as https with self signed certificate:
    /*
    secure:   true,
    selfSign: true,
    port:     443,  // requires start as root
    user:     'nobody',
    */

    // as https with your own certificates
    /*
    secure:      true,
    sslKeyFile:  '/path/to/my/server-key.pem',
    sslCertFile: '/path/to/my/server-cert.pem',
    port:        443,  // requires start as root
    user:        'nobody',
    */

    // Below are the default settings for the rampart web server:

    /* ipAddr              String. The ipv4 address to bind   */
    //ipAddr:              '127.0.0.1',

    /* ipv6Addr            String. The ipv6 address to bind   */
    //ipv6Addr:            '[::1]',

    /* bindAll             Bool.   Set ipAddr and ipv6Addr to 0.0.0.0 and [::] respectively   */
    //bindAll:             false,

    /* ipPort              Number. Set ipv4 port   */
    //ipPort:              8088,

    /* ipv6Port            Number. Set ipv6 port   */
    //ipv6Port:            8088,

    /* port                Number. Set both ipv4 and ipv6 port if > -1   */
    //port:                -1,

    /* htmlRoot            String. Root directory from which to serve files   */
    //htmlRoot:            working_directory + '/html',

    /* appsRoot            String. Root directory from which to serve apps   */
    //appsRoot:            working_directory + '/apps',

    /* wsappsRoot          String. Root directory from which to serve websocket apps   */
    //wsappsRoot:          working_directory + '/wsapps',

    /* dataRoot            String. Setting for user scripts   */
    //dataRoot:            working_directory + '/data',

    /* logRoot             String. Log directory   */
    //logRoot:             working_directory + '/logs',

    /* irohProxy           Bool.  Whether to start the irohProxy server to proxy http to iroh-webproxy client */
    //irohProxy:           false,

    /* redirPort           Number. Launch http->https redirect server and set port if < -1  */
    //redirPort:           -1,

    /* redir               Bool.   Launch http->https redirect server and set to port 80   */
    //redir:               false,

    /* redirTemp           Bool. If true, and if redir is true or redirPort is set, send a
                                 302 Moved Temporarily instead of a 301 Moved Permanently   */
    //redirTemp            false,

    /* accessLog           String. Log file name or null for stdout  */
    //accessLog:           working_directory + '/logs/access.log',

    /* errorLog            String. error log file name or null for stderr*/
    //errorLog:            working_directory + '/logs/error.log',

    /* log                 Bool.   Whether to log requests and errors   */
    //log:                 true,

    /* rotateLogs          Bool.   Whether to rotate the logs   */
    //rotateLogs:          false,

    /* rotateStart         String. Time to start log rotations   */
    //rotateStart:         '00:00',

    /* rotateInterval      Number. Interval between log rotations in seconds or
                           String. One of "hourly", "daily" or "weekly"        */
    //rotateInterval:      86400,

    /* user                String. If started as root, switch to this user
                                   It is necessary to start as root if using ports < 1024   */
    //user:                'nobody',

    /* threads             Number. Limit the number of threads used by the server.
                                   Default (-1) is the number of cores on the system   */
    //threads:             -1,

    /* secure              Bool.   Whether to use https.  If true sslKeyFile and sslCertFile must be set   */
    //secure:              false,

    /* sslKeyFile          String. If https, the ssl/tls key file location   */
    //sslKeyFile:          '',

    /* sslCertFile         String. If https, the ssl/tls cert file location   */
    //sslCertFile:         '',

    /* selfSign            Bool.   Whether to generate and use a self signed certificate
                                   If set, secure must be true and sslKeyFile/sslCertFile/letsencrypt must be unset.
    //selfSign             false,

    /* developerMode       Bool.   Whether JavaScript errors result in 500 and return a stack trace.
                                   Otherwise errors return 404 Not Found                             */
    //developerMode:       true,

    /* letsencrypt         String. If using letsencrypt, the 'domain.tld' name for automatic setup of https
                                   ( sets secure true and looks for '/etc/letsencrypt/live/domain.tld/' directory
                                     to set sslKeyFile and sslCertFile ).
                                   ( also sets "port" to 443 ).                                                      */
    //letsencrypt:         "",     //empty string - don't configure using letsencrypt

    /* rootScripts         Bool.   Whether to treat *.js files in htmlRoot as apps
                                   (not secure; don't use on a public facing server)      */
    //rootScripts:         false,

    /* directoryFunc       Bool.   Whether to provide a directory listing if no index.html is found   */
    //directoryFunc:       false,

    /* daemon              Bool.   whether to detach from terminal and run as a daemon  */
    //daemon:              true,

    /* monitor':           Bool.   whether to launch monitor process to auto restart server if
                                   killed or unrecoverable error */
    //monitor:             false,

    /* scriptTimeout       Number. Max time to wait for a script module to return a reply in
                           seconds (default 20). Script callbacks normally should be crafted
                           to return in a reasonable period of time.  Timeout and reconstruction
                           of environment is expensive, so this should be a last resort fallback.   */
    //scriptTimeout:       20,

    /* connectTimeout      Number. Max time to wait for client send request in seconds (default 20)   */
    //connectTimeout:      20,

    /* quickserver         Bool.   whether to load the alternate quickserver setting which serves
                                   files from serverRoot only and no apps or wsapps unless
                                   explicity set                                                    */
    //quickserver:         false,

    /* serverRoot          String.  base path for logs, htmlRoot, appsRoot and wsappsRoot.
    //serverRoot:          rampart.utils.realPath('.'),  Note: here ere serverRoot is defined below

    /* map                 Object.  Define filesystem and script mappings, set from htmlRoot,
                           appsRoot and wsappsRoot above.                                         */
    /*map:                 {
                               "/":                working_directory + '/html',
                               "/apps/":           {modulePath: working_directory + '/apps'},
                               "ws://wsapps/":     {modulePath: working_directory + '/wsapps'}
                           }
                           // note: if this is changed, serverConf.htmlRoot defaults et al will not be used or correct.
    */

    /* appendMap           Object.  Append the default map above with more mappings
                           e.g - {"/images": working_directory + '/images'}
                           or  - {"myfunc.html" : function(req) { ...} }
                           or  - {
                                     "/images": working_directory + '/images',
                                     myfunc.html: {module: working_directory + '/myfuncmod.js'}
                                 }                                                                 */
    //appendMap:           undefined,

    /* appendProcTitle     Bool.  Whether to append ip:port to process name as seen in ps */
    //appendProcTitle:     false,

    /* beginFunc           Bool/Obj/Function.  A function to run at the beginning of each JavaScript
                           function or on file load
                           e.g. -
       beginFunc:          {module: working_directory+'/apps/beginfunc.js'}, //where beginfunc.js is "modules.exports=function(req) {...}"
       or
       beginFunc:          myglobalbeginfunc,
       or
       beginFunc:          function(req) { ... }
       or
       beginFunc:          undefined|false|null  // begin function disabled

                           The function, like all server callback function takes
                           req, which if altered will be reflected in the call
                           of the normal callback for the requested page.
                           Returning false will skip the normal callback and
                           send a 404 Not Found page.  Returning an object (ie
                           {html:myhtml}) will skip the normal callback and send
                           that content.

                           For "file" `req.fsPath` will be set to the file being
                           retrieved.  If `req.fsPath` is set to a new path and
                           the function returns true, the updated file will be
                           sent instead.

                           For websocket connections, it is run only befor the
                           first connect (when req.count == 0)                    */
    //beginFunc:           false,

    /* beginFuncOnFile     Whether to run the begin function before serving a
                           file (-i.e. files from the web_server/html/ directory)  */
    //beginFuncOnFile:     false,

    /* endFunc             Bool/Obj/Function.  A function to run after each JavaScript function

                           Value (i.e. {module: mymod}) is the same as beginFunc above.

                           It will also receive the `req` object.  In addition,
                           `req.reply` will be set to the return value of the
                           normal server callback function and req.reply can be
                           modified before it is sent.

                           For websocket connections, it is run after websockets
                           disconnects and after the req.wsOnDisconnect
                           callback, if any.  `req.reply` is an empty object,
                           modifying it has no effect and return value from
                           endFunc has not effect.

                           End function is never run on file requests.                     */
    //endfunc:             false,

    /* logFunc             Function - a function to replace normal logging, if log:true set above
                           See two examples below.
                           -e.g.
                           logFunc: myloggingfunc,                                                 */
    //logFunc:             false,

    /* defaultRangeMBytes  Number (range 0.01 to 1000) default range size for a "range: x-"
                           open ended request in megabytes (often used to seek into and chunk videos) */
    //defaultRangeMbytes:  8,
    serverRoot:            working_directory,
}

/*  Example logging functions :
    logdata: an object of various individual logging datum
    logline: the line which would have been written but for logFunc being set

// example logging func - log output abbreviated if not 200
function myloggingfunc (logdata, logline) {
    if(logdata.code != 200)
        rampart.utils.fprintf(rampart.utils.accessLog,
            '%s %s "%s %s%s%s %d"\n',
            logdata.addr, logdata.dateStr, logdata.method,
            logdata.path, logdata.query?"?":"", logdata.query,
            logdata.code );
    else
        rampart.utils.fprintf(rampart.utils.accessLog,
            "%s\n", logline);
}

// example logging func - skip logging for connections from localhost
function myloggingfunc_alt (logdata, logline) {
    if(logdata.addr=="127.0.0.1" || logdata.addr=="::1")
        return;
    rampart.utils.fprintf(rampart.utils.accessLog,
        "%s\n", logline);
}
*/


/* **************************************************** *
 *  Demo Mode setup                                    *
 * **************************************************** */
global.DEMO_MODE = demoMode;
global.DEMO_CLEAR_TIME = demoClearTime;
global.DEMO_MAX_FILE_SIZE = demoMaxFileSize;
global.DEMO_MAX_QUOTA = demoMaxQuota;

if (demoMode) {
    serverConf.dataRoot = working_directory + '/demo-data';
    if (!rampart.utils.stat(serverConf.dataRoot)) {
        rampart.utils.mkdir(serverConf.dataRoot);
        if (serverConf.user) {
            try { rampart.utils.chown({user: serverConf.user, path: serverConf.dataRoot}); } catch(e) {}
        }
    }
    rampart.utils.printf("DEMO MODE enabled (wipe interval: %ds, max file: %dMB, quota: %dMB)\n",
        demoClearTime, demoMaxFileSize/(1024*1024), demoMaxQuota/(1024*1024));
}



/* **************************************************** *
 *  ONLYOFFICE Document Server — detect, start, config *
 * **************************************************** */
var _oo = rampart.utils;

global.OO_AVAILABLE = false;
global.OO_JWT_SECRET = '';
global.OO_PORT = 0;

var ooDir = working_directory + '/onlyoffice';
var ooCompose = ooDir + '/docker-compose.yml';
var ooLocalJson = ooDir + '/local.json';

// Check if docker-compose.yml exists
if (!_oo.stat(ooCompose)) {
    _oo.printf("WARNING: ONLYOFFICE not installed — %s not found. Document editing disabled.\n", ooCompose);

} else if (_oo.exec("docker", "info").exitStatus !== 0) {
    _oo.printf("WARNING: Docker is not available. ONLYOFFICE document editing disabled.\n");

} else {
    // Parse docker-compose.yml for JWT_SECRET and port mapping
    var composeText = _oo.readFile(ooCompose, {returnString: true});

    var jwtMatch = composeText.match(/JWT_SECRET=(.+)/);
    var portMatch = composeText.match(/"(\d+):80"/);

    if (!jwtMatch) {
        _oo.printf("WARNING: JWT_SECRET not found in %s. Document editing disabled.\n", ooCompose);
    } else if (!portMatch) {
        _oo.printf("WARNING: Port mapping not found in %s. Document editing disabled.\n", ooCompose);
    } else {
        var jwtSecret = jwtMatch[1].trim();
        var ooPort = parseInt(portMatch[1]);

        // Verify local.json secrets match (if local.json exists)
        var localOk = true;
        if (_oo.stat(ooLocalJson)) {
            try {
                var localConf = JSON.parse(_oo.readFile(ooLocalJson, {returnString: true}));
                var localSecret = localConf.services && localConf.services.CoAuthoring &&
                                  localConf.services.CoAuthoring.secret &&
                                  localConf.services.CoAuthoring.secret.inbox &&
                                  localConf.services.CoAuthoring.secret.inbox.string;
                if (localSecret && localSecret !== jwtSecret) {
                    _oo.printf("WARNING: JWT_SECRET in docker-compose.yml does not match local.json.\n");
                }
            } catch(e) {
                _oo.printf("WARNING: Failed to parse %s. Document editing disabled.\n", ooLocalJson);
                localOk = false;
            }
        }

        if (localOk) {
            // Extract image and container names from docker-compose.yml
            var containerName = 'onlyoffice';
            var nameMatch = composeText.match(/container_name:\s*(\S+)/);
            if (nameMatch) containerName = nameMatch[1];

            var imageName = '';
            var imageMatch = composeText.match(/image:\s*(\S+)/);
            if (imageMatch) imageName = imageMatch[1];

            // Check if the Docker image is downloaded
            if (imageName) {
                var imageCheck = _oo.shell("docker image inspect " + imageName + " > /dev/null 2>&1; echo $?");
                if (imageCheck.stdout.trim() !== '0') {
                    _oo.printf("WARNING: ONLYOFFICE Docker image '%s' not downloaded. Document editing disabled.\n", imageName);
                    _oo.printf("  To install, run:\n    cd %s && sudo docker-compose up -d\n", ooDir);
                    _oo.printf("  Then restart the server.\n");
                    localOk = false;
                }
            }
        }

        if (localOk) {
            // Check if container is running
            var isRunning = _oo.shell("docker inspect -f '{{.State.Running}}' " + containerName + " 2>/dev/null");
            if (isRunning.stdout.trim() !== 'true') {
                _oo.printf("ONLYOFFICE container '%s' is not running. Starting...\n", containerName);
                var _dcCmd = _oo.exec("docker", "compose", "version").exitStatus === 0
                    ? 'docker compose' : 'docker-compose';
                var startResult = _oo.shell(_dcCmd + " -f " + ooCompose + " up -d 2>&1");
                if (startResult.exitStatus !== 0) {
                    _oo.printf("WARNING: Failed to start ONLYOFFICE:\n%s\nDocument editing disabled.\n",
                        startResult.stdout + startResult.stderr);
                    localOk = false;
                } else {
                    _oo.printf("ONLYOFFICE container started on port %d.\n", ooPort);
                }
            } else {
                _oo.printf("ONLYOFFICE container '%s' already running on port %d.\n", containerName, ooPort);
            }
        }

        if (localOk) {
            // Ensure rejectUnauthorized is false in local.json
            // (host.docker.internal won't match the server's TLS cert)
            var _ooPatched = _oo.shell(
                "docker exec " + containerName + " python3 -c '" +
                "import json; f=\"/etc/onlyoffice/documentserver/local.json\"; " +
                "d=json.load(open(f)); " +
                "co=d.setdefault(\"services\",{}).setdefault(\"CoAuthoring\",{}); " +
                "r=co.get(\"requestDefaults\",{}).get(\"rejectUnauthorized\"); " +
                "co.__setitem__(\"requestDefaults\",{\"rejectUnauthorized\":False}) if r is not False else None; " +
                "json.dump(d,open(f,\"w\"),indent=2) if r is not False else None; " +
                "print(\"patched\" if r is not False else \"ok\")' 2>/dev/null");
            _oo.shell("docker exec " + containerName + " supervisorctl restart ds:docservice ds:converter 2>/dev/null");

            // Extract version prefix from container's nginx config
            var verResult = _oo.exec("docker", "exec", containerName,
                "grep", "-oP", "\\d+\\.\\d+\\.\\d+(?=-\\$cache_tag)",
                "/etc/nginx/includes/ds-docservice.conf");
            var ooVersion = verResult.stdout.trim();

            var tagResult = _oo.exec("docker", "exec", containerName,
                "grep", "-oP", '(?<=")[^"]+(?=")',
                "/etc/nginx/includes/ds-cache.conf");
            var ooTag = tagResult.stdout.trim() || '0';

            if (!ooVersion) {
                _oo.printf("WARNING: Could not determine ONLYOFFICE version. Document editing disabled.\n");
            } else {
                var ooPrefix = '/' + ooVersion + '-' + ooTag + '/';

                // Add ONLYOFFICE proxy routes to serverConf.appendMap
                var ooBase = 'http://127.0.0.1:' + ooPort;
                var ooRoutes = [
                    '/web-apps/', '/sdkjs/', '/sdkjs-plugins/', '/fonts/',
                    '/dictionaries/', '/cache/', '/doc/', '/coauthoring/'
                ];
                for (var i = 0; i < ooRoutes.length; i++) {
                    serverConf.appendMap[ooRoutes[i]] = { proxy: ooBase + ooRoutes[i] };
                }
                serverConf.appendMap['/healthcheck'] = { proxy: ooBase + '/healthcheck' };
                serverConf.appendMap[ooPrefix] = { proxy: ooBase + ooPrefix };

                global.OO_AVAILABLE = true;
                global.OO_JWT_SECRET = jwtSecret;
                global.OO_PORT = ooPort;
                _oo.printf("ONLYOFFICE ready (version prefix: %s, port: %d).\n", ooPrefix, ooPort);
            }
        }
    }
}

/* **************************************************** *
 *  process command line options and start/stop server  *
 * **************************************************** */
require("rampart-webserver").web_server_conf(serverConf);

