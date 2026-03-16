/*
 * Video Downloader Plugin (drop handler)
 *
 * Uses yt-dlp to download video, audio (MP3), and subtitles
 * from dropped URLs. Supports hundreds of sites.
 *
 * Requires yt-dlp or yt-dlp_linux installed on the system.
 * If not found, this plugin is silently disabled.
 *
 * On first load, this plugin scrapes the yt-dlp GitHub
 * repository to build a list of supported URL patterns.
 * The list is saved to url-patterns.json in this directory.
 * Delete that file to trigger a rebuild.
 *
 * Command line usage:
 *   rampart apps/webdav/plugins/10-video-download.js
 *     Runs the URL pattern scraper directly.
 */

var curl = require("rampart-curl");

// Shell escape helper (also defined in webdav.js, but
// needed when running standalone from command line)
function _shellEscape(s) {
    return "'" + s.replace(/'/g, "'\\''") + "'";
}

// When loaded as a module, process.scriptPath is the
// web_server root. When run directly, it's this directory.
var PLUGIN_DIR = stat(process.scriptPath + '/apps/webdav/plugins')
    ? process.scriptPath + '/apps/webdav/plugins'
    : process.scriptPath;

var PATTERNS_FILE = PLUGIN_DIR + '/url-patterns.json';
var PID_FILE = PLUGIN_DIR + '/.url-patterns-build.pid';

// Find yt-dlp binary
var YTDLP_PATH = (function() {
    var names = ['yt-dlp', 'yt-dlp_linux'];
    for (var i = 0; i < names.length; i++) {
        try {
            var res = shell('which ' + names[i],
                {timeout: 3000});
            if (res.exitStatus === 0)
                return res.stdout.trim();
        } catch(e) {}
    }
    var paths = [
        '/usr/local/bin/yt-dlp',
        '/usr/bin/yt-dlp',
        '/usr/local/bin/yt-dlp_linux'
    ];
    for (var j = 0; j < paths.length; j++) {
        if (stat(paths[j])) return paths[j];
    }
    return null;
})();

// -------------------------------------------------------
// URL Pattern Scraper
// -------------------------------------------------------

// Convert Python regex to JavaScript regex
function pyRegexToJs(pyPattern) {
    var s = pyPattern;
    // Named groups: (?P<name>...) → (?:...)
    // Duktape doesn't support (?<name>), so convert to
    // non-capturing groups
    s = s.replace(/\(\?P<[^>]+>/g, '(?:');
    // Also strip JS-style named groups in case
    s = s.replace(/\(\?<[^>]+>/g, '(?:');
    // Python verbose mode flag
    s = s.replace(/\(\?x\)/g, '');
    // Inline flags (?i), (?x:...) → (?:...)
    s = s.replace(/\(\?[ix]+\)/g, '');
    s = s.replace(/\(\?[ix]+:/g, '(?:');
    return s;
}

// Scrape _VALID_URL patterns from yt-dlp GitHub repo
function scrapePatterns() {
    fprintf(stderr,
        "video-download: scraping yt-dlp URL patterns...\n");

    // Get file listing from GitHub API
    var allPatterns = [];

    // Major known sites (always included as fallback)
    var knownSites = [
        'youtube\\.com/', 'youtu\\.be/',
        'vimeo\\.com/', 'dailymotion\\.com/',
        'twitch\\.tv/', 'twitter\\.com/', 'x\\.com/',
        'facebook\\.com/', 'instagram\\.com/',
        'tiktok\\.com/', 'reddit\\.com/',
        'soundcloud\\.com/', 'bandcamp\\.com/',
        'bilibili\\.com/', 'rumble\\.com/',
        'bitchute\\.com/', 'odysee\\.com/',
        'streamable\\.com/', 'v\\.redd\\.it/',
        'vm\\.tiktok\\.com/',
        'm\\.youtube\\.com/', 'music\\.youtube\\.com/'
    ];
    for (var k = 0; k < knownSites.length; k++) {
        allPatterns.push({
            source: knownSites[k], flags: 'i'
        });
    }

    // Download the repo as a tarball and extract
    // _VALID_URL patterns from extractor .py files.
    // This avoids GitHub API rate limits.
    fprintf(stderr,
        "video-download: downloading yt-dlp source...\n");

    var tarResp;
    try {
        tarResp = curl.fetch(
            'https://github.com/yt-dlp/yt-dlp/archive' +
            '/refs/heads/master.tar.gz',
            {
                "max-time": 120,
                location: true,
                header: [
                    'User-Agent: rampart-filemanager'
                ]
            }
        );
    } catch(e) {
        fprintf(stderr,
            "video-download: download failed: %s\n",
            e.message);
        return allPatterns;
    }

    if (tarResp.status !== 200 || !tarResp.body) {
        fprintf(stderr,
            "video-download: download failed (HTTP %d)\n",
            tarResp.status);
        return allPatterns;
    }

    // Write tarball to temp file
    var tarPath = '/tmp/yt-dlp-extractors.tar.gz';
    var tarFd = fopen(tarPath, 'w');
    fwrite(tarFd, tarResp.body);
    fclose(tarFd);

    // Extract just the extractor .py files
    var extractDir = '/tmp/yt-dlp-extractors';
    shell('rm -rf ' + extractDir, {timeout: 5000});
    shell('mkdir -p ' + extractDir, {timeout: 2000});
    shell('tar xzf ' + tarPath +
        ' --strip-components=3' +
        ' -C ' + extractDir +
        ' "yt-dlp-master/yt_dlp/extractor/"' +
        ' 2>/dev/null',
        {timeout: 30000});

    // Process extracted files
    var pyFiles;
    try { pyFiles = readdir(extractDir); }
    catch(e) { pyFiles = []; }

    var skip = {
        '__init__.py': 1, '_extractors.py': 1,
        'common.py': 1, 'commonmistakes.py': 1,
        'commonprotocols.py': 1, 'adobepass.py': 1
    };

    var fetched = 0;
    for (var fi = 0; fi < pyFiles.length; fi++) {
        var pyName = pyFiles[fi];
        if (!/\.py$/.test(pyName) || skip[pyName]) continue;

        var src;
        try {
            src = readFile(extractDir + '/' + pyName,
                {returnString: true});
        } catch(e) { continue; }

        // Extract _VALID_URL patterns.
        // Handles single-line: _VALID_URL = r'...'
        //   and multi-line:    _VALID_URL = r'''...'''
        var urlPatterns = [];

        // Triple-quoted (multi-line)
        var tripleRe = /_VALID_URL\s*=\s*r?(?:'''([\s\S]*?)'''|"""([\s\S]*?)""")/g;
        var tm;
        while ((tm = tripleRe.exec(src)) !== null) {
            var tp = (tm[1] || tm[2] || '').replace(/\s+/g, '');
            if (tp) urlPatterns.push(tp);
        }

        // Single-quoted (single-line)
        var singleRe = /_VALID_URL\s*=\s*r?(?:'([^']+)'|"([^"]+)")/g;
        var sm;
        while ((sm = singleRe.exec(src)) !== null) {
            var sp = sm[1] || sm[2];
            if (sp) urlPatterns.push(sp);
        }

        for (var pi = 0; pi < urlPatterns.length; pi++) {
            var jsPattern = pyRegexToJs(urlPatterns[pi]);
            jsPattern = jsPattern.replace(/^\(\?x\)/, '');
            // Skip overly broad patterns that match
            // any URL (generic extractor etc.)
            if (jsPattern.length < 10 ||
                /^\.\*$|^https\?:?$/.test(jsPattern)) {
                continue;
            }
            try {
                new RegExp(jsPattern, 'i');
                allPatterns.push({
                    source: jsPattern,
                    flags: 'i'
                });
            } catch(e) {}
        }
        fetched++;
    }

    // Cleanup
    shell('rm -rf ' + extractDir + ' ' + tarPath,
        {timeout: 5000});

    fprintf(stderr,
        "video-download: done. %d patterns from " +
        "%d files\n",
        allPatterns.length, fetched);

    return allPatterns;
}

// -------------------------------------------------------
// Command-line mode: run scraper directly
//   rampart -g 10-video-download.js --scrape
// -------------------------------------------------------
if (process.argv &&
    process.argv.indexOf('--scrape') !== -1) {
    var patterns = scrapePatterns();
    var fd = fopen(PATTERNS_FILE, 'w');
    fwrite(fd, JSON.stringify(patterns, null, 2));
    fclose(fd);
    // Clean up PID file
    try { rmFile(PID_FILE); } catch(e) {}
    printf("Saved %d patterns to %s\n",
        patterns.length, PATTERNS_FILE);
    process.exit(0);
}

// -------------------------------------------------------
// Module mode: plugin registration
// -------------------------------------------------------

// If yt-dlp is not installed, export empty object
if (!YTDLP_PATH) {
    module.exports = {};
} else {

    // Fallback patterns for when JSON hasn't been built yet
    var FALLBACK_PATTERNS = [
        /youtube\.com\//i,
        /youtu\.be\//i,
        /vimeo\.com\//i,
        /dailymotion\.com\//i,
        /twitch\.tv\//i,
        /twitter\.com\//i,
        /x\.com\//i,
        /facebook\.com\//i,
        /instagram\.com\//i,
        /tiktok\.com\//i,
        /reddit\.com\//i,
        /soundcloud\.com\//i,
        /bandcamp\.com\//i,
        /bilibili\.com\//i,
        /rumble\.com\//i,
        /bitchute\.com\//i,
        /odysee\.com\//i,
        /streamable\.com\//i,
        /v\.redd\.it\//i
    ];

    // Load patterns from JSON, or start background build
    function loadPatterns() {
        if (stat(PATTERNS_FILE)) {
            try {
                var data = JSON.parse(
                    readFile(PATTERNS_FILE,
                        {returnString: true})
                );
                var regexes = [];
                for (var i = 0; i < data.length; i++) {
                    try {
                        regexes.push(new RegExp(
                            data[i].source, data[i].flags
                        ));
                    } catch(e) {}
                }
                if (regexes.length > 0) return regexes;
            } catch(e) {}
        }
        return null;
    }

    // Kick off background build if needed (only once)
    function ensurePatternsBuilding() {
        if (stat(PATTERNS_FILE)) return;

        // Check if build already in progress
        if (stat(PID_FILE)) {
            try {
                var pidStr = readFile(PID_FILE,
                    {returnString: true}).trim();
                var pid = parseInt(pidStr);
                if (pid && kill(pid, 0)) return;
            } catch(e) {}
            try { rmFile(PID_FILE); } catch(e) {}
        }

        // Start background scraper
        fprintf(stderr,
            "video-download: starting background " +
            "pattern build...\n");
        try {
            var scrapeCmd = 'nohup rampart -g ' +
                _shellEscape(PLUGIN_DIR +
                    '/10-video-download.js') +
                ' --scrape > /dev/null 2>&1 & echo $!';
            var res = shell(scrapeCmd, {timeout: 5000});
            var bgPid = res.stdout.trim();
            if (bgPid) {
                var pidFd = fopen(PID_FILE, 'w');
                fwrite(pidFd, bgPid);
                fclose(pidFd);
            }
        } catch(e) {}
    }

    // Start build on module load if needed
    ensurePatternsBuilding();

    // Use fallback patterns for dropPattern registration.
    // The drop() function checks the full pattern list.
    // Include a heuristic for video-like URLs.
    FALLBACK_PATTERNS.push(
        /\/(video|watch|embed|clip|episode|play|stream|media)\b/i
    );
    var COOKIE_CONF = PLUGIN_DIR + '/browser-cookies.json';

    // Get the actual home directory of the server user
    // (HOME env var may be wrong after privilege drop)
    var SERVER_HOME = (function() {
        try {
            var res = shell('eval echo ~$(whoami)',
                {timeout: 2000});
            return res.stdout.trim();
        } catch(e) {}
        return '';
    })();

    // Environment override for yt-dlp to find browser cookies
    // (HOME may be wrong after server privilege drop)
    // Build shell options with correct HOME and a timeout
    function ytdlpShellOpts(timeout) {
        var opts = { timeout: timeout || 15000 };
        if (SERVER_HOME) {
            opts.env = { HOME: SERVER_HOME };
            opts.appendEnv = true;
        }
        return opts;
    }

    function ytdlpShell(cmd, timeout) {
        return shell(cmd, ytdlpShellOpts(timeout));
    }

    // Get saved browser cookie config, or null
    function getCookieArgs() {
        if (!stat(COOKIE_CONF)) return '';
        try {
            var conf = JSON.parse(
                readFile(COOKIE_CONF, {returnString:true})
            );
            if (conf.browser) {
                return ' --cookies-from-browser ' +
                    _shellEscape(conf.browser);
            }
        } catch(e) {}
        return '';
    }

    function saveBrowserChoice(browser) {
        var fd = fopen(COOKIE_CONF, 'w');
        fwrite(fd, JSON.stringify(
            {browser: browser}, null, 2));
        fclose(fd);
    }

    module.exports = {
        name: 'Video Downloader',

        dropPattern: FALLBACK_PATTERNS,

        drop: function(url, fsDir, davDir, choice) {
            var cookieArgs = getCookieArgs();

            // Handle browser selection choice
            if (choice && choice.indexOf('browser:') === 0) {
                var browser = choice.substring(8);
                saveBrowserChoice(browser);
                // Show download options
                return {
                    prompt: true,
                    title: 'Download from: ' +
                        url.substring(0, 80) +
                        (url.length > 80 ? '...' : ''),
                    choices: [
                        { label: 'Video (best quality)',
                          value: 'video' },
                        { label: 'Audio only (MP3)',
                          value: 'audio' },
                        { label: 'Video + Audio + Subtitles',
                          value: 'all' }
                    ]
                };
            }

            // First call — check patterns and show options
            if (!choice) {
                var fullPatterns = loadPatterns();
                var matched = false;
                if (fullPatterns) {
                    for (var p = 0; p < fullPatterns.length;
                         p++) {
                        if (fullPatterns[p].test(url)) {
                            matched = true;
                            break;
                        }
                    }
                } else {
                    // No patterns loaded yet — check
                    // fallback patterns
                    for (var fp = 0;
                         fp < FALLBACK_PATTERNS.length;
                         fp++) {
                        if (FALLBACK_PATTERNS[fp].test(url)) {
                            matched = true;
                            break;
                        }
                    }
                }

                if (!matched) {
                    // Heuristic: URLs with video-like
                    // paths are worth a quick check
                    if (/\/(video|watch|embed|clip|episode|play|stream|media)\b/i.test(url)) {
                        try {
                            var hCheck = ytdlpShell(
                                YTDLP_PATH +
                                ' --simulate --no-warnings' +
                                ' -q' +
                                ' --ies "default,-generic"' +
                                ' --js-runtimes node' +
                                ' --remote-components ejs:github' +
                                cookieArgs + ' ' +
                                _shellEscape(url) +
                                ' 2>&1', 15000);
                            if (hCheck.exitStatus !== 0) {
                                return { pass: true };
                            }
                        } catch(e) {
                            return { pass: true };
                        }
                    } else {
                        return { pass: true };
                    }
                }

                var dlChoices = [
                    { label: 'Video (best quality)',
                      value: 'video' },
                    { label: 'Audio only (MP3)',
                      value: 'audio' },
                    { label: 'Video + Audio + Subtitles',
                      value: 'all' }
                ];
                if (!getCookieArgs()) {
                    dlChoices.push({
                        label: 'Configure browser cookies ' +
                            '(for sites requiring login)',
                        value: 'cookies'
                    });
                }
                return {
                    prompt: true,
                    title: 'Download from: ' +
                        url.substring(0, 80) +
                        (url.length > 80 ? '...' : ''),
                    choices: dlChoices
                };
            }

            // Handle cookie configuration
            if (choice === 'cookies') {
                return {
                    prompt: true,
                    title: 'Select a browser where you ' +
                        'are logged in to this site. ' +
                        'The browser must be installed ' +
                        'on the server.',
                    choices: [
                        { label: 'Firefox',
                          value: 'browser:firefox' },
                        { label: 'Chrome',
                          value: 'browser:chrome' },
                        { label: 'Chromium',
                          value: 'browser:chromium' },
                        { label: 'Edge',
                          value: 'browser:edge' },
                        { label: 'Brave',
                          value: 'browser:brave' },
                        { label: 'Opera',
                          value: 'browser:opera' }
                    ]
                };
            }

            // Build the download command(s)
            var tmpl = fsDir + '/%(title).80s.%(ext)s';
            var commonArgs = ' --no-playlist' +
                ' --js-runtimes node' +
                ' --remote-components ejs:github' +
                cookieArgs;
            var cmds = [];

            if (choice === 'audio' || choice === 'all') {
                cmds.push(YTDLP_PATH +
                    ' -x --audio-format mp3' +
                    commonArgs +
                    ' -o ' + _shellEscape(
                        fsDir + '/%(title).80s.mp3') +
                    ' ' + _shellEscape(url));
            }

            if (choice === 'video' || choice === 'all') {
                cmds.push(YTDLP_PATH +
                    ' -f "bv*+ba/b/best"' +
                    commonArgs +
                    ' -o ' + _shellEscape(tmpl) +
                    ' ' + _shellEscape(url));
            }

            if (choice === 'all') {
                cmds.push(YTDLP_PATH +
                    ' --write-sub --write-auto-sub' +
                    ' --sub-format srt --sub-lang en' +
                    ' --skip-download' +
                    commonArgs +
                    ' -o ' + _shellEscape(
                        fsDir + '/%(title).80s') +
                    ' ' + _shellEscape(url));
            }

            // Run in background — join commands with ;
            var fullCmd = cmds.join(' 2>&1; ') + ' 2>&1';

            return {
                background: true,
                cmd: fullCmd,
                shellOpts: ytdlpShellOpts(600000),
                dir: fsDir,
                open: (choice === 'video' ||
                       choice === 'all')
            };
        }
    };

} // end if (YTDLP_PATH)
