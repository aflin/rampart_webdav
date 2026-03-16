# File Manager Plugin System

Plugins extend the file manager with custom file viewers,
editors, and URL drop handlers. Each plugin is a JavaScript
file in this directory that exports a configuration object.

Plugins are loaded automatically when the server starts. No
restart is needed when adding a new plugin — just refresh
the browser.


## Plugin Types

A plugin can be a **file handler** (opens files in a window),
a **drop handler** (processes URLs dropped onto the file
panel), or both.


## Basic Structure

    module.exports = {
        name: 'My Plugin',
        // ... properties described below
    };


## Common Properties

### name
(String, required)

Display name of the plugin. Must be unique across all
plugins.

    name: 'CSV Editor'


## File Handler Properties

File handler plugins open files in a window when clicked
in the file manager.

### extensions
(Array of Strings, required for file handlers)

File extensions this plugin handles, without the dot,
case-insensitive. Plugins are checked before the built-in
viewers, so a plugin can override the default behavior for
any extension.

    extensions: ['csv', 'tsv']

### mimeTypes
(Array of Strings, optional)

MIME types this plugin handles. Checked if the extension
match fails. Useful for files with non-standard extensions.

    mimeTypes: ['text/csv', 'text/tab-separated-values']

### mode
(String, optional, default: 'viewer')

Describes the plugin's capability:

  - 'viewer'  — read-only display
  - 'editor'  — supports editing and saving
  - 'both'    — adapts based on the user's write permission

    mode: 'editor'

### singleton
(Boolean, optional, default: false)

If true, only one window of this plugin type can be open
at a time. Opening another file with the same plugin
replaces the existing window. Useful for media players or
tools where multiple instances don't make sense.

    singleton: false

### icon
(String, optional)

Inline SVG data URI for the statusbar icon shown when the
window is minimized. Should be a 24x24 viewBox SVG with
stroke-based paths. If omitted, a default plugin icon is
used.

    icon: "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/
          2000/svg' viewBox='0 0 24 24' fill='none'
          stroke='%23000' stroke-width='2'%3E%3Crect x='3'
          y='3' width='18' height='18' rx='2'/%3E%3C/svg%3E"

### render
(Function, required for file handlers)

Called server-side to generate the HTML page displayed in
the plugin's window (rendered in an iframe).

Parameters:

  fileContent  (String)   The full text content of the file
  fileName     (String)   The file's name (e.g. 'data.csv')
  canEdit      (Boolean)  true if user has write permission
  fileUrl      (String)   The DAV URL of the file
                          (e.g. '/dav/aaron/data.csv')

Returns: A complete HTML document as a String.

    render: function(fileContent, fileName, canEdit, fileUrl) {
        return '<!DOCTYPE html><html><body>' +
            '<pre>' + fileContent + '</pre>' +
            '</body></html>';
    }

#### Client-Side JavaScript in render()

The HTML returned by render() can include any client-side
JavaScript. The authentication cookie is already present
in the browser, so fetch() to DAV URLs works without extra
authentication.

Saving files from the client:

    fetch(fileUrl, {
        method: 'PUT',
        body: newContent,
        credentials: 'same-origin'
    });

Communicating dirty state to the file manager:

The file manager tracks unsaved changes and warns before
closing. Use postMessage to communicate:

    // Mark as having unsaved changes (dot in title bar)
    window.parent.postMessage(
        {type: 'oo-dirty', dirty: true}, '*'
    );

    // Mark as saved (removes dot, disables close warning)
    window.parent.postMessage(
        {type: 'oo-dirty', dirty: false}, '*'
    );


## Drop Handler Properties

Drop handler plugins process URLs dragged and dropped onto
the file panel from external sources (browser address bar,
links, etc.).

### dropPattern
(RegExp or Array of RegExp, required for drop handlers)

Tested against the dropped URL. If any pattern matches,
this plugin handles the drop. A single RegExp or an array
are both accepted. Invalid entries (non-RegExp values in
an array) are silently filtered out with a warning to
stderr.

    // Single pattern
    dropPattern: /^https?:\/\//i

    // Array of patterns
    dropPattern: [
        /youtube\.com\/watch/i,
        /youtu\.be\//i,
        /vimeo\.com\/\d+/i
    ]

### drop
(Function, required for drop handlers)

Called server-side when a matching URL is dropped.

Parameters:

  url      (String)       The dropped URL
  fsDir    (String)       Absolute filesystem path of the
                          target directory
  davDir   (String)       DAV-relative path of the target
                          directory
  choice   (String|null)  User's selection if prompted,
                          null on first call

Returns one of the following objects:


#### Prompt the user for a choice

The drop() function is called again with the user's
selection in the choice parameter.

    return {
        prompt: true,
        title: 'How would you like to save this?',
        choices: [
            { label: 'Save as HTML', value: 'html' },
            { label: 'Save as text', value: 'text' }
        ]
    };


#### Return file content for the server to save

The server writes the content and auto-renames the file
if one with the same name already exists (e.g. page.html
becomes page-1.html).

    return {
        name: 'page.html',
        content: '<html>...</html>'
    };


#### Indicate the plugin already created the file

Use this when the plugin writes the file directly to
fsDir (e.g. for binary files). Auto-renaming is the
plugin's responsibility — use the autoRenameFile() helper.

    var fileName = autoRenameFile(fsDir, 'video.mp4');
    // ... write file to fsDir + '/' + fileName ...
    return {
        name: fileName,
        created: true
    };


#### Auto-open the file after saving

Add open: true to any of the above return objects. If
the plugin also has a render handler matching the file's
extension, the file opens with the plugin. Otherwise it
opens with the default viewer for that file type.

    return {
        name: 'page.html',
        content: '<html>...</html>',
        open: true
    };


#### Pass to the next plugin

If this plugin can't handle the URL after all (e.g.
a validation check fails), return pass. The file
manager will try the next matching plugin, or fall
back to the default URL fetch behavior.

    return { pass: true };


#### Run a long download in the background

For downloads that take more than a few seconds (e.g.
video downloads), return a background job. The server
runs the shell command in a thread and the frontend
polls for progress, showing a live byte count.

    return {
        background: true,
        cmd: 'yt-dlp -o /path/to/dir/%(title)s.%(ext)s'
             + ' ' + url,
        shellOpts: { timeout: 600000 },
        open: true
    };

Properties:
  background  (true)     Triggers background execution
  cmd         (String)   Shell command to run
  shellOpts   (Object)   Options passed to shell():
                         timeout, env, appendEnv, etc.
  open        (Boolean)  Auto-open the largest new file
                         when the job completes
  dir         (String)   Directory to monitor for new
                         files (defaults to the drop
                         target directory)

The frontend displays a persistent toast with the
download progress (bytes downloaded). When the job
completes, it checks for new files in the directory
and shows a success or error message.

The job status can be polled at:
  GET /dav/_plugin/job?id=<jobId>


#### Return an error

    return {
        error: 'Failed to download: server returned 404'
    };


## Plugin Priority

Plugins are loaded in filename order. Use numeric
prefixes to control priority:

    10-video-download.js   (checked first)
    50-csv.js
    90-web-clipper.js      (checked last)

When a URL is dropped, all matching plugins are tried
in order. If a plugin returns { pass: true }, the next
matching plugin is tried. If all plugins pass, the
default URL fetch behavior runs.


## Combining Both Types

A single plugin can have both file handler and drop handler
capabilities. For example, a plugin could download a URL
and save it as a .csv file (drop handler), and also render
.csv files as interactive tables (file handler).

    module.exports = {
        name: 'CSV Tool',
        extensions: ['csv'],
        mode: 'editor',
        render: function(fileContent, fileName,
                         canEdit, fileUrl) {
            // ... render table ...
        },

        dropPattern: /\.csv$/i,
        drop: function(url, fsDir, davDir, choice) {
            // ... download and save ...
        }
    };


## Available Server-Side APIs

Plugins run in the Rampart server environment. All
rampart.utils functions are globalized and available:

  stat(path)             — file/directory info
  readFile(path)         — read file (returns buffer)
  readFile(path, {returnString:true})  — read as string
  mkdir(path)            — create directory
  rmFile(path)           — delete file
  fopen/fwrite/fclose    — binary file writes
  bufferToString(buf)    — buffer to string
  stringToBuffer(str)    — string to buffer
  shell(cmd, options)    — run shell commands
  exec(cmd, opts, args)  — run executables
  autoRenameFile(dir, name) — non-conflicting filename

Additional modules:

  rampart.import.csv(data, options)    — CSV parser
  require('rampart-cmark').toHtml(md)  — Markdown to HTML
  require('rampart-crypto')            — crypto functions
  require('rampart-curl').fetch(url)   — HTTP client

See the Rampart documentation for the full API reference:
https://rampart.dev/docs


## Examples

See the included plugins in this directory:

  50-csv.js             — CSV/TSV table editor with
                          sorting, cell editing, and save
  10-video-download.js  — Video downloader using yt-dlp
                          with background downloads,
                          browser cookie auth, and auto-
                          scraped URL patterns from GitHub
  90-web-clipper.js     — URL drop handler that saves web
                          pages as HTML or plain text
                          (catch-all fallback)
