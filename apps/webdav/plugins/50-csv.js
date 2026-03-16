/*
 * CSV/TSV Editor Plugin
 *
 * This plugin serves as a reference implementation for the file manager
 * plugin system. Plugins are JavaScript files in this directory that
 * export an object with the properties described below.
 *
 * PLUGIN PROPERTIES:
 *
 *   name        (String, required)
 *     Display name of the plugin. Must be unique across all plugins.
 *
 *   extensions  (Array of Strings, required)
 *     File extensions this plugin handles (without the dot, case-insensitive).
 *     When a file is opened, extensions are checked before the built-in viewers.
 *
 *   mimeTypes   (Array of Strings, optional)
 *     MIME types this plugin handles. Checked if extension match fails.
 *     Example: ['text/csv', 'text/tab-separated-values']
 *
 *   mode        (String, optional, default: 'viewer')
 *     'viewer'  — read-only display
 *     'editor'  — supports editing and saving
 *     'both'    — adapts based on the canEdit parameter
 *
 *   icon        (String, optional)
 *     Inline SVG data URI for the statusbar icon when the window is minimized.
 *     Should be a 24x24 viewBox SVG with stroke-based paths.
 *     If omitted, a default plugin icon is used.
 *
 *   singleton   (Boolean, optional, default: false)
 *     If true, only one window of this plugin type can be open at a time.
 *     Opening another file with the same plugin replaces the existing window.
 *     Useful for media players or tools where multiple instances don't make sense.
 *
 *   render      (Function, required)
 *     Called server-side to generate the HTML page displayed in the plugin window.
 *
 *     Parameters:
 *       fileContent  (String)  — the full text content of the file
 *       fileName     (String)  — the file's name (e.g. 'data.csv')
 *       canEdit      (Boolean) — true if the user has write permission
 *       fileUrl      (String)  — the DAV URL of the file (e.g. '/dav/aaron/data.csv')
 *                                Use this in client-side fetch() to save:
 *                                  fetch(fileUrl, {method:'PUT', body:newContent})
 *
 *     Returns:
 *       (String) — a complete HTML document to render in an iframe.
 *
 *     The rendered HTML can include any client-side JavaScript. The auth
 *     cookie is already present in the browser, so fetch() to the DAV
 *     URL works without extra authentication.
 *
 *     To communicate dirty state to the parent file manager, use postMessage:
 *       window.parent.postMessage({type:'oo-dirty', dirty:true}, '*');   // unsaved
 *       window.parent.postMessage({type:'oo-dirty', dirty:false}, '*');  // saved
 *     The file manager will show a dot in the title bar and warn on close.
 *
 * DROP HANDLER PROPERTIES (for URL drop plugins):
 *
 *   dropPattern  (RegExp or Array of RegExp, required for drop plugins)
 *     Tested against the dropped URL. If any pattern matches, this plugin
 *     handles the drop. A single RegExp or an array are both accepted.
 *
 *   drop         (Function, required for drop plugins)
 *     Called server-side when a matching URL is dropped.
 *
 *     Parameters:
 *       url      (String)      — the dropped URL
 *       fsDir    (String)      — absolute filesystem path of the target directory
 *       davDir   (String)      — DAV-relative path of the target directory
 *       choice   (String|null) — user's selection if prompted, null on first call
 *
 *     Returns one of:
 *       { prompt: true, title: '...', choices: [{label:'...', value:'...'}] }
 *         — asks the user to choose; drop() is called again with the choice
 *
 *       { name: 'file.html', content: '...' }
 *         — server saves the content as a file
 *
 *       { name: 'file.html', created: true }
 *         — plugin already wrote the file to fsDir
 *
 *       { name: 'file.html', content: '...', open: true }
 *         — save and open the file. If this plugin has a render handler
 *           matching the file extension, it will be used. Otherwise the
 *           default viewer for the file type is used.
 *
 *       { pass: true }
 *         — this plugin can't handle the URL after all;
 *           try the next matching plugin or fall back to
 *           the default URL fetch behavior
 *
 *       { error: 'something went wrong' }
 *         — shown to the user as an error message
 *
 *   A plugin can have both render and drop handlers — they are independent.
 *
 *   Available server-side APIs:
 *     All rampart.utils functions are available (globalized).
 *     rampart.import.csv() — CSV parser
 *     require('rampart-cmark').toHtml() — Markdown to HTML
 *     require('rampart-crypto') — crypto functions
 *     require('rampart-curl') — HTTP client
 *     See Rampart documentation for the full API.
 */

module.exports = {
    // Display name (required, must be unique)
    name: 'CSV Editor',

    // File extensions to handle (required, case-insensitive, no dot)
    extensions: ['csv', 'tsv'],

    // MIME types to handle (optional, checked if extension match fails)
    mimeTypes: ['text/csv', 'text/tab-separated-values'],

    // Plugin mode: 'viewer', 'editor', or 'both'
    mode: 'editor',

    // Only one window of this type at a time? (optional, default: false)
    singleton: false,

    // Statusbar icon SVG (optional, 24x24 viewBox, stroke-based)
    // This icon is a table/grid shape
    icon: "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%23000' stroke-width='2'%3E%3Crect x='3' y='3' width='18' height='18' rx='2'/%3E%3Cline x1='3' y1='9' x2='21' y2='9'/%3E%3Cline x1='3' y1='15' x2='21' y2='15'/%3E%3Cline x1='9' y1='3' x2='9' y2='21'/%3E%3C/svg%3E",

    // Render function (required)
    // Called server-side. Returns a complete HTML document string.
    render: function(fileContent, fileName, canEdit, fileUrl) {
        var delimiter = /\.tsv$/i.test(fileName) ? '\t' : ',';
        var parsed = rampart.import.csv(fileContent, {
            hasHeaderRow: true,
            delimiter: delimiter,
            returnType: 'array',
            normalize: false
        });
        var columns = parsed.columns || [];
        var rows = parsed.results || [];

        // Build JSON data for the client
        var dataJson = JSON.stringify({
            columns: columns,
            rows: rows,
            canEdit: canEdit,
            fileUrl: fileUrl,
            delimiter: delimiter
        });

        return '<!DOCTYPE html><html><head>' +
            '<meta charset="utf-8">' +
            '<style>' +
            '* { box-sizing: border-box; margin: 0; padding: 0; }' +
            'body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; font-size: 13px; }' +
            '.csv-wrap { width: 100%; height: 100vh; display: flex; flex-direction: column; }' +
            '.csv-toolbar { padding: 6px 12px; background: #f0f0f0; border-bottom: 1px solid #ddd; display: flex; gap: 8px; align-items: center; flex-shrink: 0; }' +
            '.csv-toolbar button { padding: 4px 12px; border: 1px solid #ccc; border-radius: 4px; background: #fff; cursor: pointer; font-size: 13px; }' +
            '.csv-toolbar button:hover { background: #e8e8e8; }' +
            '.csv-toolbar button:disabled { opacity: 0.5; cursor: default; }' +
            '.csv-toolbar button.primary { background: #0366d6; color: #fff; border-color: #0366d6; }' +
            '.csv-toolbar button.primary:hover { background: #0255b3; }' +
            '.csv-toolbar .spacer { flex: 1; }' +
            '.csv-toolbar .info { color: #666; font-size: 12px; }' +
            '.csv-scroll { flex: 1; overflow: auto; }' +
            'table { border-collapse: collapse; width: max-content; min-width: 100%; }' +
            'th, td { border: 1px solid #ddd; padding: 4px 8px; text-align: left; white-space: pre; min-width: 60px; max-width: 400px; overflow: hidden; text-overflow: ellipsis; }' +
            'th { background: #f6f8fa; font-weight: 600; position: sticky; top: 0; z-index: 1; cursor: pointer; user-select: none; }' +
            'th:hover { background: #e8ecf0; }' +
            'th .sort-arrow { font-size: 10px; margin-left: 4px; }' +
            'td[contenteditable="true"]:focus { outline: 2px solid #0366d6; outline-offset: -2px; background: #fff8e1; }' +
            'tr:nth-child(even) { background: #fafbfc; }' +
            'tr:hover { background: #f0f4f8; }' +
            '@media (prefers-color-scheme: dark) {' +
            '  body { background: #0d1117; color: #c9d1d9; }' +
            '  .csv-toolbar { background: #161b22; border-color: #30363d; }' +
            '  .csv-toolbar button { background: #21262d; border-color: #30363d; color: #c9d1d9; }' +
            '  .csv-toolbar button:hover { background: #30363d; }' +
            '  .csv-toolbar button.primary { background: #238636; border-color: #238636; color: #fff; }' +
            '  .csv-toolbar .info { color: #8b949e; }' +
            '  th { background: #161b22; border-color: #30363d; }' +
            '  th:hover { background: #1c2128; }' +
            '  td { border-color: #30363d; }' +
            '  td[contenteditable="true"]:focus { background: #1c1e00; }' +
            '  tr:nth-child(even) { background: #0d1117; }' +
            '  tr:hover { background: #161b22; }' +
            '}' +
            '</style></head><body>' +
            '<div class="csv-wrap">' +
            '<div class="csv-toolbar">' +
            (canEdit ? '<button class="primary" id="save-btn" disabled>Save</button>' +
                '<button id="add-row-btn">Add Row</button>' +
                '<button id="del-row-btn">Delete Row</button>' +
                '<button id="add-col-btn">Add Column</button>' +
                '<button id="del-col-btn">Delete Column</button>' : '') +
            '<div class="spacer"></div>' +
            '<span class="info" id="info"></span>' +
            '</div>' +
            '<div class="csv-scroll"><table id="csv-table"></table></div>' +
            '</div>' +
            '<script>' +
            'var DATA = ' + dataJson + ';\n' +
            'var dirty = false;\n' +
            'var sortCol = -1, sortDesc = false;\n' +
            '\n' +
            'function renderTable() {\n' +
            '  var t = document.getElementById("csv-table");\n' +
            '  var html = "<thead><tr>";\n' +
            '  for (var c = 0; c < DATA.columns.length; c++) {\n' +
            '    var arrow = sortCol === c ? (sortDesc ? " \\u25BC" : " \\u25B2") : "";\n' +
            '    html += "<th data-col=\\"" + c + "\\">" + esc(DATA.columns[c]) + "<span class=\\"sort-arrow\\">" + arrow + "</span></th>";\n' +
            '  }\n' +
            '  html += "</tr></thead><tbody>";\n' +
            '  for (var r = 0; r < DATA.rows.length; r++) {\n' +
            '    html += "<tr>";\n' +
            '    for (var c = 0; c < DATA.columns.length; c++) {\n' +
            '      var val = DATA.rows[r][c];\n' +
            '      if (val === null || val === undefined) val = "";\n' +
            '      html += "<td" + (DATA.canEdit ? " contenteditable=\\"true\\"" : "") + " data-r=\\"" + r + "\\" data-c=\\"" + c + "\\">" + esc(String(val)) + "</td>";\n' +
            '    }\n' +
            '    html += "</tr>";\n' +
            '  }\n' +
            '  html += "</tbody>";\n' +
            '  t.innerHTML = html;\n' +
            '  document.getElementById("info").textContent = DATA.rows.length + " rows, " + DATA.columns.length + " columns";\n' +
            '}\n' +
            '\n' +
            'function esc(s) { return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }\n' +
            '\n' +
            'function setDirty() {\n' +
            '  if (!dirty) {\n' +
            '    dirty = true;\n' +
            '    var btn = document.getElementById("save-btn");\n' +
            '    if (btn) btn.disabled = false;\n' +
            '    window.parent.postMessage({type:"oo-dirty", dirty:true}, "*");\n' +
            '  }\n' +
            '}\n' +
            '\n' +
            'function toCsv() {\n' +
            '  var delim = DATA.delimiter;\n' +
            '  var lines = [];\n' +
            '  lines.push(DATA.columns.map(function(c) { return csvQuote(c, delim); }).join(delim));\n' +
            '  for (var r = 0; r < DATA.rows.length; r++) {\n' +
            '    var cells = [];\n' +
            '    for (var c = 0; c < DATA.columns.length; c++) {\n' +
            '      var v = DATA.rows[r][c];\n' +
            '      cells.push(csvQuote(v === null || v === undefined ? "" : String(v), delim));\n' +
            '    }\n' +
            '    lines.push(cells.join(delim));\n' +
            '  }\n' +
            '  return lines.join("\\n") + "\\n";\n' +
            '}\n' +
            '\n' +
            'function csvQuote(s, d) {\n' +
            '  if (s.indexOf(d) !== -1 || s.indexOf(\'"\') !== -1 || s.indexOf("\\n") !== -1) {\n' +
            '    return \'"\' + s.replace(/"/g, \'""\') + \'"\';\n' +
            '  }\n' +
            '  return s;\n' +
            '}\n' +
            '\n' +
            '// Cell editing\n' +
            'document.getElementById("csv-table").addEventListener("input", function(e) {\n' +
            '  if (e.target.tagName === "TD" && e.target.dataset.r !== undefined) {\n' +
            '    DATA.rows[parseInt(e.target.dataset.r)][parseInt(e.target.dataset.c)] = e.target.textContent;\n' +
            '    setDirty();\n' +
            '  }\n' +
            '});\n' +
            '\n' +
            '// Column sorting\n' +
            'document.getElementById("csv-table").addEventListener("click", function(e) {\n' +
            '  var th = e.target.closest("th");\n' +
            '  if (!th || th.dataset.col === undefined) return;\n' +
            '  var col = parseInt(th.dataset.col);\n' +
            '  if (sortCol === col) sortDesc = !sortDesc;\n' +
            '  else { sortCol = col; sortDesc = false; }\n' +
            '  DATA.rows.sort(function(a, b) {\n' +
            '    var va = a[col] === null ? "" : String(a[col]);\n' +
            '    var vb = b[col] === null ? "" : String(b[col]);\n' +
            '    var na = parseFloat(va), nb = parseFloat(vb);\n' +
            '    if (!isNaN(na) && !isNaN(nb)) return sortDesc ? nb - na : na - nb;\n' +
            '    return sortDesc ? vb.localeCompare(va) : va.localeCompare(vb);\n' +
            '  });\n' +
            '  renderTable();\n' +
            '});\n' +
            '\n' +
            '// Save\n' +
            'if (document.getElementById("save-btn")) {\n' +
            '  document.getElementById("save-btn").addEventListener("click", function() {\n' +
            '    var csv = toCsv();\n' +
            '    fetch(DATA.fileUrl, { method: "PUT", body: csv, credentials: "same-origin" }).then(function(r) {\n' +
            '      if (r.ok || r.status === 204 || r.status === 201) {\n' +
            '        dirty = false;\n' +
            '        document.getElementById("save-btn").disabled = true;\n' +
            '        window.parent.postMessage({type:"oo-dirty", dirty:false}, "*");\n' +
            '      }\n' +
            '    });\n' +
            '  });\n' +
            '}\n' +
            '\n' +
            '// Add/delete row\n' +
            'if (document.getElementById("add-row-btn")) {\n' +
            '  document.getElementById("add-row-btn").addEventListener("click", function() {\n' +
            '    var newRow = [];\n' +
            '    for (var i = 0; i < DATA.columns.length; i++) newRow.push("");\n' +
            '    DATA.rows.push(newRow);\n' +
            '    setDirty();\n' +
            '    renderTable();\n' +
            '  });\n' +
            '}\n' +
            'if (document.getElementById("del-row-btn")) {\n' +
            '  document.getElementById("del-row-btn").addEventListener("click", function() {\n' +
            '    if (DATA.rows.length === 0) return;\n' +
            '    DATA.rows.pop();\n' +
            '    setDirty();\n' +
            '    renderTable();\n' +
            '  });\n' +
            '}\n' +
            '\n' +
            '// Add/delete column\n' +
            'if (document.getElementById("add-col-btn")) {\n' +
            '  document.getElementById("add-col-btn").addEventListener("click", function() {\n' +
            '    var name = prompt("Column name:");\n' +
            '    if (!name) return;\n' +
            '    DATA.columns.push(name);\n' +
            '    for (var r = 0; r < DATA.rows.length; r++) DATA.rows[r].push("");\n' +
            '    setDirty();\n' +
            '    renderTable();\n' +
            '  });\n' +
            '}\n' +
            'if (document.getElementById("del-col-btn")) {\n' +
            '  document.getElementById("del-col-btn").addEventListener("click", function() {\n' +
            '    if (DATA.columns.length === 0) return;\n' +
            '    DATA.columns.pop();\n' +
            '    for (var r = 0; r < DATA.rows.length; r++) DATA.rows[r].pop();\n' +
            '    setDirty();\n' +
            '    renderTable();\n' +
            '  });\n' +
            '}\n' +
            '\n' +
            'renderTable();\n' +
            '<\/script></body></html>';
    }
};
