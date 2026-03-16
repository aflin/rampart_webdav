/*
 * Web Clipper Plugin (drop handler)
 *
 * Drop a URL onto the file panel to save the web page.
 * Prompts the user to save as HTML (full page) or plain text.
 *
 * This plugin demonstrates the 'drop' mode:
 *
 *   dropPattern  (RegExp, required for drop plugins)
 *     Regular expression tested against the dropped URL.
 *     If it matches, this plugin handles the drop.
 *
 *   drop         (Function, required for drop plugins)
 *     Called server-side when a matching URL is dropped.
 *
 *     Parameters:
 *       url      (String)  — the dropped URL
 *       fsDir    (String)  — absolute filesystem path of the target directory
 *       davDir   (String)  — DAV-relative path of the target directory
 *       choice   (String|null) — user's selection if prompted, null on first call
 *
 *     Returns one of:
 *       { prompt: true, title: 'Pick one', choices: [{label:'...', value:'...'}] }
 *         — asks the user to choose, then drop() is called again with the choice
 *
 *       { name: 'file.html', content: '...' }
 *         — plugin returns the file content; server saves it
 *
 *       { name: 'file.html', created: true }
 *         — plugin already wrote the file to fsDir; server just records metadata
 *
 *       { error: 'something went wrong' }
 *         — shown to the user as an error message
 */

var curl = require("rampart-curl");

module.exports = {
    name: 'Web Clipper',

    // Uncomment extensions to enable as an HTML viewer for clipped pages
    // extensions: ['html', 'htm'],
    // mode: 'viewer',

    /* render: function(fileContent, fileName, canEdit, fileUrl) {
        // Render the HTML in a sandboxed iframe
        return '<!DOCTYPE html><html><head>' +
            '<meta charset="utf-8">' +
            '<style>' +
            'body { margin: 0; padding: 0; height: 100vh; display: flex; flex-direction: column; }' +
            '.toolbar { padding: 6px 12px; background: #f0f0f0; border-bottom: 1px solid #ddd; font-size: 12px; color: #666; flex-shrink: 0; display: flex; align-items: center; gap: 12px; }' +
            '.toolbar a { color: #0366d6; text-decoration: none; }' +
            '.toolbar a:hover { text-decoration: underline; }' +
            '.content { flex: 1; overflow: auto; }' +
            '@media (prefers-color-scheme: dark) {' +
            '  .toolbar { background: #161b22; border-color: #30363d; color: #8b949e; }' +
            '  .toolbar a { color: #58a6ff; }' +
            '}' +
            '</style></head><body>' +
            '<div class="toolbar">' +
            '<span>' + fileName.replace(/</g, '&lt;') + '</span>' +
            '</div>' +
            '<div class="content">' + fileContent + '</div>' +
            '</body></html>';
    }, */

    // Match any http/https URL
    dropPattern: /^https?:\/\//i,

    drop: function(url, fsDir, davDir, choice) {
        // First call — ask what format to save
        if (!choice) {
            return {
                prompt: true,
                title: 'Save "' + url.substring(0, 80) + (url.length > 80 ? '...' : '') + '"',
                choices: [
                    { label: 'Save as HTML', value: 'html' },
                    { label: 'Save as plain text', value: 'text' }
                ]
            };
        }

        // Fetch the page
        var resp = curl.fetch(url, {
            returnText: true,
            location: true,
            "max-time": 30,
            insecure: true
        });

        if (resp.status !== 200) {
            return { error: 'Failed to fetch URL (HTTP ' + resp.status + ')' };
        }

        var content = resp.text || '';

        // Extract page title for the filename
        var titleMatch = content.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
        var pageTitle = titleMatch ? titleMatch[1].trim() : '';
        // Clean title for use as filename
        pageTitle = pageTitle
            .replace(/<[^>]+>/g, '')           // strip HTML tags
            .replace(/&[^;]+;/g, ' ')          // strip HTML entities
            .replace(/[\/\\:*?"<>|]/g, '-')    // replace illegal filename chars
            .replace(/\s+/g, ' ')              // collapse whitespace
            .trim();
        if (!pageTitle) pageTitle = 'clipped-page';
        if (pageTitle.length > 100) pageTitle = pageTitle.substring(0, 100);

        if (choice === 'text') {
            // Strip HTML tags for plain text
            var text = content
                .replace(/<script[\s\S]*?<\/script>/gi, '')
                .replace(/<style[\s\S]*?<\/style>/gi, '')
                .replace(/<[^>]+>/g, ' ')
                .replace(/&nbsp;/g, ' ')
                .replace(/&amp;/g, '&')
                .replace(/&lt;/g, '<')
                .replace(/&gt;/g, '>')
                .replace(/&quot;/g, '"')
                .replace(/  +/g, ' ')
                .replace(/\n\s*\n\s*\n/g, '\n\n')
                .trim();
            return { name: pageTitle + '.txt', content: text, open: false };
        }

        // HTML — save as-is, open with default handler
        return { name: pageTitle + '.html', content: content, open: false };
    }
};
