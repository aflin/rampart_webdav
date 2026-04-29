/*
 * rdp-audio.js — client-side MITM between IronRDP WASM and the RDCleanPath
 * proxy WebSocket, to inject rdpsnd channel negotiation and plug the
 * resulting audio stream into the Web Audio API.
 *
 * This file exposes a single function:
 *
 *   rdpAudio.wrapWebSocket(ws) -> ws
 *
 * Call it with a newly-constructed WebSocket BEFORE IronRDP attaches its
 * own listeners. The function returns the same ws with intercepted send
 * and addEventListener('message', ...) hooks.
 *
 * Current scope: Phase 1 — observe only. Logs the first few outgoing
 * binary messages (should be RDCleanPath Request, then MCS Connect
 * Initial, then MCS Erect Domain, etc.) so we can confirm the hook
 * is in the right place before we start modifying bytes.
 */

var rdpAudio = (function() {

    // ── BER / PER micro-parser ─────────────────────────────────────
    // We only need enough to locate length fields along the path to
    // the Client Network Data block so we can patch them all after
    // inserting 12 bytes for a new channel entry.

    // Read a BER length starting at `off`. Returns { len, hdrSize }.
    // hdrSize is the number of bytes consumed by the length encoding.
    function readBerLen(u8, off) {
        var b = u8[off];
        if (b < 0x80) return { len: b, hdrSize: 1 };
        var nBytes = b & 0x7F;
        if (nBytes === 0 || nBytes > 4) throw new Error('BER len: unsupported');
        var len = 0;
        for (var i = 0; i < nBytes; i++) len = (len << 8) | u8[off + 1 + i];
        return { len: len, hdrSize: 1 + nBytes };
    }

    // Write a BER length back at `off` using the SAME hdrSize it originally had.
    // Caller must have verified that the new length fits. Returns true on
    // success, false if the new length would require a different encoding size.
    function writeBerLen(u8, off, origHdrSize, newLen) {
        if (origHdrSize === 1) {
            if (newLen >= 0x80) return false;
            u8[off] = newLen;
            return true;
        }
        var nBytes = origHdrSize - 1;
        var max = Math.pow(256, nBytes) - 1;
        if (newLen > max) return false;
        u8[off] = 0x80 | nBytes;
        for (var i = 0; i < nBytes; i++) {
            u8[off + 1 + i] = (newLen >>> ((nBytes - 1 - i) * 8)) & 0xFF;
        }
        return true;
    }

    // PER length (as used in T.124 / GCC). Two-byte form if high bit set:
    //   byte0 high bit set  →  len = ((byte0 & 0x3F) << 8) | byte1   (2-byte)
    //   byte0 high bit clear → len = byte0                           (1-byte)
    function readPerLen(u8, off) {
        var b = u8[off];
        if ((b & 0x80) === 0) return { len: b, hdrSize: 1 };
        return { len: ((b & 0x3F) << 8) | u8[off + 1], hdrSize: 2 };
    }
    function writePerLen(u8, off, origHdrSize, newLen) {
        if (origHdrSize === 1) {
            if (newLen >= 0x80) return false;
            u8[off] = newLen;
            return true;
        }
        if (newLen > 0x3FFF) return false;
        u8[off]     = 0x80 | ((newLen >> 8) & 0x3F);
        u8[off + 1] = newLen & 0xFF;
        return true;
    }

    // Given a Connect Initial TPKT (Uint8Array), inject an rdpsnd channel
    // entry into the Client Network Data (CS_NET) block and patch every
    // length field that now spans additional bytes. Returns the modified
    // Uint8Array, or null on failure (in which case we pass the original).
    function injectRdpsndChannel(pdu) {
        try {
            // --- Sanity ---
            if (pdu.length < 20) return null;
            if (pdu[0] !== 0x03 || pdu[1] !== 0x00) return null;
            var tpktLen = (pdu[2] << 8) | pdu[3];
            if (tpktLen !== pdu.length) return null;
            if (pdu[4] !== 0x02 || pdu[5] !== 0xF0 || pdu[6] !== 0x80) return null;
            // BER Application 101 (Connect-Initial) tag
            if (pdu[7] !== 0x7F || pdu[8] !== 0x65) return null;

            // Track length fields to patch:
            //   { off, hdrSize, kind: 'tpkt'|'ber'|'per' }
            var lenFields = [];
            lenFields.push({ off: 2, hdrSize: 2, kind: 'tpkt' });

            // --- BER Application 101 outer length at offset 9 ---
            var berApp = readBerLen(pdu, 9);
            lenFields.push({ off: 9, hdrSize: berApp.hdrSize, kind: 'ber' });
            var bodyStart = 9 + berApp.hdrSize;
            var bodyEnd   = bodyStart + berApp.len;

            // --- Skip Connect-Initial fixed head fields:
            //      callingDomainSelector OCTET STRING
            //      calledDomainSelector  OCTET STRING
            //      upwardFlag            BOOLEAN
            //      targetParameters      SEQUENCE
            //      minimumParameters     SEQUENCE
            //      maximumParameters     SEQUENCE
            //      userData              OCTET STRING  ← we need this one
            function skipBerElement(off) {
                var tagSize = 1;
                var tag = pdu[off];
                if ((tag & 0x1F) === 0x1F) {
                    // multi-byte tag — advance until low bit cleared
                    tagSize = 1;
                    while (pdu[off + tagSize] & 0x80) tagSize++;
                    tagSize++;
                }
                var ln = readBerLen(pdu, off + tagSize);
                return off + tagSize + ln.hdrSize + ln.len;
            }

            var cur = bodyStart;
            // 6 fixed elements before userData
            for (var skip = 0; skip < 6; skip++) {
                cur = skipBerElement(cur);
                if (cur > bodyEnd) throw new Error('walk overran');
            }
            // Now cur points at userData OCTET STRING
            if (pdu[cur] !== 0x04) throw new Error('expected OCTET STRING at userData');
            var udLen = readBerLen(pdu, cur + 1);
            lenFields.push({ off: cur + 1, hdrSize: udLen.hdrSize, kind: 'ber' });
            var udStart = cur + 1 + udLen.hdrSize;

            // --- Inside userData: GCC Connect-Data header ---
            // Fixed bytes: 00 05 00 14 7C 00 01  (T.124 generic application identifier)
            var gccHdr = [0x00, 0x05, 0x00, 0x14, 0x7C, 0x00, 0x01];
            for (var g = 0; g < gccHdr.length; g++) {
                if (pdu[udStart + g] !== gccHdr[g]) throw new Error('bad GCC header');
            }
            // Then PER length of ConnectGCCPDU
            var gccLen = readPerLen(pdu, udStart + 7);
            lenFields.push({ off: udStart + 7, hdrSize: gccLen.hdrSize, kind: 'per' });
            var gccBodyStart = udStart + 7 + gccLen.hdrSize;

            // --- Inside ConnectGCCPDU, find ClientData user-data list.
            // Practical shortcut: scan forward for the h221Key "Duca" (44 75 63 61),
            // which marks the start of the client-to-server data block. The PER
            // length of this block's contents immediately follows "Duca".
            var Duca = [0x44, 0x75, 0x63, 0x61];
            var ducaOff = -1;
            for (var i = gccBodyStart; i < pdu.length - 4; i++) {
                if (pdu[i]   === Duca[0] && pdu[i+1] === Duca[1] &&
                    pdu[i+2] === Duca[2] && pdu[i+3] === Duca[3]) {
                    ducaOff = i; break;
                }
            }
            if (ducaOff < 0) throw new Error('Duca marker not found');
            var ducaLen = readPerLen(pdu, ducaOff + 4);
            lenFields.push({ off: ducaOff + 4, hdrSize: ducaLen.hdrSize, kind: 'per' });

            // --- Find the CS_NET block (type 0xC003 LE) inside Duca contents.
            // CS_NET header: [type=03 C0][len lo hi][count LE u32][entries]
            var csnetOff = -1;
            for (var j = ducaOff; j < pdu.length - 8; j++) {
                if (pdu[j] === 0x03 && pdu[j+1] === 0xC0) {
                    var bl = pdu[j+2] | (pdu[j+3] << 8);
                    if (bl >= 8 && bl <= 4096 && j + bl <= pdu.length) {
                        var cc = pdu[j+4] | (pdu[j+5] << 8) |
                                 (pdu[j+6] << 16) | (pdu[j+7] << 24);
                        if (cc >= 0 && cc <= 31 && bl === 8 + cc * 12) {
                            csnetOff = j; break;
                        }
                    }
                }
            }
            if (csnetOff < 0) throw new Error('CS_NET not found');

            var channelCount = pdu[csnetOff+4] | (pdu[csnetOff+5] << 8) |
                               (pdu[csnetOff+6] << 16) | (pdu[csnetOff+7] << 24);

            // Walk existing channels. Overwrite any "cliprdr" entry with
            // "rdpsnd" in place — that way we get audio AND we don't have
            // IronRDP's cliprdr state machine getting confused by the
            // extra channel (it can't reach Ready when there's a channel
            // it didn't register, and a copy-in-remote then kills the
            // session). If cliprdr is absent, we still append rdpsnd.
            var rdpsndExists = false;
            var cliprdrOff   = -1;
            for (var c = 0; c < channelCount; c++) {
                var base = csnetOff + 8 + c * 12;
                // rdpsnd = 72 64 70 73 6E 64
                if (pdu[base]   === 0x72 && pdu[base+1] === 0x64 &&
                    pdu[base+2] === 0x70 && pdu[base+3] === 0x73 &&
                    pdu[base+4] === 0x6E && pdu[base+5] === 0x64) {
                    rdpsndExists = true;
                }
                // cliprdr = 63 6C 69 70 72 64 72
                if (cliprdrOff < 0 &&
                    pdu[base]   === 0x63 && pdu[base+1] === 0x6C &&
                    pdu[base+2] === 0x69 && pdu[base+3] === 0x70 &&
                    pdu[base+4] === 0x72 && pdu[base+5] === 0x64 &&
                    pdu[base+6] === 0x72) {
                    cliprdrOff = base;
                }
            }

            if (rdpsndExists) return null;   // already injected on prior attempt

            // If cliprdr is present, rewrite that entry to rdpsnd in place.
            // Channel count and CS_NET length are unchanged → no other
            // lengths need patching; return the (in-place-modified) buffer
            // as "modified" so caller sets state.injected.
            if (cliprdrOff >= 0) {
                var snd = 'rdpsnd';
                for (var n = 0; n < 8; n++) {
                    pdu[cliprdrOff + n] = (n < snd.length) ? snd.charCodeAt(n) : 0;
                }
                // Options: CHANNEL_OPTION_INITIALIZED | ENCRYPT_RDP = 0xC0000000
                pdu[cliprdrOff + 8]  = 0x00;
                pdu[cliprdrOff + 9]  = 0x00;
                pdu[cliprdrOff + 10] = 0x00;
                pdu[cliprdrOff + 11] = 0xC0;
                console.log('[rdp-audio] replaced cliprdr channel with rdpsnd in CS_NET');
                return pdu;   // in-place; pdu IS u8's underlying bytes
            }

            var csnetLen = pdu[csnetOff+2] | (pdu[csnetOff+3] << 8);
            var insertAt = csnetOff + 8 + channelCount * 12;

            // Build new PDU with 12 extra bytes inserted at insertAt.
            var newPdu = new Uint8Array(pdu.length + 12);
            newPdu.set(pdu.subarray(0, insertAt), 0);
            // rdpsnd channel entry: 8-byte name + 4-byte options (LE)
            //   options = CHANNEL_OPTION_INITIALIZED (0x80000000)
            //           | CHANNEL_OPTION_ENCRYPT_RDP (0x40000000) = 0xC0000000
            var name = 'rdpsnd';
            for (var n = 0; n < name.length; n++) newPdu[insertAt + n] = name.charCodeAt(n);
            // padding 0 bytes already zero from Uint8Array init
            newPdu[insertAt + 8]  = 0x00;
            newPdu[insertAt + 9]  = 0x00;
            newPdu[insertAt + 10] = 0x00;
            newPdu[insertAt + 11] = 0xC0;
            newPdu.set(pdu.subarray(insertAt), insertAt + 12);

            // --- Patch CS_NET header fields in the new buffer ---
            //   channelCount += 1
            var newCount = channelCount + 1;
            newPdu[csnetOff+4] = newCount & 0xFF;
            newPdu[csnetOff+5] = (newCount >> 8) & 0xFF;
            newPdu[csnetOff+6] = (newCount >> 16) & 0xFF;
            newPdu[csnetOff+7] = (newCount >> 24) & 0xFF;
            //   block length += 12
            var newCsnetLen = csnetLen + 12;
            newPdu[csnetOff+2] = newCsnetLen & 0xFF;
            newPdu[csnetOff+3] = (newCsnetLen >> 8) & 0xFF;

            // --- Patch all enclosing length fields. Every length field sits
            //     at a position BEFORE the injection point (since we walked
            //     from outer → inner), so newPdu[fld.off..] currently holds
            //     the ORIGINAL pre-inject bytes. Read original, add 12, write.
            for (var f = 0; f < lenFields.length; f++) {
                var fld = lenFields[f];
                if (fld.kind === 'tpkt') {
                    var newTpkt = tpktLen + 12;
                    newPdu[fld.off]     = (newTpkt >> 8) & 0xFF;
                    newPdu[fld.off + 1] = newTpkt & 0xFF;
                }
                else if (fld.kind === 'ber') {
                    var lb = readBerLen(newPdu, fld.off);
                    if (!writeBerLen(newPdu, fld.off, fld.hdrSize, lb.len + 12))
                        throw new Error('BER length overflow (off ' + fld.off + ')');
                }
                else if (fld.kind === 'per') {
                    var lp = readPerLen(newPdu, fld.off);
                    if (!writePerLen(newPdu, fld.off, fld.hdrSize, lp.len + 12))
                        throw new Error('PER length overflow (off ' + fld.off + ')');
                }
            }

            console.log('[rdp-audio] injected rdpsnd into CS_NET: channels ' +
                        channelCount + '→' + newCount +
                        ', CS_NET ' + csnetLen + '→' + newCsnetLen +
                        ', PDU ' + pdu.length + '→' + newPdu.length);
            return newPdu;
        } catch (e) {
            console.log('[rdp-audio] injectRdpsndChannel failed:', e.message || e);
            return null;
        }
    }

    // Remove the cliprdr channel entry from CS_NET in a Connect Initial.
    // Used when audio is disabled — we still want to drop cliprdr because
    // IronRDP's cliprdr state machine deadlocks the session as soon as
    // copy/paste is attempted (error: "clipboard channel is not in Ready
    // state"), followed by a freeze. The same walker as injectRdpsndChannel
    // but in reverse: splice out 12 bytes and patch every enclosing length
    // by -12.
    //
    // Returns the modified Uint8Array on success, or null if the PDU isn't
    // a Connect Initial, or if cliprdr isn't present.
    function stripCliprdrChannel(pdu) {
        try {
            if (pdu.length < 20) return null;
            if (pdu[0] !== 0x03 || pdu[1] !== 0x00) return null;
            var tpktLen = (pdu[2] << 8) | pdu[3];
            if (tpktLen !== pdu.length) return null;
            if (pdu[4] !== 0x02 || pdu[5] !== 0xF0 || pdu[6] !== 0x80) return null;
            if (pdu[7] !== 0x7F || pdu[8] !== 0x65) return null;

            var lenFields = [];
            lenFields.push({ off: 2, hdrSize: 2, kind: 'tpkt' });
            var berApp = readBerLen(pdu, 9);
            lenFields.push({ off: 9, hdrSize: berApp.hdrSize, kind: 'ber' });
            var bodyStart = 9 + berApp.hdrSize;
            var bodyEnd   = bodyStart + berApp.len;

            function skipBerElement(off) {
                var tagSize = 1;
                var tag = pdu[off];
                if ((tag & 0x1F) === 0x1F) {
                    while (pdu[off + tagSize] & 0x80) tagSize++;
                    tagSize++;
                }
                var ln = readBerLen(pdu, off + tagSize);
                return off + tagSize + ln.hdrSize + ln.len;
            }

            var cur = bodyStart;
            for (var skip = 0; skip < 6; skip++) {
                cur = skipBerElement(cur);
                if (cur > bodyEnd) throw new Error('walk overran');
            }
            if (pdu[cur] !== 0x04) throw new Error('expected OCTET STRING at userData');
            var udLen = readBerLen(pdu, cur + 1);
            lenFields.push({ off: cur + 1, hdrSize: udLen.hdrSize, kind: 'ber' });
            var udStart = cur + 1 + udLen.hdrSize;

            var gccHdr = [0x00, 0x05, 0x00, 0x14, 0x7C, 0x00, 0x01];
            for (var g = 0; g < gccHdr.length; g++) {
                if (pdu[udStart + g] !== gccHdr[g]) throw new Error('bad GCC header');
            }
            var gccLen = readPerLen(pdu, udStart + 7);
            lenFields.push({ off: udStart + 7, hdrSize: gccLen.hdrSize, kind: 'per' });
            var gccBodyStart = udStart + 7 + gccLen.hdrSize;

            var Duca = [0x44, 0x75, 0x63, 0x61];
            var ducaOff = -1;
            for (var i = gccBodyStart; i < pdu.length - 4; i++) {
                if (pdu[i]   === Duca[0] && pdu[i+1] === Duca[1] &&
                    pdu[i+2] === Duca[2] && pdu[i+3] === Duca[3]) {
                    ducaOff = i; break;
                }
            }
            if (ducaOff < 0) throw new Error('Duca marker not found');
            var ducaLen = readPerLen(pdu, ducaOff + 4);
            lenFields.push({ off: ducaOff + 4, hdrSize: ducaLen.hdrSize, kind: 'per' });

            var csnetOff = -1;
            for (var j = ducaOff; j < pdu.length - 8; j++) {
                if (pdu[j] === 0x03 && pdu[j+1] === 0xC0) {
                    var bl = pdu[j+2] | (pdu[j+3] << 8);
                    if (bl >= 8 && bl <= 4096 && j + bl <= pdu.length) {
                        var cc = pdu[j+4] | (pdu[j+5] << 8) |
                                 (pdu[j+6] << 16) | (pdu[j+7] << 24);
                        if (cc >= 0 && cc <= 31 && bl === 8 + cc * 12) {
                            csnetOff = j; break;
                        }
                    }
                }
            }
            if (csnetOff < 0) throw new Error('CS_NET not found');

            var channelCount = pdu[csnetOff+4] | (pdu[csnetOff+5] << 8) |
                               (pdu[csnetOff+6] << 16) | (pdu[csnetOff+7] << 24);

            var cliprdrOff = -1;
            for (var c = 0; c < channelCount; c++) {
                var base = csnetOff + 8 + c * 12;
                if (pdu[base]   === 0x63 && pdu[base+1] === 0x6C &&
                    pdu[base+2] === 0x69 && pdu[base+3] === 0x70 &&
                    pdu[base+4] === 0x72 && pdu[base+5] === 0x64 &&
                    pdu[base+6] === 0x72) {
                    cliprdrOff = base;
                    break;
                }
            }
            if (cliprdrOff < 0) return null;   // nothing to do

            // If this was the last channel entry, we'd end up with a 0-count
            // CS_NET block. xrdp probably still accepts that, but erring on
            // the side of caution we bail — the caller then just sends the
            // PDU through untouched and IronRDP handles cliprdr on its own.
            if (channelCount <= 1) return null;

            var csnetLen = pdu[csnetOff+2] | (pdu[csnetOff+3] << 8);
            var removeAt = cliprdrOff;

            // Build new PDU with 12 bytes removed at removeAt.
            var newPdu = new Uint8Array(pdu.length - 12);
            newPdu.set(pdu.subarray(0, removeAt), 0);
            newPdu.set(pdu.subarray(removeAt + 12), removeAt);

            // Patch CS_NET header: count-1, length-12.
            var newCount = channelCount - 1;
            newPdu[csnetOff+4] = newCount & 0xFF;
            newPdu[csnetOff+5] = (newCount >> 8) & 0xFF;
            newPdu[csnetOff+6] = (newCount >> 16) & 0xFF;
            newPdu[csnetOff+7] = (newCount >> 24) & 0xFF;
            var newCsnetLen = csnetLen - 12;
            newPdu[csnetOff+2] = newCsnetLen & 0xFF;
            newPdu[csnetOff+3] = (newCsnetLen >> 8) & 0xFF;

            // Patch every enclosing length by -12.
            for (var f = 0; f < lenFields.length; f++) {
                var fld = lenFields[f];
                if (fld.kind === 'tpkt') {
                    var newTpkt = tpktLen - 12;
                    newPdu[fld.off]     = (newTpkt >> 8) & 0xFF;
                    newPdu[fld.off + 1] = newTpkt & 0xFF;
                }
                else if (fld.kind === 'ber') {
                    var lb = readBerLen(newPdu, fld.off);
                    if (!writeBerLen(newPdu, fld.off, fld.hdrSize, lb.len - 12))
                        throw new Error('BER length underflow (off ' + fld.off + ')');
                }
                else if (fld.kind === 'per') {
                    var lp = readPerLen(newPdu, fld.off);
                    if (!writePerLen(newPdu, fld.off, fld.hdrSize, lp.len - 12))
                        throw new Error('PER length underflow (off ' + fld.off + ')');
                }
            }

            console.log('[rdp-audio] stripped cliprdr from CS_NET: channels ' +
                        channelCount + '→' + newCount +
                        ', CS_NET ' + csnetLen + '→' + newCsnetLen +
                        ', PDU ' + pdu.length + '→' + newPdu.length);
            return newPdu;
        } catch (e) {
            console.log('[rdp-audio] stripCliprdrChannel failed:', e.message || e);
            return null;
        }
    }

    // Scan an MCS Connect Response for its SC_NET block and return the
    // last channel ID listed — that's our injected rdpsnd. (We leave
    // the response unmodified; xrdp verifies the client joins every
    // channel it advertised, so we keep rdpsnd visible to WASM.)
    function parseConnectResponseRdpsndIdAndStrip(pdu) {
        try {
            // Validate TPKT → X.224 Data → BER Application 102 (Connect Response)
            if (pdu[0] !== 0x03 || pdu[1] !== 0x00) return 0;
            if (pdu[4] !== 0x02 || pdu[5] !== 0xF0 || pdu[6] !== 0x80) return 0;
            if (pdu[7] !== 0x7F || pdu[8] !== 0x66) return 0;

            // SC_NET header: 03 0C LL LL  (type=0x0C03 LE, length=LL LL LE)
            // Followed by: MCSChannelId (u16 LE, I/O channel), channelCount (u16 LE),
            // then channelCount × 2 bytes of channel IDs (LE), then optional padding
            // to 4-byte alignment.
            for (var i = 10; i < pdu.length - 8; i++) {
                if (pdu[i] === 0x03 && pdu[i+1] === 0x0C) {
                    var blockLen = pdu[i+2] | (pdu[i+3] << 8);
                    if (blockLen < 8 || i + blockLen > pdu.length) continue;
                    var count = pdu[i+6] | (pdu[i+7] << 8);
                    // Expected size: 4 hdr + 2 IOChan + 2 count + 2*count + pad
                    var expectedNoPad = 8 + 2 * count;
                    var expectedWithPad = expectedNoPad + 2;
                    if (blockLen !== expectedNoPad && blockLen !== expectedWithPad) continue;
                    // Sanity: count in reasonable range for RDP (≤ 31 static channels)
                    if (count < 1 || count > 31) continue;
                    // Last channel ID — that's our injected rdpsnd
                    var lastIdOff = i + 8 + (count - 1) * 2;
                    var lastId = pdu[lastIdOff] | (pdu[lastIdOff + 1] << 8);
                    // Don't strip it — xrdp expects the client to join
                    // every channel it advertised, and stripping causes
                    // a DisconnectProviderUltimatum.
                    return lastId;
                }
            }
        } catch (e) {
            console.log('[rdp-audio] parseConnectResponseRdpsndId error', e);
        }
        return 0;
    }

    // Patch the INFO_AUTOLOGON (0x00000008) flag into a Client Info PDU
    // sent from client → server. IronRDP's SessionBuilder has a password()
    // method but no API to set the AUTOLOGON flag, so when credentials are
    // supplied the server (xrdp) receives them but still prompts — it pre-
    // fills the username, then waits for the user to type the password.
    //
    // Structure after enhanced (TLS) security:
    //   TPKT (4) + X.224 Data (3) + MCS Send Data Request header
    //   payload = Security Header (4 LE: flags, flagsHi) + Info Packet
    //   Info Packet = CodePage(4) + flags(4 LE) + cbDomain(2) + cbUserName(2)
    //                 + cbPassword(2) + ... + strings
    //
    // Only patch when the Security Header has SEC_INFO_PKT (0x0040) and the
    // Info Packet has cbPassword > 0 (no point requesting auto-login with a
    // blank password — xrdp would just re-prompt anyway).
    //
    // Returns true if the PDU was the Client Info PDU and we patched it
    // (so the caller can stop scanning future sends).
    function tryPatchClientInfoAutologon(u8) {
        try {
            if (u8.length < 20) return false;
            if (u8[0] !== 0x03 || u8[1] !== 0x00) return false;
            if (u8[4] !== 0x02 || u8[5] !== 0xF0 || u8[6] !== 0x80) return false;
            if (u8[7] !== 0x64) return false;   // MCS Send Data Request

            // MCS header: 0x64 init(2) chan(2) pri(1) perLen(1 or 2)
            var perLenFirst = u8[13];
            var payloadOff = (perLenFirst & 0x80) ? 15 : 14;
            if (payloadOff + 22 > u8.length) return false;

            // Security Header flags (LE u16) — must have SEC_INFO_PKT.
            var secFlags = u8[payloadOff] | (u8[payloadOff + 1] << 8);
            if ((secFlags & 0x0040) === 0) return false;   // not Info PDU

            // SEC_ENCRYPT (0x0008) would mean Standard RDP Security encrypted
            // the Info Packet — we can't patch that. TLS path has no encrypt.
            if (secFlags & 0x0008) return false;

            var ipOff = payloadOff + 4;   // skip security header (4 bytes)
            // Info Packet: CodePage(4) | flags(4 LE) | cbDomain(2) | cbUser(2)
            //              | cbPass(2) | cbAltShell(2) | cbWorkDir(2) | ...
            var cbPassword = u8[ipOff + 12] | (u8[ipOff + 13] << 8);
            if (cbPassword <= 0) return false;   // no password → don't set

            var flagsByteOff = ipOff + 4;   // low byte of LE u32 flags
            var before = u8[flagsByteOff]       |
                        (u8[flagsByteOff + 1] << 8) |
                        (u8[flagsByteOff + 2] << 16) |
                        (u8[flagsByteOff + 3] << 24);

            u8[flagsByteOff] |= 0x08;   // INFO_AUTOLOGON

            var after = before | 0x08;
            console.log('[rdp-audio] Client Info PDU: set AUTOLOGON flag ' +
                        '(flags 0x' + (before >>> 0).toString(16) +
                        ' → 0x' + (after >>> 0).toString(16) +
                        ', cbPassword=' + cbPassword + ')');
            return true;
        } catch (e) {
            console.log('[rdp-audio] tryPatchClientInfoAutologon error', e);
            return false;
        }
    }

    // Returns true if u8 is a TPKT/X.224 MCS Send Data Indication on the
    // given channel ID. Used to filter audio-channel payloads from what
    // the WASM sees. We deliberately DO NOT match Channel Join Confirm:
    // the WASM joins all channels in SC_NET and must receive its own
    // Confirm PDUs for the joins to complete.
    function isSdiForChannel(u8, channelId) {
        if (u8.length < 12) return false;
        if (u8[0] !== 0x03 || u8[1] !== 0x00) return false;
        if (u8[5] !== 0xF0) return false;   // X.224 Data PDU
        if (u8[7] !== 0x68) return false;   // MCS Send Data Indication
        var id = (u8[10] << 8) | u8[11];
        return id === channelId;
    }

    // ── RDPSND stream dispatcher ──────────────────────────────────
    // Called for each MCS Send-Data-Indication on the rdpsnd channel.
    // Accumulates VC-PDU fragments, then dispatches by RDPSND msgType
    // or treats the data as Wave-PDU continuation of a preceding WaveInfo.
    function processRdpsndSdi(state, u8) {
        // Find MCS userData offset: header is 14 bytes if len byte < 0x80,
        // else 15 bytes (2-byte PER length).
        var mcsUdOff;
        if (u8[13] < 0x80) mcsUdOff = 14;
        else mcsUdOff = 15;

        // Virtual Channel PDU header: length (u32 LE) + flags (u32 LE)
        var vcLen   = u8[mcsUdOff]     |
                     (u8[mcsUdOff + 1] << 8) |
                     (u8[mcsUdOff + 2] << 16) |
                     (u8[mcsUdOff + 3] << 24);
        var vcFlags = u8[mcsUdOff + 4] |
                     (u8[mcsUdOff + 5] << 8) |
                     (u8[mcsUdOff + 6] << 16) |
                     (u8[mcsUdOff + 7] << 24);

        var vcDataStart = mcsUdOff + 8;
        var vcData = u8.subarray(vcDataStart);

        // Accumulate fragments (FIRST without LAST → start; continuing → append;
        // LAST set → complete and dispatch).
        if (vcFlags & 0x01) state.vcBuf = null;   // FIRST: start fresh
        if (!state.vcBuf) state.vcBuf = new Uint8Array(0);

        // Append vcData to state.vcBuf
        var newBuf = new Uint8Array(state.vcBuf.length + vcData.length);
        newBuf.set(state.vcBuf, 0);
        newBuf.set(vcData, state.vcBuf.length);
        state.vcBuf = newBuf;

        if (!(vcFlags & 0x02)) return;   // not LAST — wait for more fragments
        var pdu = state.vcBuf;
        state.vcBuf = null;

        // Dispatch: wave continuation or new RDPSND PDU with SNDPROLOG
        if (state.waveRemaining > 0) {
            // Wave PDU: starts with 4 bytes of padding, then audio data.
            if (pdu.length < 4) return;
            var wavePayload = pdu.subarray(4);
            var take = Math.min(wavePayload.length, state.waveRemaining);
            handleWaveData(state, wavePayload.subarray(0, take));
            state.waveRemaining -= take;
            if (state.waveRemaining === 0) {
                // Block complete. Rate-limit confirms: confirming every
                // block generates enough traffic to visibly slow video.
                // Protocol-wise we only need to ack often enough that the
                // server's ~256-block output buffer doesn't fill; acking
                // block N also implicitly confirms all blocks < N.
                state.blocksSinceConfirm = (state.blocksSinceConfirm || 0) + 1;
                var N = state.confirmEvery || 32;   // ~1.5s of audio per confirm
                if (state.blocksSinceConfirm >= N) {
                    sendWaveConfirm(state, state.currentTimestamp, state.currentBlockNo);
                    state.blocksSinceConfirm = 0;
                }
            }
            return;
        }

        // New PDU with SNDPROLOG (4 bytes): msgType, bPad, bodySize (LE u16)
        if (pdu.length < 4) return;
        var msgType = pdu[0];
        var bodySize = pdu[2] | (pdu[3] << 8);
        var body = pdu.subarray(4);

        state.rdpsndPduCount = (state.rdpsndPduCount || 0) + 1;

        switch (msgType) {
            case 0x07:  // SNDC_FORMATS — Server Audio Formats
                handleServerFormats(state, body);
                break;
            case 0x02:  // SNDC_WAVE — WaveInfo (starts a wave transfer)
                handleWaveInfo(state, body, bodySize);
                break;
            case 0x06:  // SNDC_TRAINING
                // TODO: send Training Confirm
                console.log('[rdp-audio] Training PDU, no response sent yet');
                break;
            case 0x0C:  // SNDC_QUALITYMODE
                console.log('[rdp-audio] Quality Mode PDU, ignored');
                break;
            case 0x0D:  // SNDC_WAVE2
                handleWave2(state, body, bodySize);
                break;
            default:
                // Unknown (e.g. 0x27 seen on xrdp). Ignore.
                break;
        }
    }

    function handleServerFormats(state, body) {
        // Server Formats PDU body layout:
        //   dwFlags (4), dwVolume (4), dwPitch (4), wDGramPort (2),
        //   wNumberOfFormats (2), cLastBlockConfirmed (1),
        //   wVersion (2), bPad (1)   — 20 bytes fixed header
        //   followed by wNumberOfFormats × AUDIO_FORMAT (18+ bytes each)
        if (body.length < 20) return;
        var nFormats = body[16] | (body[17] << 8);
        var version  = body[19] | (body[20] << 8);
        console.log('[rdp-audio] ServerFormats: nFormats=' + nFormats +
                    ' version=0x' + version.toString(16));
        var off = 20;
        var formats = [];
        for (var i = 0; i < nFormats && off + 18 <= body.length; i++) {
            var tag    = body[off]   | (body[off+1] << 8);
            var ch     = body[off+2] | (body[off+3] << 8);
            var rate   = body[off+4] | (body[off+5] << 8) | (body[off+6] << 16) | (body[off+7] << 24);
            var avgBps = body[off+8] | (body[off+9] << 8) | (body[off+10] << 16) | (body[off+11] << 24);
            var blkAln = body[off+12]| (body[off+13] << 8);
            var bits   = body[off+14]| (body[off+15] << 8);
            var cbSize = body[off+16]| (body[off+17] << 8);
            formats.push({
                index: i, tag: tag, channels: ch, sampleRate: rate,
                avgBps: avgBps, blockAlign: blkAln, bitsPerSample: bits, extra: cbSize
            });
            console.log('  fmt[' + i + '] tag=0x' + tag.toString(16) +
                        ' ch=' + ch + ' rate=' + rate +
                        ' bits=' + bits + ' extra=' + cbSize);
            off += 18 + cbSize;
        }
        state.serverFormats = formats;
        // TODO: respond with Client Formats PDU accepting PCM formats.
    }

    function handleWaveInfo(state, body, bodySize) {
        // WaveInfo body: wTimeStamp(2) wFormatNo(2) cBlockNo(1) bPad(3) Data(4)
        if (body.length < 12) return;
        var timestamp = body[0] | (body[1] << 8);
        var formatNo  = body[2] | (body[3] << 8);
        var blockNo   = body[4];
        var firstData = body.subarray(8, 12);

        state.currentFormatNo  = formatNo;
        state.currentBlockNo   = blockNo;
        state.currentTimestamp = timestamp;
        // bodySize from WaveInfo = bytes after SNDPROLOG, i.e. WaveInfo body
        //   (12 bytes) + subsequent Wave PDU payload(s)
        state.waveRemaining = bodySize - 12;

        state.rdpsndWaveCount = (state.rdpsndWaveCount || 0) + 1;
        handleWaveData(state, firstData);
    }

    // Send a Wave Confirm PDU (SNDC_WAVECONFIRM) back to the server
    // acknowledging the last Wave block. Without this, xrdp's pulse module
    // fills its output buffer (~2 MiB, 256 blocks) and stops sending.
    //
    // PDU structure:
    //   TPKT (4 bytes: 03 00 len-hi len-lo)
    //   X.224 Data (3 bytes: 02 F0 80)
    //   MCS Send Data Request:
    //     tag (1): 0x64
    //     initiator (2 BE): userChannelId - 1001
    //     channel   (2 BE): rdpsnd channel id
    //     priority  (1):    0x70
    //     per-length(1):    payload length (1-byte form)
    //   Virtual Channel PDU header (8 bytes): length (LE), flags = 0x03
    //   RDPSND SNDC_WAVECONFIRM (8 bytes):
    //     msgType=0x05, bPad=0, BodySize=4,
    //     wTimeStamp (2 LE), cConfirmedBlockNo (1), bPad (1)
    function sendWaveConfirm(state, timestamp, blockNo) {
        if (!state.rdpsndChannelId) return;

        var rdpsnd = new Uint8Array(8);
        rdpsnd[0] = 0x05;  rdpsnd[1] = 0x00;
        rdpsnd[2] = 0x04;  rdpsnd[3] = 0x00;    // BodySize = 4
        rdpsnd[4] = timestamp & 0xFF;
        rdpsnd[5] = (timestamp >> 8) & 0xFF;
        rdpsnd[6] = blockNo & 0xFF;
        rdpsnd[7] = 0x00;

        var vc = new Uint8Array(16);
        vc[0] = 0x08; vc[1] = 0; vc[2] = 0; vc[3] = 0;  // VC length = 8
        vc[4] = 0x03; vc[5] = 0; vc[6] = 0; vc[7] = 0;  // flags = FIRST|LAST
        vc.set(rdpsnd, 8);

        var userOff = (state.userChannelId || 1002) - 1001;
        var chanId  = state.rdpsndChannelId;
        var mcs = new Uint8Array(7 + vc.length);
        mcs[0] = 0x64;                             // Send Data Request
        mcs[1] = (userOff >> 8) & 0xFF;
        mcs[2] = userOff & 0xFF;
        mcs[3] = (chanId >> 8) & 0xFF;
        mcs[4] = chanId & 0xFF;
        mcs[5] = 0x70;                             // priority/segmentation
        mcs[6] = vc.length;                        // PER length (fits in 1 byte)
        mcs.set(vc, 7);

        var total = 4 + 3 + mcs.length;
        var pdu = new Uint8Array(total);
        pdu[0] = 0x03; pdu[1] = 0x00;
        pdu[2] = (total >> 8) & 0xFF;
        pdu[3] = total & 0xFF;
        pdu[4] = 0x02; pdu[5] = 0xF0; pdu[6] = 0x80;
        pdu.set(mcs, 7);

        try {
            state.origSend(pdu);
            state.waveConfirmsSent = (state.waveConfirmsSent || 0) + 1;
        } catch (e) {
            console.log('[rdp-audio] sendWaveConfirm failed:', e);
        }
    }

    function handleWave2(state, body, bodySize) {
        // Wave2 is the newer PDU (version 6+). Layout similar to WaveInfo
        // but with dwAudioTimeStamp (4) instead of wTimeStamp/bPad.
        // For now log, don't decode.
        console.log('[rdp-audio] Wave2 body=' + body.length + 'B (not decoded)');
    }

    function handleWaveData(state, data) {
        state.audioBytes = (state.audioBytes || 0) + data.length;

        // Accumulate raw bytes into pending buffer, then flush into
        // AudioBuffer blocks whenever we have enough.
        if (!state.pcmPending) state.pcmPending = [];
        state.pcmPending.push(new Uint8Array(data));
        state.pcmPendingLen = (state.pcmPendingLen || 0) + data.length;

        // Flush when we've accumulated enough for ~250 ms of audio. That
        // reduces the number of AudioBufferSourceNodes by ~5× vs flushing
        // every wave block, which relieves main-thread pressure from node
        // creation / GC (which was starving the video render loop).
        var FLUSH_BYTES = 44100 * 4 / 4;   // 250 ms of stereo 16-bit PCM ≈ 44100 B
        if (state.pcmPendingLen >= FLUSH_BYTES) {
            flushPcmBlock(state);
        }
    }

    function flushPcmBlock(state) {
        if (state.torndown) return;
        if (!state.pcmPendingLen) return;
        // Concatenate pending chunks.
        var merged = new Uint8Array(state.pcmPendingLen);
        var off = 0;
        for (var i = 0; i < state.pcmPending.length; i++) {
            merged.set(state.pcmPending[i], off);
            off += state.pcmPending[i].length;
        }
        state.pcmPending = [];
        state.pcmPendingLen = 0;

        // Interpret as 16-bit LE signed PCM, 2 channels. Count samples/channel.
        //   bytesPerFrame = 4 (2 bytes × 2 channels).
        var nFrames = (merged.length - (merged.length % 4)) / 4;
        if (nFrames === 0) return;

        // Create AudioContext on first block (requires user gesture to
        // start; our canvas click satisfies that). We insert a master
        // GainNode between every source and the destination so teardown
        // can silence everything by setting gain to 0 — faster and more
        // reliable than trying to stop() every scheduled source.
        if (!state.audioCtx) {
            try {
                state.audioCtx = new (window.AudioContext || window.webkitAudioContext)({
                    sampleRate: 44100
                });
                state.masterGain = state.audioCtx.createGain();
                state.masterGain.gain.value = 1;
                state.masterGain.connect(state.audioCtx.destination);
                state.audioNextTime = state.audioCtx.currentTime + 0.1;  // 100 ms jitter buffer
                state.audioBlocksPlayed = 0;
                console.log('[rdp-audio] AudioContext created, state=' + state.audioCtx.state +
                            ' sampleRate=' + state.audioCtx.sampleRate);
                if (state.audioCtx.state === 'suspended') {
                    console.log('[rdp-audio] AudioContext suspended — will resume on next user click');
                }
                // Capture ctx locally — teardown nulls state.audioCtx before
                // calling suspend/close, which fires this very event.
                (function(c) {
                    c.addEventListener('statechange', function() {
                        console.log('[rdp-audio] AudioContext state → ' + c.state);
                    });
                })(state.audioCtx);
            } catch(e) {
                console.log('[rdp-audio] AudioContext create failed:', e);
                return;
            }
        }
        var ctx = state.audioCtx;

        // Build a stereo Float32 AudioBuffer at the context's sample rate.
        // Using Int16Array view + multiply by 1/32768 is ~4× faster than
        // DataView.getInt16 + divide, which matters because we run this
        // loop for every audio block on the main thread.
        var buffer = ctx.createBuffer(2, nFrames, ctx.sampleRate);
        var left  = buffer.getChannelData(0);
        var right = buffer.getChannelData(1);
        var i16 = new Int16Array(merged.buffer, merged.byteOffset, nFrames * 2);
        var SCALE = 1 / 32768;
        for (var f = 0; f < nFrames; f++) {
            left[f]  = i16[f * 2]     * SCALE;
            right[f] = i16[f * 2 + 1] * SCALE;
        }

        // Schedule playback. Only guard against underrun (lead < 0) — we
        // must never schedule blocks with overlapping time ranges, which
        // a "ran ahead" resync would do (jumping audioNextTime backward
        // into the region where earlier blocks are already queued). If
        // the server bursts data at us, we just buffer it as scheduled
        // AudioBufferSourceNodes; Web Audio plays them cleanly in order.
        var now = ctx.currentTime;
        var lead = state.audioNextTime - now;
        if (lead < 0.01) {
            console.log('[rdp-audio] audio underrun (lead=' +
                        lead.toFixed(3) + 's) — resync forward');
            state.audioNextTime = now + 0.05;
        }
        var src = ctx.createBufferSource();
        src.buffer = buffer;
        src.connect(state.masterGain || ctx.destination);
        var thisBlock = state.audioBlocksPlayed;
        if (!state.liveSources) state.liveSources = [];
        state.liveSources.push(src);
        src.onended = function() {
            // Release the node so GC can reclaim it and its buffer.
            try { src.disconnect(); } catch(_) {}
            var idx = state.liveSources ? state.liveSources.indexOf(src) : -1;
            if (idx >= 0) state.liveSources.splice(idx, 1);
            state.audioBlocksEnded = (state.audioBlocksEnded || 0) + 1;
        };
        try {
            src.start(state.audioNextTime);
        } catch (e) {
            console.log('[rdp-audio] src.start threw at block #' + thisBlock + ':', e);
        }
        state.audioNextTime += buffer.duration;
        state.audioBlocksPlayed++;

        // Very sparse status — only every ~1000 scheduled (~every 4 min).
        if ((state.audioBlocksPlayed % 1000) === 0) {
            console.log('[rdp-audio] scheduled=' + state.audioBlocksPlayed +
                        ' ended=' + (state.audioBlocksEnded || 0) +
                        ' lead=' + (state.audioNextTime - now).toFixed(3) + 's');
        }
    }

    // Stop and release all audio resources associated with the session.
    // Any BufferSourceNodes already scheduled via src.start(future_time)
    // will keep playing after the WebSocket closes unless we explicitly
    // stop them. We also disable further incoming-audio processing so no
    // new sources get scheduled after teardown.
    function teardownAudio(state) {
        console.log('[rdp-audio] teardownAudio called' +
                    ' (ctx=' + !!state.audioCtx +
                    ', sources=' + (state.liveSources ? state.liveSources.length : 0) + ')');
        // Disable the rdpsnd path so any message that arrives during
        // teardown (or after) doesn't create fresh AudioBufferSourceNodes.
        state.audioOn = false;
        state.torndown = true;
        state.pcmPending = [];
        state.pcmPendingLen = 0;

        // First, silence the master gain. Any source still connected
        // through it is inaudible immediately — no scheduling tricks
        // needed. We also disconnect the gain from the destination as a
        // belt-and-braces measure in case a source somehow bypassed it.
        if (state.masterGain) {
            try { state.masterGain.gain.value = 0; } catch(_) {}
            try { state.masterGain.disconnect(); } catch(_) {}
            state.masterGain = null;
        }

        var ctx = state.audioCtx;
        if (!ctx) return;
        state.audioCtx = null;

        var live = state.liveSources || [];
        state.liveSources = [];
        for (var i = 0; i < live.length; i++) {
            try { live[i].stop(0); } catch(_) {}
            try { live[i].disconnect(); } catch(_) {}
        }

        try { ctx.suspend(); } catch(_) {}
        try { ctx.close(); } catch(_) {}
    }

    function hex(u8, n) {
        n = Math.min(n || 32, u8.length);
        var out = '';
        for (var i = 0; i < n; i++) {
            var h = u8[i].toString(16);
            out += (h.length < 2 ? '0' : '') + h + ' ';
        }
        if (u8.length > n) out += '…';
        return out.trim();
    }

    function classifyOutgoing(u8, state) {
        // Our first outgoing message is the RDCleanPath Request (a DER
        // SEQUENCE starting 0x30). After that, we expect RDP PDUs in
        // TPKT framing (starting 0x03 0x00).
        if (u8[0] === 0x30) return 'rdcleanpath-req';
        if (u8[0] === 0x03 && u8[1] === 0x00) {
            // TPKT → X.224 Data → MCS PDU
            // At offset 7 is the MCS tag
            if (u8.length > 7) {
                var mcs = u8[7];
                if (mcs === 0x7f && u8[8] === 0x65) return 'mcs-connect-initial'; // BER app 101
                if (mcs === 0x04) return 'mcs-erect-domain';
                if (mcs === 0x28) return 'mcs-attach-user-req';
                if (mcs === 0x38) return 'mcs-channel-join-req';
                if (mcs === 0x64) return 'mcs-send-data-req';
                return 'mcs-other(tag=0x' + mcs.toString(16) + ')';
            }
            return 'tpkt-short';
        }
        return 'unknown(0x' + u8[0].toString(16) + ')';
    }

    function classifyIncoming(u8, state) {
        if (u8[0] === 0x30) return 'rdcleanpath-resp';
        if (u8[0] === 0x03 && u8[1] === 0x00) {
            if (u8.length > 7) {
                var mcs = u8[7];
                if (mcs === 0x7f && u8[8] === 0x66) return 'mcs-connect-response';
                if (mcs === 0x2e) return 'mcs-attach-user-confirm';
                if (mcs === 0x3e) return 'mcs-channel-join-confirm';
                if (mcs === 0x68) return 'mcs-send-data-ind';
                return 'mcs-other(tag=0x' + mcs.toString(16) + ')';
            }
            return 'tpkt-short';
        }
        // Fast-Path Output: low 2 bits of byte 0 == 0
        if ((u8[0] & 0x03) === 0) return 'fastpath-out';
        return 'unknown(0x' + u8[0].toString(16) + ')';
    }

    function wrapWebSocket(ws, opts) {
        opts = opts || {};
        // Default: audio on. When false, we skip CS_NET rdpsnd injection
        // and inbound audio-channel processing; we still patch AUTOLOGON
        // on the Client Info PDU so credential auto-login still works.
        var audioOn = (opts.audio !== false);

        // Escape hatch: set localStorage.rdpAudioDisabled = '1' and reload
        // to force-disable the audio path regardless of dialog choice.
        if (localStorage.getItem('rdpAudioDisabled') === '1') {
            console.log('[rdp-audio] audio DISABLED via localStorage.rdpAudioDisabled');
            audioOn = false;
        }

        var origSend = ws.send.bind(ws);
        var origAddListener = ws.addEventListener.bind(ws);
        var origRemoveListener = ws.removeEventListener.bind(ws);

        var state = {
            outCount:       0,
            inCount:        0,
            phase:          'cleanpath',  // cleanpath | mcs-setup | relay
            audioOn:        audioOn,
            // `injected` is our one-shot flag for "we've touched CS_NET":
            //   audio on  → inject rdpsnd (or replace cliprdr with it)
            //   audio off → strip cliprdr (or leave alone if absent)
            // In either case we only inspect the Connect Initial once.
            injected:       false,
            clientInfoPatched: false,     // AUTOLOGON flag set on Client Info PDU
            rdpsndChannelId: null,        // learned from Connect Response
            userChannelId:   1002,        // default; RDP user channel
            rdpsndJoined:    false,
            origSend:        origSend     // for sending WaveConfirm etc.
        };

        // Use the NATIVE event machinery for message delivery to WASM.
        // We attach a capture-phase listener that inspects each message,
        // and if it's a rdpsnd payload we handle it and call
        // stopImmediatePropagation() so WASM's own listener never runs.
        // For everything else (video fastpath, SDIs on other channels,
        // control traffic) we do NOTHING — native dispatch takes over
        // without any synthesized-event overhead.

        // Outgoing data is a Blob when IronRDP sends. We only need to
        // inspect/modify ONE send (the MCS Connect Initial where we
        // inject rdpsnd). After that, pass everything through directly
        // to keep the input/video-ack path unblocked.
        var sendQ = [];
        var draining = false;

        async function drainOnce() {
            if (draining) return;
            draining = true;
            try {
                while (sendQ.length) {
                    var job = sendQ.shift();
                    try {
                        var u8 = null;
                        if (job.data instanceof Blob) {
                            var ab = await job.data.arrayBuffer();
                            u8 = new Uint8Array(ab);
                        } else if (job.data instanceof ArrayBuffer) {
                            u8 = new Uint8Array(job.data);
                        } else if (ArrayBuffer.isView(job.data)) {
                            u8 = new Uint8Array(job.data.buffer, job.data.byteOffset, job.data.byteLength);
                        } else {
                            origSend(job.data);
                            continue;
                        }

                        var kind = classifyOutgoing(u8, state);
                        var outBytes = u8;
                        if (!state.injected && kind === 'mcs-connect-initial') {
                            var modified = state.audioOn
                                ? injectRdpsndChannel(u8)
                                : stripCliprdrChannel(u8);
                            if (modified) {
                                outBytes = modified;
                                console.log('[rdp-audio] OUT ' + outBytes.length +
                                            'B mcs-connect-initial [' +
                                            (state.audioOn ? 'rdpsnd injected' : 'cliprdr stripped') + ']');
                            }
                            // Mark injected=true even on null return (cliprdr
                            // absent, or walker bailed): we only scan the
                            // Connect Initial once regardless.
                            state.injected = true;
                        } else if (!state.clientInfoPatched && kind === 'mcs-send-data-req') {
                            if (tryPatchClientInfoAutologon(u8)) {
                                state.clientInfoPatched = true;
                            }
                        }
                        origSend(outBytes);
                    } catch (e) {
                        console.log('[rdp-audio] OUT drain error', e);
                    }
                }
            } finally {
                draining = false;
            }
        }

        ws.send = function(data) {
            state.outCount++;
            // Fast path: once both our one-shot patches (CS_NET rdpsnd and
            // Client Info AUTOLOGON) have been applied, every subsequent
            // send is forwarded verbatim to avoid the async Blob-read
            // microtask delay, which otherwise starves input/ack traffic
            // during streaming audio+video.
            if (state.injected && state.clientInfoPatched) {
                origSend(data);
                return;
            }
            sendQ.push({ data: data, seq: state.outCount });
            drainOnce();
        };

        origAddListener('close', function(e) {
            console.log('[rdp-audio] ws close code=' + e.code + ' clean=' + e.wasClean);
            teardownAudio(state);
        });
        origAddListener('error', function() {
            console.log('[rdp-audio] ws error');
        });

        // Capture-phase listener: we look at every message FIRST and
        // either stop it (for rdpsnd traffic) or let it flow natively to
        // WASM's own listener. For the first 20 messages we log for
        // diagnostics; after that the handler is cheap.
        origAddListener('message', function(e) {
            // When audio is disabled there is no rdpsnd channel to watch
            // for and no SDIs to absorb — everything flows native to WASM.
            if (!state.audioOn) return;

            state.inCount++;
            var d = e.data;
            if (!(d instanceof ArrayBuffer)) return;   // let WASM handle strings

            var u8 = new Uint8Array(d);

            // Identify rdpsnd channel from the first Connect Response —
            // and strip rdpsnd from SC_NET so WASM's cliprdr state
            // machine only sees the channels it asked for.
            if (!state.rdpsndChannelId) {
                var kind = classifyIncoming(u8, state);
                if (state.inCount <= 20) {
                    console.log('[rdp-audio] IN  #' + state.inCount + ' ' + u8.length +
                                'B ' + kind + ': ' + hex(u8, 24));
                }
                if (kind === 'mcs-connect-response') {
                    var id = parseConnectResponseRdpsndIdAndStrip(u8);
                    if (id) {
                        state.rdpsndChannelId = id;
                        console.log('[rdp-audio] rdpsnd channel id = ' + id +
                                    ' (stripped from SC_NET seen by WASM)');
                    }
                }
                return;   // pass through (possibly modified in place)
            }

            // After rdpsnd channel is known, only inspect messages that
            // could be rdpsnd SDIs (TPKT + X.224 Data, byte 0x03).
            // Fast-path output starts with !0x03 and is pure video — skip.
            if (u8[0] !== 0x03) return;

            if (isSdiForChannel(u8, state.rdpsndChannelId)) {
                state.rdpsndAbsorbCount = (state.rdpsndAbsorbCount || 0) + 1;
                try {
                    processRdpsndSdi(state, u8);
                } catch (ex) {
                    console.log('[rdp-audio] processRdpsndSdi threw:', ex,
                                'at absorb #' + state.rdpsndAbsorbCount);
                }
                e.stopImmediatePropagation();   // don't deliver to WASM
            }
        }, true);   // capture phase: we see events before WASM's listener

        // Resume suspended AudioContext on any user gesture — browsers
        // block audio playback until the user has interacted.
        function tryResume() {
            if (state.audioCtx && state.audioCtx.state === 'suspended') {
                state.audioCtx.resume().then(function() {
                    console.log('[rdp-audio] AudioContext resumed');
                }).catch(function(e) {
                    console.log('[rdp-audio] AudioContext resume failed:', e);
                });
            }
        }
        ['click', 'keydown', 'touchstart'].forEach(function(ev) {
            document.addEventListener(ev, tryResume, { capture: true });
        });

        // Diagnostic hook: let the devtools console toggle WaveConfirm
        // sending so we can A/B test the video-slowdown hypothesis.
        //   window.rdpAudioConfirms(true)  → confirms on (audio continuous)
        //   window.rdpAudioConfirms(false) → confirms off (audio ~12s then cuts out)
        window.rdpAudioConfirms = function(on) {
            state.sendConfirms = !!on;
            console.log('[rdp-audio] WaveConfirms ' + (on ? 'ON' : 'OFF'));
        };

        // Expose a synchronous teardown hook. When the RDP window closes
        // in filemanager.js, the onClose handler calls this to silence
        // audio immediately — the ws.close() that follows may not fire
        // the 'close' event promptly, so we can't rely on the listener.
        ws._rdpAudioTeardown = function() { teardownAudio(state); };

        console.log('[rdp-audio] interceptor installed on', ws.url,
                    '(audio=' + state.audioOn + ')');
        return ws;
    }

    return {
        wrapWebSocket: wrapWebSocket
    };
})();
